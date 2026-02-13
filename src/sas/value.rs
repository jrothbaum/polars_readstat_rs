use crate::encoding;
use crate::error::{Error, Result};
use crate::types::{Column, ColumnType, Endian, Metadata};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::Cursor;

/// Represents a parsed value from a SAS file
#[derive(Debug, Clone)]
pub enum Value {
    Numeric(Option<f64>),
    Character(Option<String>),
}

/// Value parser for extracting column values from row bytes
pub struct ValueParser {
    endian: Endian,
    encoding: &'static encoding_rs::Encoding,
    encoding_byte: u8,
    missing_string_as_null: bool,
}

impl ValueParser {
    pub fn new(endian: Endian, encoding_byte: u8, missing_string_as_null: bool) -> Self {
        let encoding = encoding::get_encoding(encoding_byte);
        Self {
            endian,
            encoding,
            encoding_byte,
            missing_string_as_null,
        }
    }

    /// Parse a single column value from row bytes
    pub fn parse_value(&self, row_bytes: &[u8], column: &Column) -> Result<Value> {
        // Check bounds
        if column.offset + column.length > row_bytes.len() {
            eprintln!("DEBUG: parse_value BufferOutOfBounds - column '{}' offset={}, length={}, row_bytes.len()={}",
                      column.name, column.offset, column.length, row_bytes.len());
            return Err(Error::BufferOutOfBounds {
                offset: column.offset,
                length: column.length,
            });
        }

        let value_bytes = &row_bytes[column.offset..column.offset + column.length];

        match column.col_type {
            ColumnType::Numeric => self.parse_numeric(value_bytes),
            ColumnType::Character => self.parse_character(value_bytes),
        }
    }

    /// Parse numeric value (f64)
    /// SAS stores truncated IEEE 754 doubles (3-7 bytes) for narrow numeric columns.
    /// The stored bytes are the most significant bytes; we pad with zeros to reconstruct
    /// the full 8-byte double.
    fn parse_numeric(&self, bytes: &[u8]) -> Result<Value> {
        if bytes.is_empty() {
            return Ok(Value::Numeric(None));
        }

        let value = if bytes.len() >= 8 {
            let mut cursor = Cursor::new(bytes);
            match self.endian {
                Endian::Little => cursor.read_f64::<LittleEndian>()?,
                Endian::Big => cursor.read_f64::<BigEndian>()?,
            }
        } else {
            // Short numeric: pad to 8 bytes following C++ get_incomplete_double pattern.
            // Stored bytes are the most significant bytes of the IEEE 754 double.
            let mut buf = [0u8; 8];
            match self.endian {
                Endian::Little => {
                    // LE: MSB is at high address; pad zeros at start, copy bytes to end
                    let start = 8 - bytes.len();
                    buf[start..].copy_from_slice(bytes);
                }
                Endian::Big => {
                    // BE: MSB is at low address; copy bytes to start, pad zeros at end
                    buf[..bytes.len()].copy_from_slice(bytes);
                }
            }
            let mut cursor = Cursor::new(&buf[..]);
            match self.endian {
                Endian::Little => cursor.read_f64::<LittleEndian>()?,
                Endian::Big => cursor.read_f64::<BigEndian>()?,
            }
        };

        if value.is_nan() || value.is_infinite() {
            Ok(Value::Numeric(None))
        } else {
            Ok(Value::Numeric(Some(value)))
        }
    }

    /// Parse character value (String)
    fn parse_character(&self, bytes: &[u8]) -> Result<Value> {
        // SAS strings are space-padded and may contain null bytes
        // Trim trailing spaces and null bytes
        let mut end = bytes.len();
        while end > 0 && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\x00') {
            end -= 1;
        }

        // Check for empty or all-space/null string
        if end == 0 {
            return if self.missing_string_as_null {
                Ok(Value::Character(None))
            } else {
                Ok(Value::Character(Some(String::new())))
            };
        }

        // Decode using the file's encoding
        let s = encoding::decode_string(&bytes[..end], self.encoding_byte, self.encoding);

        Ok(Value::Character(Some(s)))
    }
}

const SAS_MISSING_MIN: u64 = 0x7ff0_0000_0000_0000;

/// Decode numeric bytes into (value, is_missing) without allocating Value.
pub fn decode_numeric_bytes_mask(endian: Endian, bytes: &[u8]) -> (f64, bool) {
    if bytes.is_empty() {
        return (0.0, true);
    }
    let bits = if bytes.len() >= 8 {
        let slice = &bytes[..8];
        match endian {
            Endian::Little => u64::from_le_bytes(slice.try_into().unwrap_or([0u8; 8])),
            Endian::Big => u64::from_be_bytes(slice.try_into().unwrap_or([0u8; 8])),
        }
    } else {
        let mut buf = [0u8; 8];
        match endian {
            Endian::Little => {
                let start = 8 - bytes.len();
                buf[start..].copy_from_slice(bytes);
            }
            Endian::Big => {
                buf[..bytes.len()].copy_from_slice(bytes);
            }
        }
        match endian {
            Endian::Little => u64::from_le_bytes(buf),
            Endian::Big => u64::from_be_bytes(buf),
        }
    };
    let abs_bits = bits & 0x7fff_ffff_ffff_ffff;
    // Treat any NaN/Inf as missing (matches prior behavior and polars_readstat).
    let is_missing = abs_bits >= SAS_MISSING_MIN;
    (f64::from_bits(bits), is_missing)
}

/// Parse row values into a reusable buffer to avoid per-row allocations.
pub fn parse_row_values_into(
    parser: &ValueParser,
    row_bytes: &[u8],
    metadata: &Metadata,
    column_indices: Option<&[usize]>,
    out: &mut Vec<Value>,
) -> Result<()> {
    out.clear();
    match column_indices {
        Some(indices) => {
            out.reserve(indices.len());
            for &idx in indices {
                out.push(parser.parse_value(row_bytes, &metadata.columns[idx])?);
            }
        }
        None => {
            out.reserve(metadata.columns.len());
            for col in &metadata.columns {
                out.push(parser.parse_value(row_bytes, col)?);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_numeric() {
        // Use UTF-8 encoding (byte 20)
        let parser = ValueParser::new(Endian::Little, 20, true);

        // Test normal value
        let bytes = 42.5f64.to_le_bytes();
        let value = parser.parse_numeric(&bytes).unwrap();
        match value {
            Value::Numeric(Some(v)) => assert!((v - 42.5).abs() < 0.001),
            _ => panic!("Expected numeric value"),
        }

        // Test missing value (NaN)
        let bytes = f64::NAN.to_le_bytes();
        let value = parser.parse_numeric(&bytes).unwrap();
        match value {
            Value::Numeric(None) => {}
            _ => panic!("Expected missing numeric value"),
        }
    }

    #[test]
    fn test_parse_short_numeric_little_endian() {
        let parser = ValueParser::new(Endian::Little, 20, true);

        // Value 1.0 as f64 LE bytes: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F]
        // SAS stores the 3 most significant bytes: [0xF0, 0x3F] would be 2 bytes,
        // but for 3 bytes it stores [0x00, 0xF0, 0x3F] (bytes[5..8] of the full LE double)
        let full_bytes = 1.0f64.to_le_bytes();
        // 3-byte truncated: last 3 bytes of the LE representation
        let short = &full_bytes[5..8]; // [0x00, 0xF0, 0x3F]
        let value = parser.parse_numeric(short).unwrap();
        match value {
            Value::Numeric(Some(v)) => assert!((v - 1.0).abs() < 0.001, "Expected ~1.0, got {}", v),
            _ => panic!(
                "Expected numeric value for short 3-byte LE, got {:?}",
                value
            ),
        }

        // Value 2.0 as f64 LE bytes: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40]
        let full_bytes = 2.0f64.to_le_bytes();
        let short = &full_bytes[5..8];
        let value = parser.parse_numeric(short).unwrap();
        match value {
            Value::Numeric(Some(v)) => assert!((v - 2.0).abs() < 0.001, "Expected ~2.0, got {}", v),
            _ => panic!("Expected numeric value for short 3-byte LE"),
        }

        // Empty bytes → None
        let value = parser.parse_numeric(&[]).unwrap();
        match value {
            Value::Numeric(None) => {}
            _ => panic!("Expected None for empty bytes"),
        }
    }

    #[test]
    fn test_parse_short_numeric_big_endian() {
        let parser = ValueParser::new(Endian::Big, 20, true);

        // Value 1.0 as f64 BE bytes: [0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        // 3-byte truncated: first 3 bytes [0x3F, 0xF0, 0x00]
        let full_bytes = 1.0f64.to_be_bytes();
        let short = &full_bytes[0..3];
        let value = parser.parse_numeric(short).unwrap();
        match value {
            Value::Numeric(Some(v)) => assert!((v - 1.0).abs() < 0.001, "Expected ~1.0, got {}", v),
            _ => panic!(
                "Expected numeric value for short 3-byte BE, got {:?}",
                value
            ),
        }
    }

    #[test]
    fn test_parse_character() {
        // Use UTF-8 encoding (byte 20)
        let parser = ValueParser::new(Endian::Little, 20, true);

        // Test normal string
        let bytes = b"hello   ";
        let value = parser.parse_character(bytes).unwrap();
        match value {
            Value::Character(Some(s)) => assert_eq!(s, "hello"),
            _ => panic!("Expected character value"),
        }

        // Test empty string (all spaces)
        let bytes = b"        ";
        let value = parser.parse_character(bytes).unwrap();
        match value {
            Value::Character(None) => {}
            _ => panic!("Expected missing character value"),
        }
    }
}
