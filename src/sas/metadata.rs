use crate::buffer::Buffer;
use crate::constants::*;
use crate::encoding;
use crate::error::{Error, Result};
use crate::page::{PageHeader, PageReader, PageSubheader};
use crate::types::{Column, ColumnType, Compression, Endian, Format, Header, Metadata};
use std::io::{Read, Seek};

/// Read metadata from SAS7BDAT file.
/// Returns (metadata, initial_data_subheaders, first_data_page, mix_data_rows).
/// - `first_data_page`: 0-based index of the first non-metadata (DATA) page.
/// - `mix_data_rows`: total data rows on MIX pages encountered before the first DATA page.
///   These rows are returned by the DataReader before any DATA-page rows.
pub fn read_metadata<R: Read + Seek>(
    reader: R,
    header: &Header,
    endian: Endian,
    format: Format,
) -> Result<(Metadata, Vec<DataSubheader>, usize, usize)> {
    use crate::types::PageType;

    let page_bit_offset: usize = match format {
        Format::Bit64 => 32,
        Format::Bit32 => 16,
    };
    let integer_size: usize = match format {
        Format::Bit64 => 8,
        Format::Bit32 => 4,
    };

    let mut page_reader = PageReader::new(reader, header.clone(), endian, format);
    let mut metadata_builder = MetadataBuilder::new(header.encoding_byte);
    let mut pages_read = 0usize;
    let mut mix_data_rows = 0usize;

    // Read metadata pages until we have all column information
    loop {
        if !page_reader.read_page()? {
            break;
        }
        pages_read += 1;

        let page_header = page_reader.get_page_header()?;

        // Only process metadata pages
        if !is_metadata_page(&page_header) {
            break; // Stop when we hit data pages
        }

        let subheaders = page_reader.get_subheaders(&page_header)?;
        let page_buffer = page_reader.page_buffer();
        let buf = Buffer::from_vec(page_buffer.to_vec(), endian);

        for subheader in subheaders {
            metadata_builder.process_subheader(&buf, &subheader, format)?;
        }

        // For MIX pages, count data rows they contain (uncompressed files only).
        // Do this AFTER processing subheaders so row_length / mix_page_row_count
        // are available even when the ROW_SIZE subheader lives on this same page.
        if matches!(page_header.page_type, PageType::Mix1 | PageType::Mix2) {
            if let (Some(row_length), Some(mix_row_count)) = (
                metadata_builder.row_length,
                metadata_builder.mix_page_row_count,
            ) {
                if row_length > 0 {
                    let subheader_size = 3 * integer_size;
                    let mut data_start = page_bit_offset
                        + 8
                        + page_header.subheader_count as usize * subheader_size;
                    if data_start % 8 == 4 {
                        data_start += 4;
                    }
                    let available = header.page_length.saturating_sub(data_start);
                    let max_fit = available / row_length;
                    mix_data_rows += max_fit.min(mix_row_count);
                }
            }
        }

        // Continue reading metadata pages until we hit a data page
        // Don't stop early - we need to read ALL metadata pages to get
        // COLUMN_NAME, COLUMN_ATTRIBUTES, and FORMAT_AND_LABEL subheaders
    }

    // The last page read was the first DATA page (or EOF if no data pages).
    // first_data_page = pages_read - 1 (0-indexed).
    let first_data_page = pages_read.saturating_sub(1);

    // Don't collect data_subheaders - causes issues with page state management
    // Instead, filter during data reading phase
    let data_subheaders = Vec::new();
    let metadata = metadata_builder.build()?;
    Ok((metadata, data_subheaders, first_data_page, mix_data_rows))
}

fn is_metadata_page(page_header: &PageHeader) -> bool {
    use crate::types::PageType;
    matches!(
        page_header.page_type,
        PageType::Meta | PageType::Mix1 | PageType::Mix2 | PageType::Amd
    )
}

use crate::data::DataSubheader;

/// Helper struct for building metadata incrementally
struct MetadataBuilder {
    encoding: &'static encoding_rs::Encoding,
    encoding_byte: u8,
    row_count: Option<usize>,
    row_length: Option<usize>,
    mix_page_row_count: Option<usize>,
    column_count: Option<usize>,
    compression: Compression,
    columns: Vec<ColumnBuilder>,
    creator: String,
    creator_proc: String,
    lcs: usize, // length of creator string (from row_size subheader)
    lcp: usize, // length of creator_proc string (from row_size subheader)
    column_texts: Vec<Vec<u8>>, // Keep as bytes to preserve offset indexing
    next_column_name_position: usize, // Track position across multiple COLUMN_NAME subheaders
    next_column_attributes_position: usize, // Track position across multiple COLUMN_ATTRIBUTES subheaders
    next_format_label_position: usize, // Track position across multiple FORMAT_AND_LABEL subheaders
    data_subheaders: Vec<DataSubheader>, // Data subheaders collected during metadata reading
}

#[derive(Default, Clone)]
struct ColumnBuilder {
    name: String,
    label: String,
    format: String,
    col_type: ColumnType,
    offset: usize,
    length: usize,
}

impl MetadataBuilder {
    fn new(encoding_byte: u8) -> Self {
        Self {
            encoding: encoding::get_encoding(encoding_byte),
            encoding_byte,
            row_count: None,
            row_length: None,
            mix_page_row_count: None,
            column_count: None,
            compression: Compression::None,
            columns: Vec::new(),
            creator: String::new(),
            creator_proc: String::new(),
            lcs: 0,
            lcp: 0,
            column_texts: Vec::new(),
            next_column_name_position: 0,
            next_column_attributes_position: 0,
            next_format_label_position: 0,
            data_subheaders: Vec::new(),
        }
    }

    /// Check if a subheader is a data subheader (matches C++ DataSubHeader::check)
    fn is_data_subheader(&self, subheader: &PageSubheader) -> bool {
        // Data subheaders have:
        // 1. compression != none (file must be compressed)
        // 2. subheader.compression == 4 (compressed) - exclude compression=0 as those are metadata/padding
        // 3. subheader.type == 1
        self.compression != Compression::None
            && subheader.compression == 4  // Only accept compressed=4, not 0
            && subheader.subheader_type == 1
    }

    fn process_subheader(
        &mut self,
        buf: &Buffer,
        subheader: &PageSubheader,
        format: Format,
    ) -> Result<()> {
        let sig_len = match format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };

        let signature = buf.get_bytes(subheader.offset, sig_len)?;

        if matches_signature(signature, &get_row_size_signatures(format)) {
            self.process_row_size_subheader(buf, subheader, format)?;
        } else if matches_signature(signature, &get_column_size_signatures(format)) {
            self.process_column_size_subheader(buf, subheader, format)?;
        } else if matches_signature(signature, &get_column_text_signatures(format)) {
            self.process_column_text_subheader(buf, subheader, format)?;
        } else if matches_signature(signature, &get_column_name_signatures(format)) {
            self.process_column_name_subheader(buf, subheader, format)?;
        } else if matches_signature(signature, &get_column_attributes_signatures(format)) {
            self.process_column_attributes_subheader(buf, subheader, format)?;
        } else if matches_signature(signature, &get_format_and_label_signatures(format)) {
            self.process_format_and_label_subheader(buf, subheader, format)?;
        } else if self.is_data_subheader(subheader) {
            // Collect data subheaders during metadata reading (matching C++ behavior)
            self.data_subheaders.push(DataSubheader {
                offset: subheader.offset,
                length: subheader.length,
                compression: subheader.compression,
            });
        }

        Ok(())
    }

    fn process_row_size_subheader(
        &mut self,
        buf: &Buffer,
        subheader: &PageSubheader,
        format: Format,
    ) -> Result<()> {
        let offset = subheader.offset;
        let integer_size = match format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };

        // Read fields at correct offsets based on integer_size
        let row_length = buf.get_integer(offset + 5 * integer_size, format)? as usize;
        self.row_length = Some(row_length);

        let row_count = buf.get_integer(offset + 6 * integer_size, format)? as usize;
        self.row_count = Some(row_count);

        let _col_count_p1 = buf.get_integer(offset + 9 * integer_size, format)? as usize;
        let _col_count_p2 = buf.get_integer(offset + 10 * integer_size, format)? as usize;
        let mix_page_row_count = buf.get_integer(offset + 15 * integer_size, format)? as usize;
        self.mix_page_row_count = Some(mix_page_row_count);

        // NOTE: C++ code does NOT set column_count here!
        // It only stores col_count_p1 and col_count_p2 for reference.
        // The actual column_count should ONLY be set by COLUMN_SIZE subheader.

        // Read lcs and lcp for creator_proc extraction (C++ offsets: 64-bit=682/706, 32-bit=354/378)
        let (lcs_off, lcp_off) = match format {
            Format::Bit64 => (682usize, 706usize),
            Format::Bit32 => (354usize, 378usize),
        };
        if let Ok(lcs) = buf.get_u16(offset + lcs_off) {
            self.lcs = lcs as usize;
        }
        if let Ok(lcp) = buf.get_u16(offset + lcp_off) {
            self.lcp = lcp as usize;
        }

        Ok(())
    }

    fn process_column_size_subheader(
        &mut self,
        buf: &Buffer,
        subheader: &PageSubheader,
        format: Format,
    ) -> Result<()> {
        let offset = subheader.offset;
        let integer_size = match format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };

        // C++ code: buf.get_uinteger(_subheader.offset + integer_size)
        let column_count = buf.get_integer(offset + integer_size, format)? as usize;

        // C++ code always updates column_count (no if check)
        // Initialize columns vector if not already done
        if self.columns.is_empty() || self.columns.len() != column_count {
            self.column_count = Some(column_count);
            self.columns = vec![ColumnBuilder::default(); column_count];
        } else {
            self.column_count = Some(column_count);
        }

        Ok(())
    }

    fn process_column_text_subheader(
        &mut self,
        buf: &Buffer,
        subheader: &PageSubheader,
        format: Format,
    ) -> Result<()> {
        let offset = subheader.offset;
        let integer_size = match format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };

        // Length is at offset + integer_size (2 bytes)
        let text_block_size = buf.get_u16(offset + integer_size)? as usize;

        if text_block_size > 0 && text_block_size < subheader.length {
            // Text starts at offset + integer_size and is text_block_size bytes long
            let text_offset = offset + integer_size;
            let text_bytes = buf.get_bytes(text_offset, text_block_size)?;

            // Check for compression signatures (these are ASCII so no encoding needed)
            if text_bytes
                .windows(COMPRESSION_SIGNATURE_RLE.len())
                .any(|w| w == COMPRESSION_SIGNATURE_RLE.as_bytes())
            {
                self.compression = Compression::Rle;
            } else if text_bytes
                .windows(COMPRESSION_SIGNATURE_RDC.len())
                .any(|w| w == COMPRESSION_SIGNATURE_RDC.as_bytes())
            {
                self.compression = Compression::Rdc;
            }

            // On the first column text subheader, extract creator/creator_proc.
            // Matches C++ logic: read compression string at compression_offset,
            // then pick creator_proc location based on compression type.
            if self.column_texts.is_empty() && (self.lcs > 0 || self.lcp > 0) {
                let comp_off = match format {
                    Format::Bit64 => 20usize,
                    Format::Bit32 => 16usize,
                };
                // Read the 8-byte compression string at compression_offset
                let comp_str = buf.get_string(offset + comp_off, 8).unwrap_or_default();
                let comp_str = comp_str.trim_end_matches('\0').trim();
                if comp_str.is_empty() {
                    // No compression: creator_proc at comp_off + 16, length lcp
                    self.lcs = 0;
                    if self.lcp > 0 {
                        if let Ok(s) = buf.get_string(offset + comp_off + 16, self.lcp) {
                            self.creator_proc = s.trim_end_matches('\0').trim().to_string();
                        }
                    }
                } else if comp_str == COMPRESSION_SIGNATURE_RLE.trim() || comp_str.contains("SASYZCRL") {
                    // RLE: creator_proc at comp_off + 24, length lcp
                    if self.lcp > 0 {
                        if let Ok(s) = buf.get_string(offset + comp_off + 24, self.lcp) {
                            self.creator_proc = s.trim_end_matches('\0').trim().to_string();
                        }
                    }
                } else if self.lcs > 0 {
                    // Other compression: creator at comp_off, length lcs
                    self.lcp = 0;
                    if let Ok(s) = buf.get_string(offset + comp_off, self.lcs) {
                        self.creator = s.trim_end_matches('\0').trim().to_string();
                    }
                }
            }

            // Store raw bytes for later use by column names/labels
            // We'll decode them when extracting individual strings
            self.column_texts.push(text_bytes.to_vec());
        }

        Ok(())
    }

    fn process_column_name_subheader(
        &mut self,
        buf: &Buffer,
        subheader: &PageSubheader,
        format: Format,
    ) -> Result<()> {
        let offset = subheader.offset;
        let integer_size = match format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };

        // C++ code: loop starts at offset + integer_size + 8
        // loop increment is fixed 8 bytes, NOT integer_size
        let offset_max = offset + subheader.length - 12 - integer_size;
        let mut entry_offset = offset + integer_size + 8;

        while entry_offset <= offset_max {
            let text_idx = buf.get_u16(entry_offset)? as usize; // Index into column_texts
            let name_offset = buf.get_u16(entry_offset + 2)? as usize;
            let name_len = buf.get_u16(entry_offset + 4)? as usize;

            if name_len > 0 {
                // Extract name from column_texts[text_idx]
                let name = self.extract_text_from_text_block(text_idx, name_offset, name_len)?;

                // Assign names sequentially across all COLUMN_NAME subheaders
                if self.next_column_name_position < self.columns.len() {
                    self.columns[self.next_column_name_position].name = name;
                    self.next_column_name_position += 1;
                }
            }

            entry_offset += 8; // Fixed 8-byte increment
        }

        Ok(())
    }

    fn process_column_attributes_subheader(
        &mut self,
        buf: &Buffer,
        subheader: &PageSubheader,
        format: Format,
    ) -> Result<()> {
        let offset = subheader.offset;
        let integer_size = match format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };

        // C++ code: loop starts at offset + integer_size + 8
        // loop increment is integer_size + 8
        let offset_max = offset + subheader.length - 12 - integer_size;
        let mut entry_offset = offset + integer_size + 8;
        while entry_offset <= offset_max {
            let col_offset = buf.get_integer(entry_offset, format)? as usize;
            let col_length = buf.get_u32(entry_offset + integer_size)? as usize;
            let col_type_byte = buf.get_u8(entry_offset + integer_size + 6)?; // C++ uses +6, not +4

            let col_type = if col_type_byte == 1 {
                ColumnType::Numeric
            } else {
                ColumnType::Character
            };

            if self.next_column_attributes_position < self.columns.len() {
                let idx = self.next_column_attributes_position;
                self.columns[idx].offset = col_offset;
                self.columns[idx].length = col_length;
                self.columns[idx].col_type = col_type;
                self.next_column_attributes_position += 1;
            }

            entry_offset += integer_size + 8; // Format-dependent increment
        }

        Ok(())
    }

    fn process_format_and_label_subheader(
        &mut self,
        buf: &Buffer,
        subheader: &PageSubheader,
        format: Format,
    ) -> Result<()> {
        let integer_size = match format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };

        // C++ code: offset = _subheader.offset + 3 * integer_size
        let base_offset = subheader.offset + 3 * integer_size;

        // Read format information
        let format_idx = buf.get_u16(base_offset + 22)? as usize;
        let format_offset = buf.get_u16(base_offset + 24)? as usize;
        let format_length = buf.get_u16(base_offset + 26)? as usize;

        // Read label information
        let label_idx = buf.get_u16(base_offset + 28)? as usize;
        let label_offset = buf.get_u16(base_offset + 30)? as usize;
        let label_length = buf.get_u16(base_offset + 32)? as usize;

        // Extract format and label strings from text blocks
        let column_format =
            self.extract_text_from_text_block(format_idx, format_offset, format_length)?;
        let column_label =
            self.extract_text_from_text_block(label_idx, label_offset, label_length)?;

        // Assign to the next column sequentially
        if self.next_format_label_position < self.columns.len() {
            self.columns[self.next_format_label_position].format = column_format;
            self.columns[self.next_format_label_position].label = column_label;
            self.next_format_label_position += 1;
        }

        Ok(())
    }

    fn extract_text_from_text_block(
        &self,
        text_idx: usize,
        offset: usize,
        length: usize,
    ) -> Result<String> {
        // C++ code: get_column_text_substr uses text_idx to index into column_texts
        if text_idx < self.column_texts.len() {
            let text_bytes = &self.column_texts[text_idx];
            let text_len = text_bytes.len();

            // Bounds check
            let offset = offset.min(text_len);
            let length = length.min(text_len - offset);

            if length == 0 {
                return Ok(String::new());
            }

            let extracted_bytes = &text_bytes[offset..offset + length];

            // Decode using the file's encoding
            let decoded =
                encoding::decode_string(extracted_bytes, self.encoding_byte, self.encoding);

            // Trim whitespace and unprintable characters
            Ok(decoded.trim().to_string())
        } else {
            Ok(String::new())
        }
    }

    fn build(self) -> Result<Metadata> {
        let row_count = self.row_count.ok_or(Error::MissingMetadata)?;
        let row_length = self.row_length.ok_or(Error::MissingMetadata)?;
        let column_count = self.column_count.ok_or(Error::MissingMetadata)?;
        let mix_page_row_count = self.mix_page_row_count.unwrap_or(row_count);

        // Check for empty column names and return error if found
        let empty_count = self.columns.iter().filter(|c| c.name.is_empty()).count();
        if empty_count > 0 {
            return Err(Error::ParseError(format!(
                "Failed to parse column names: {} of {} columns have empty names. \
                 This indicates missing or incorrectly parsed COLUMN_NAME subheaders.",
                empty_count,
                self.columns.len()
            )));
        }

        let columns = self
            .columns
            .into_iter()
            .map(|cb| Column {
                name: cb.name,
                label: cb.label,
                format: cb.format,
                col_type: cb.col_type,
                offset: cb.offset,
                length: cb.length,
            })
            .collect();

        Ok(Metadata {
            compression: self.compression,
            row_count,
            row_length,
            mix_page_row_count,
            column_count,
            columns,
            creator: self.creator,
            creator_proc: self.creator_proc,
            encoding_byte: self.encoding_byte,
            page_index: Vec::new(), // TODO: Build page index for fast seeking
        })
    }
}

// Signature matching functions
fn matches_signature(sig: &[u8], patterns: &[&[u8]]) -> bool {
    patterns.iter().any(|pattern| sig == *pattern)
}

fn get_row_size_signatures(format: Format) -> Vec<&'static [u8]> {
    match format {
        Format::Bit64 => vec![
            &[0x00, 0x00, 0x00, 0x00, 0xF7, 0xF7, 0xF7, 0xF7],
            &[0xF7, 0xF7, 0xF7, 0xF7, 0x00, 0x00, 0x00, 0x00],
            &[0xF7, 0xF7, 0xF7, 0xF7, 0xFF, 0xFF, 0xFB, 0xFE],
        ],
        Format::Bit32 => vec![&[0xF7, 0xF7, 0xF7, 0xF7]],
    }
}

fn get_column_size_signatures(format: Format) -> Vec<&'static [u8]> {
    match format {
        Format::Bit64 => vec![
            &[0x00, 0x00, 0x00, 0x00, 0xF6, 0xF6, 0xF6, 0xF6],
            &[0xF6, 0xF6, 0xF6, 0xF6, 0x00, 0x00, 0x00, 0x00],
            &[0xF6, 0xF6, 0xF6, 0xF6, 0xFF, 0xFF, 0xFB, 0xFE],
        ],
        Format::Bit32 => vec![&[0xF6, 0xF6, 0xF6, 0xF6]],
    }
}

fn get_column_text_signatures(format: Format) -> Vec<&'static [u8]> {
    match format {
        Format::Bit64 => vec![
            &[0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD],
        ],
        Format::Bit32 => vec![&[0xFD, 0xFF, 0xFF, 0xFF], &[0xFF, 0xFF, 0xFF, 0xFD]],
    }
}

fn get_column_name_signatures(format: Format) -> Vec<&'static [u8]> {
    match format {
        Format::Bit64 => vec![&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]],
        Format::Bit32 => vec![&[0xFF, 0xFF, 0xFF, 0xFF]],
    }
}

fn get_column_attributes_signatures(format: Format) -> Vec<&'static [u8]> {
    match format {
        Format::Bit64 => vec![
            &[0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFC],
        ],
        Format::Bit32 => vec![&[0xFC, 0xFF, 0xFF, 0xFF], &[0xFF, 0xFF, 0xFF, 0xFC]],
    }
}

fn get_format_and_label_signatures(format: Format) -> Vec<&'static [u8]> {
    match format {
        Format::Bit64 => vec![
            &[0xFE, 0xFB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFB, 0xFE],
        ],
        Format::Bit32 => vec![&[0xFE, 0xFB, 0xFF, 0xFF], &[0xFF, 0xFF, 0xFB, 0xFE]],
    }
}
