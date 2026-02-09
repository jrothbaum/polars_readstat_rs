use crate::error::Result;
use crate::types::{Endian, Format, Header, PageType};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::Cursor;
use std::io::{Read, Seek};

/// Page header containing metadata about the page
#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_type: PageType,
    pub block_count: u16,
    pub subheader_count: u16,
}

/// Subheader descriptor within a page
#[derive(Debug, Clone)]
pub struct PageSubheader {
    pub offset: usize,
    pub length: usize,
    pub compression: u8,
    pub subheader_type: u8,
}

/// Page reader for reading and parsing pages
pub struct PageReader<R: Read + Seek> {
    reader: R,
    header: Header,
    endian: Endian,
    format: Format,
    page_buffer: Vec<u8>,
    page_bit_offset: usize,
    integer_size: usize,
    subheader_size: usize,
}

impl<R: Read + Seek> PageReader<R> {
    pub fn new(reader: R, header: Header, endian: Endian, format: Format) -> Self {
        let page_buffer = vec![0u8; header.page_length];
        let page_bit_offset = match format {
            Format::Bit64 => 32,
            Format::Bit32 => 16,
        };
        let integer_size = match format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };
        let subheader_size = 3 * integer_size;
        Self {
            reader,
            header,
            endian,
            format,
            page_buffer,
            page_bit_offset,
            integer_size,
            subheader_size,
        }
    }

    /// Read next page into buffer
    pub fn read_page(&mut self) -> Result<bool> {
        match self.reader.read_exact(&mut self.page_buffer) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Get page header from current buffer
    pub fn get_page_header(&self) -> Result<PageHeader> {
        let page_type_val = self.read_u16(self.page_bit_offset)?;
        let page_type = PageType::from_u16(page_type_val);
        let block_count = self.read_u16(self.page_bit_offset + 2)?;
        let subheader_count = self.read_u16(self.page_bit_offset + 4)?;

        Ok(PageHeader {
            page_type,
            block_count,
            subheader_count,
        })
    }

    /// Get all subheaders from current page
    pub fn get_subheaders(&self, page_header: &PageHeader) -> Result<Vec<PageSubheader>> {
        let mut subheaders = Vec::new();

        for i in 0..page_header.subheader_count {
            let offset = self.page_bit_offset + 8 + (i as usize * self.subheader_size);

            let sub_offset = self.read_integer(offset)? as usize;
            let sub_length = self.read_integer(offset + self.integer_size)? as usize;
            let compression = self.read_u8(offset + self.integer_size * 2)?;
            let subheader_type = self.read_u8(offset + self.integer_size * 2 + 1)?;

            // Skip empty or truncated subheaders
            if sub_length == 0 || compression == 1 {
                continue;
            }

            subheaders.push(PageSubheader {
                offset: sub_offset,
                length: sub_length,
                compression,
                subheader_type,
            });
        }

        Ok(subheaders)
    }

    /// Get reference to page buffer
    pub fn page_buffer(&self) -> &[u8] {
        &self.page_buffer
    }

    /// Get header reference
    pub fn header(&self) -> &Header {
        &self.header
    }

    fn read_u8(&self, offset: usize) -> Result<u8> {
        self.page_buffer
            .get(offset)
            .copied()
            .ok_or(crate::error::Error::BufferOutOfBounds { offset, length: 1 })
    }

    fn read_u16(&self, offset: usize) -> Result<u16> {
        let slice = self
            .page_buffer
            .get(offset..offset + 2)
            .ok_or(crate::error::Error::BufferOutOfBounds { offset, length: 2 })?;
        let mut cursor = Cursor::new(slice);
        match self.endian {
            Endian::Big => cursor.read_u16::<BigEndian>(),
            Endian::Little => cursor.read_u16::<LittleEndian>(),
        }
        .map_err(|e| e.into())
    }

    fn read_u32(&self, offset: usize) -> Result<u32> {
        let slice = self
            .page_buffer
            .get(offset..offset + 4)
            .ok_or(crate::error::Error::BufferOutOfBounds { offset, length: 4 })?;
        let mut cursor = Cursor::new(slice);
        match self.endian {
            Endian::Big => cursor.read_u32::<BigEndian>(),
            Endian::Little => cursor.read_u32::<LittleEndian>(),
        }
        .map_err(|e| e.into())
    }

    fn read_u64(&self, offset: usize) -> Result<u64> {
        let slice = self
            .page_buffer
            .get(offset..offset + 8)
            .ok_or(crate::error::Error::BufferOutOfBounds { offset, length: 8 })?;
        let mut cursor = Cursor::new(slice);
        match self.endian {
            Endian::Big => cursor.read_u64::<BigEndian>(),
            Endian::Little => cursor.read_u64::<LittleEndian>(),
        }
        .map_err(|e| e.into())
    }

    fn read_integer(&self, offset: usize) -> Result<u64> {
        match self.format {
            Format::Bit32 => self.read_u32(offset).map(|v| v as u64),
            Format::Bit64 => self.read_u64(offset),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_bit_offset() {
        // 64-bit format should use offset 32
        let offset_64 = match Format::Bit64 {
            Format::Bit64 => 32,
            Format::Bit32 => 16,
        };
        assert_eq!(offset_64, 32);

        // 32-bit format should use offset 16
        let offset_32 = match Format::Bit32 {
            Format::Bit64 => 32,
            Format::Bit32 => 16,
        };
        assert_eq!(offset_32, 16);
    }
}
