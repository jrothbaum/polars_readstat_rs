use crate::buffer::Buffer;
use crate::error::Result;
use crate::types::{Endian, Format, Header, PageType};
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
}

impl<R: Read + Seek> PageReader<R> {
    pub fn new(reader: R, header: Header, endian: Endian, format: Format) -> Self {
        let page_buffer = vec![0u8; header.page_length];
        Self {
            reader,
            header,
            endian,
            format,
            page_buffer,
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
        let buf = Buffer::from_vec(self.page_buffer.clone(), self.endian);

        // Page header offset depends on format
        let page_bit_offset = match self.format {
            Format::Bit64 => 32,
            Format::Bit32 => 16,
        };

        let page_type_val = buf.get_u16(page_bit_offset)?;
        let page_type = PageType::from_u16(page_type_val);
        let block_count = buf.get_u16(page_bit_offset + 2)?;
        let subheader_count = buf.get_u16(page_bit_offset + 4)?;

        Ok(PageHeader {
            page_type,
            block_count,
            subheader_count,
        })
    }

    /// Get all subheaders from current page
    pub fn get_subheaders(&self, page_header: &PageHeader) -> Result<Vec<PageSubheader>> {
        let buf = Buffer::from_vec(self.page_buffer.clone(), self.endian);

        let page_bit_offset = match self.format {
            Format::Bit64 => 32,
            Format::Bit32 => 16,
        };

        let integer_size = match self.format {
            Format::Bit64 => 8,
            Format::Bit32 => 4,
        };

        let subheader_size = 3 * integer_size; // offset, length, compression+type
        let mut subheaders = Vec::new();

        for i in 0..page_header.subheader_count {
            let offset = page_bit_offset + 8 + (i as usize * subheader_size);

            let sub_offset = buf.get_integer(offset, self.format)? as usize;
            let sub_length = buf.get_integer(offset + integer_size, self.format)? as usize;
            let compression = buf.get_u8(offset + integer_size * 2)?;
            let subheader_type = buf.get_u8(offset + integer_size * 2 + 1)?;

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
