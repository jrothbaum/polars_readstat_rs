use crate::decompressor::Decompressor;
use crate::error::{Error, Result};
use crate::page::{PageHeader, PageReader, PageSubheader};
use crate::types::{Compression, Endian, Format, Metadata, PageType};
use std::io::{Read, Seek};

/// Represents a row of raw bytes from the SAS file
pub type RowBytes = Vec<u8>;

/// Iterator-like reader for SAS data rows
pub struct DataReader<R: Read + Seek> {
    page_reader: PageReader<R>,
    metadata: Metadata,
    decompressor: Decompressor,
    current_row: usize,
    page_state: Option<PageState>,
}

/// State for tracking position within a page
#[derive(Debug)]
enum PageState {
    /// META page with data subheaders
    Meta {
        data_subheaders: Vec<DataSubheader>,
        current_index: usize,
    },
    /// DATA page with sequential rows
    Data {
        block_count: usize,
        offset: usize,
        row_length: usize,
        current_index: usize,
    },
    /// MIX page with metadata and data
    Mix {
        row_count: usize,
        offset: usize,
        row_length: usize,
        current_index: usize,
    },
}

/// Data subheader from META or MIX pages
#[derive(Debug, Clone)]
pub struct DataSubheader {
    pub offset: usize,
    pub length: usize,
    pub compression: u8,  // 4 = compressed, 0 = uncompressed
}

impl<R: Read + Seek> DataReader<R> {
    pub fn new(
        page_reader: PageReader<R>,
        metadata: Metadata,
        _endian: Endian,
        _format: Format,
        initial_data_subheaders: Vec<DataSubheader>,
    ) -> Result<Self> {
        let decompressor = Decompressor::new(metadata.compression);

        // If we have initial data subheaders from metadata reading, use them as first page
        // (matching C++ behavior)
        let page_state = if !initial_data_subheaders.is_empty() {
            Some(PageState::Meta {
                data_subheaders: initial_data_subheaders,
                current_index: 0,
            })
        } else {
            None
        };

        let mut reader = Self {
            page_reader,
            metadata,
            decompressor,
            current_row: 0,
            page_state,
        };

        // Try to read the first page if we don't have initial data subheaders
        if reader.page_state.is_none() {
            reader.advance_page()?;
        }

        Ok(reader)
    }

    /// Read the next row
    pub fn read_row(&mut self) -> Result<Option<RowBytes>> {
        // Check if we've read all rows
        if self.current_row >= self.metadata.row_count {
            return Ok(None);
        }

        // Get row offset, length, and compression flag from current page state
        let (offset, length, compression) = match self.get_current_row_info() {
            Some(info) => info,
            None => {
                // Need to advance to next page
                self.advance_page()?;
                match self.get_current_row_info() {
                    Some(info) => info,
                    None => return Ok(None),
                }
            }
        };

        // Extract row bytes
        let row_bytes = self.extract_row_bytes(offset, length, compression)?;

        // Advance to next row
        self.advance_row();

        Ok(Some(row_bytes))
    }

    /// Skip n rows
    pub fn skip_rows(&mut self, n: usize) -> Result<()> {
        for _ in 0..n {
            if self.read_row()?.is_none() {
                break;
            }
        }
        Ok(())
    }

    /// Get current row offset, length, and compression flag from page state
    fn get_current_row_info(&self) -> Option<(usize, usize, u8)> {
        match &self.page_state {
            Some(PageState::Meta {
                data_subheaders,
                current_index,
            }) => {
                if *current_index < data_subheaders.len() {
                    let sub = &data_subheaders[*current_index];
                    Some((sub.offset, sub.length, sub.compression))
                } else {
                    None
                }
            }
            Some(PageState::Data {
                offset,
                row_length,
                current_index,
                block_count,
            }) => {
                if *current_index < *block_count {
                    let row_offset = offset + row_length * current_index;
                    // Check if row would fit in current page
                    let page_size = self.page_reader.header().page_length;
                    if row_offset + row_length <= page_size {
                        // For DATA pages, assume compression=4 if file is compressed
                        Some((row_offset, *row_length, 4))
                    } else {
                        // Row would extend beyond page, stop processing this page
                        None
                    }
                } else {
                    None
                }
            }
            Some(PageState::Mix {
                offset,
                row_length,
                current_index,
                row_count,
            }) => {
                if *current_index < *row_count {
                    let row_offset = offset + row_length * current_index;
                    // Check if row would fit in current page
                    let page_size = self.page_reader.header().page_length;
                    if row_offset + row_length <= page_size {
                        // For MIX pages, assume compression=4 if file is compressed
                        Some((row_offset, *row_length, 4))
                    } else {
                        // Row would extend beyond page, stop processing this page
                        None
                    }
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Advance to next row within current page
    fn advance_row(&mut self) {
        self.current_row += 1;

        match &mut self.page_state {
            Some(PageState::Meta { current_index, .. }) => {
                *current_index += 1;
            }
            Some(PageState::Data { current_index, .. }) => {
                *current_index += 1;
            }
            Some(PageState::Mix { current_index, .. }) => {
                *current_index += 1;
            }
            None => {}
        }
    }

    /// Advance to next page
    fn advance_page(&mut self) -> Result<()> {
        loop {
            if !self.page_reader.read_page()? {
                return Ok(());
            }

            let page_header = self.page_reader.get_page_header()?;

            if let Some(page_state) = self.build_page_state(&page_header)? {
                self.page_state = Some(page_state);
                return Ok(());
            }

            // Continue to next page if this one doesn't have data
        }
    }

    /// Build page state based on page type
    fn build_page_state(&mut self, page_header: &PageHeader) -> Result<Option<PageState>> {
        let format = self.page_reader.header().format;
        let page_bit_offset = match format {
            Format::Bit64 => 32,
            Format::Bit32 => 16,
        };

        match page_header.page_type {
            PageType::Meta | PageType::Mix1 | PageType::Mix2 | PageType::Amd => {
                // Extract data subheaders from META/MIX pages
                let subheaders = self.page_reader.get_subheaders(page_header)?;
                let data_subheaders = self.extract_data_subheaders(&subheaders)?;

                if !data_subheaders.is_empty() {
                    return Ok(Some(PageState::Meta {
                        data_subheaders,
                        current_index: 0,
                    }));
                }

                // If it's a MIX page with no data subheaders, treat as MIX data page
                if matches!(page_header.page_type, PageType::Mix1 | PageType::Mix2) {
                    let integer_size = match format {
                        Format::Bit64 => 8,
                        Format::Bit32 => 4,
                    };
                    let subheader_size = 3 * integer_size;
                    let mut offset = page_bit_offset + 8 + page_header.subheader_count as usize * subheader_size;

                    // Align to 8 bytes (round up to next multiple of 8)
                    if offset % 8 != 0 {
                        offset += 8 - (offset % 8);
                    }

                    let row_count = self.metadata.row_count.min(
                        page_header.block_count as usize,
                    );

                    return Ok(Some(PageState::Mix {
                        row_count,
                        offset,
                        row_length: self.metadata.row_length,
                        current_index: 0,
                    }));
                }

                Ok(None)
            }
            PageType::Data => {
                // DATA page: rows start at page_bit_offset + 8
                Ok(Some(PageState::Data {
                    block_count: page_header.block_count as usize,
                    offset: page_bit_offset + 8,
                    row_length: self.metadata.row_length,
                    current_index: 0,
                }))
            }
            _ => Ok(None),
        }
    }

    /// Extract data subheaders from page subheaders.
    ///
    /// In compressed SAS files, data rows are stored as subheaders on META pages.
    /// These pages also contain metadata subheaders (ROW_SIZE, COLUMN_SIZE, etc.)
    /// with the same compression/type flags. We distinguish them by:
    /// 1. Length: compressed data rows have length <= row_length
    /// 2. Signature: known metadata subheaders have recognizable 4-byte signatures
    fn extract_data_subheaders(
        &self,
        subheaders: &[PageSubheader],
    ) -> Result<Vec<DataSubheader>> {
        let mut data_subheaders = Vec::new();
        let page_buffer = self.page_reader.page_buffer();
        let row_length = self.metadata.row_length;

        for subheader in subheaders {
            if self.metadata.compression == Compression::None {
                continue;
            }
            // Data subheaders have compression == 4 or 0, and type == 1
            if !((subheader.compression == 4 || subheader.compression == 0)
                && subheader.subheader_type == 1)
            {
                continue;
            }

            // A compressed or uncompressed data row can never be longer than row_length.
            // Metadata subheaders (ROW_SIZE, COLUMN_TEXT, etc.) are typically much larger.
            if subheader.length > row_length {
                continue;
            }

            // Check against known metadata signatures. In compressed SAS files,
            // metadata and data subheaders share the same compression/type flags,
            // so we must check the actual content to distinguish them.
            if subheader.length >= 4 && subheader.offset + 8 <= page_buffer.len() {
                let sig8 = &page_buffer[subheader.offset..subheader.offset + 8];
                if is_known_metadata_signature(sig8) {
                    continue;
                }
                // Skip the problematic [00, FC, FF, FF] pattern found in some compressed files
                // This appears to be metadata/padding, not actual data
                if sig8.len() >= 4 && sig8[0] == 0x00 && sig8[1] == 0xFC && sig8[2] == 0xFF && sig8[3] == 0xFF {
                    continue;
                }
                // Skip [FF, FF, FC, 00] pattern - another metadata variant seen in test12
                if sig8.len() >= 4 && sig8[0] == 0xFF && sig8[1] == 0xFF && sig8[2] == 0xFC && sig8[3] == 0x00 {
                    continue;
                }
            } else if subheader.length >= 4 && subheader.offset + 4 <= page_buffer.len() {
                let sig4 = &page_buffer[subheader.offset..subheader.offset + 4];
                if is_known_metadata_signature(sig4) {
                    continue;
                }
                // Skip the problematic [00, FC, FF, FF] pattern
                if sig4.len() >= 4 && sig4[0] == 0x00 && sig4[1] == 0xFC && sig4[2] == 0xFF && sig4[3] == 0xFF {
                    continue;
                }
                // Skip [FF, FF, FC, 00] pattern
                if sig4.len() >= 4 && sig4[0] == 0xFF && sig4[1] == 0xFF && sig4[2] == 0xFC && sig4[3] == 0x00 {
                    continue;
                }
            }

            data_subheaders.push(DataSubheader {
                offset: subheader.offset,
                length: subheader.length,
                compression: subheader.compression,
            });
        }

        Ok(data_subheaders)
    }

    /// Extract row bytes from page buffer
    fn extract_row_bytes(&mut self, offset: usize, length: usize, _compression: u8) -> Result<RowBytes> {
        let page_buffer = self.page_reader.page_buffer();

        // Validate bounds
        if offset + length > page_buffer.len() {
            return Err(Error::BufferOutOfBounds {
                offset,
                length,
            });
        }

        let raw_bytes = &page_buffer[offset..offset + length];

        // Match C++ behavior: decompress whenever length < row_length,
        // regardless of the compression flag. The compression flag on the
        // subheader isn't a reliable indicator; length comparison is.
        if length < self.metadata.row_length {
            self.decompressor.decompress(raw_bytes, self.metadata.row_length)
        } else {
            Ok(raw_bytes.to_vec())
        }
    }

}

/// Check if an 8-byte signature matches any known metadata subheader type.
/// This covers all known 64-bit and 32-bit signature patterns.
fn is_known_metadata_signature(sig: &[u8]) -> bool {
    if sig.len() < 4 {
        return false;
    }
    // 4-byte signatures (32-bit format, or first 4 bytes of 64-bit)
    let sig4 = &sig[..4];
    if matches!(
        sig4,
        [0xF7, 0xF7, 0xF7, 0xF7]
            | [0xF6, 0xF6, 0xF6, 0xF6]
            | [0xFD, 0xFF, 0xFF, 0xFF]
            | [0xFF, 0xFF, 0xFF, 0xFD]
            | [0xFF, 0xFF, 0xFF, 0xFF]
            | [0xFC, 0xFF, 0xFF, 0xFF]
            | [0xFF, 0xFF, 0xFF, 0xFC]
            | [0xFE, 0xFB, 0xFF, 0xFF]
            | [0xFF, 0xFF, 0xFB, 0xFE]
            | [0xFE, 0xFF, 0xFF, 0xFF]
            | [0xFF, 0xFF, 0xFF, 0xFE]
    ) {
        return true;
    }
    // 64-bit signatures with zero-padded first half
    if sig.len() >= 8 && sig4 == [0x00, 0x00, 0x00, 0x00] {
        let sig4_hi = &sig[4..8];
        if matches!(
            sig4_hi,
            [0xF7, 0xF7, 0xF7, 0xF7]
                | [0xF6, 0xF6, 0xF6, 0xF6]
                | [0xFD, 0xFF, 0xFF, 0xFF]
                | [0xFC, 0xFF, 0xFF, 0xFF]
                | [0xFE, 0xFB, 0xFF, 0xFF]
                | [0xFE, 0xFF, 0xFF, 0xFF]
        ) {
            return true;
        }
    }
    false
}
