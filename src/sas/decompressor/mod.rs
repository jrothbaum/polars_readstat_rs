// Decompressor module
mod rdc;
mod rle;

pub use rdc::RdcDecompressor;
pub use rle::RleDecompressor;

use crate::error::Result;
use crate::types::Compression;

pub enum Decompressor {
    None,
    Rle(RleDecompressor),
    Rdc(RdcDecompressor),
}

impl Decompressor {
    pub fn new(compression: Compression) -> Self {
        match compression {
            Compression::None => Self::None,
            Compression::Rle => Self::Rle(RleDecompressor::new()),
            Compression::Rdc => Self::Rdc(RdcDecompressor::new()),
        }
    }

    pub fn decompress(&mut self, input: &[u8], expected_output_size: usize) -> Result<Vec<u8>> {
        match self {
            Self::None => Ok(input.to_vec()),
            Self::Rle(d) => d.decompress(input, expected_output_size),
            Self::Rdc(d) => d.decompress(input, expected_output_size),
        }
    }
}
