pub(crate) mod buffer;
pub(crate) mod constants;
pub(crate) mod data;
pub(crate) mod decompressor;
pub(crate) mod encoding;
pub(crate) mod error;
pub(crate) mod page;
pub(crate) mod pipeline;
pub mod polars_output;
pub(crate) mod types;
pub(crate) mod value;
pub mod writer;

pub mod arrow_output;
pub mod header;
pub mod metadata;
pub mod reader;

pub use error::{Error, Result};
pub use polars_output::scan_sas7bdat;
pub use reader::Sas7bdatReader;
pub use writer::{
    SasValueLabelKey,
    SasValueLabelMap,
    SasValueLabels,
    SasVariableLabels,
    SasWriter,
};
pub use types::{Compression, Endian, Format, Platform};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};

use serde_json::json;
use std::path::Path;

/// Export SAS metadata as a JSON string.
pub fn metadata_json(path: impl AsRef<Path>) -> Result<String> {
    let reader = Sas7bdatReader::open(path)?;
    let meta = reader.metadata();
    let columns = meta.columns.iter().map(|c| {
        json!({
            "name": c.name,
            "label": c.label,
            "format": c.format,
            "type": format!("{:?}", c.col_type),
            "offset": c.offset,
            "length": c.length,
        })
    }).collect::<Vec<_>>();
    let v = json!({
        "compression": format!("{:?}", meta.compression),
        "row_count": meta.row_count,
        "row_length": meta.row_length,
        "column_count": meta.column_count,
        "creator": meta.creator,
        "creator_proc": meta.creator_proc,
        "encoding_byte": meta.encoding_byte,
        "columns": columns,
    });
    Ok(v.to_string())
}

#[derive(Debug, Clone)]
pub struct DebugRawRow {
    pub row_idx: usize,
    pub raw_hex: String,
    pub trimmed_hex: String,
    pub decoded: String,
}

#[derive(Debug, Clone)]
pub struct DebugRawColumn {
    pub encoding_byte: u8,
    pub encoding_name: &'static str,
    pub compression: Compression,
    pub row_length: usize,
    pub offset: usize,
    pub length: usize,
    pub rows: Vec<DebugRawRow>,
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

fn trim_sas_bytes(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\x00') {
        end -= 1;
    }
    &bytes[..end]
}

/// Debug helper: extract raw bytes for a column across a row range.
pub fn debug_dump_raw_column(
    path: impl AsRef<Path>,
    column: &str,
    start_row: usize,
    count: usize,
) -> Result<DebugRawColumn> {
    let reader = Sas7bdatReader::open(&path)?;
    let metadata = reader.metadata().clone();
    let header = reader.header().clone();
    let endian = reader.endian();
    let format = reader.format();
    let initial_subheaders = reader.initial_data_subheaders().to_vec();

    let col = metadata
        .columns
        .iter()
        .find(|c| c.name == column)
        .ok_or_else(|| Error::ParseError(format!("Column not found: {column}")))?;

    let mut file_handle = BufReader::new(File::open(path.as_ref())?);
    file_handle.seek(SeekFrom::Start(header.header_length as u64))?;
    let page_reader = page::PageReader::new(file_handle, header, endian, format);
    let mut data_reader = data::DataReader::new(
        page_reader,
        metadata.clone(),
        endian,
        format,
        initial_subheaders,
    )?;

    if start_row > 0 {
        data_reader.skip_rows(start_row)?;
    }

    let encoding = encoding::get_encoding(metadata.encoding_byte);
    let mut rows = Vec::new();
    for i in 0..count {
        let row_idx = start_row + i;
        let row_bytes = match data_reader.read_row()? {
            Some(b) => b,
            None => break,
        };
        let end = col.offset + col.length;
        if end > row_bytes.len() {
            rows.push(DebugRawRow {
                row_idx,
                raw_hex: String::new(),
                trimmed_hex: String::new(),
                decoded: String::new(),
            });
            continue;
        }
        let raw = &row_bytes[col.offset..end];
        let trimmed = trim_sas_bytes(raw);
        let decoded = encoding::decode_string(trimmed, metadata.encoding_byte, encoding);
        rows.push(DebugRawRow {
            row_idx,
            raw_hex: hex(raw),
            trimmed_hex: hex(trimmed),
            decoded,
        });
    }

    Ok(DebugRawColumn {
        encoding_byte: metadata.encoding_byte,
        encoding_name: encoding.name(),
        compression: metadata.compression,
        row_length: metadata.row_length,
        offset: col.offset,
        length: col.length,
        rows,
    })
}
