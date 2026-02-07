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

pub mod arrow_output;
pub mod header;
pub mod metadata;
pub mod reader;

pub use error::{Error, Result};
pub use polars_output::scan_sas7bdat;
pub use reader::Sas7bdatReader;
pub use types::{Compression, Endian, Format, Platform};

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
