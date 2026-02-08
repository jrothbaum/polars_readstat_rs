pub(crate) mod error;
pub(crate) mod header;
pub(crate) mod metadata;
pub(crate) mod types;
pub mod writer;
pub(crate) mod data;
pub(crate) mod reader;
pub mod polars_output;
pub mod arrow_output;

pub use error::{Error, Result};
pub use reader::SpssReader;
pub use writer::{
    SpssValueLabelKey,
    SpssValueLabelMap,
    SpssValueLabels,
    SpssVariableLabels,
    SpssWriteColumn,
    SpssWriteSchema,
    SpssWriter,
};
pub use polars_output::scan_sav;
pub use types::{Endian, Header, Metadata, VarType};

use serde_json::json;
use std::path::Path;

/// Export SPSS metadata as a JSON string.
pub fn metadata_json(path: impl AsRef<Path>) -> Result<String> {
    let reader = SpssReader::open(path)?;
    let meta = reader.metadata();
    let variables = meta.variables.iter().map(|v| {
        json!({
            "name": v.name,
            "type": format!("{:?}", v.var_type),
            "string_len": v.string_len,
            "label": v.label,
            "value_label": v.value_label,
            "missing_range": v.missing_range,
            "missing_doubles": v.missing_doubles,
            "missing_strings": v.missing_strings,
        })
    }).collect::<Vec<_>>();
    let v = json!({
        "row_count": meta.row_count,
        "data_offset": meta.data_offset,
        "encoding": meta.encoding.name(),
        "variables": variables,
    });
    Ok(v.to_string())
}
