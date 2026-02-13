pub(crate) mod compress;
pub(crate) mod data;
pub(crate) mod encoding;
pub(crate) mod error;
pub(crate) mod header;
pub(crate) mod metadata;
pub(crate) mod types;
pub(crate) mod value;

pub mod polars_output;

pub mod arrow_output;
pub mod reader;
pub mod writer;

pub use compress::{compress_df, CompressOptions};
pub use error::{Error, Result};
pub use polars_output::scan_dta;
pub use reader::StataReader;
pub use types::{Endian, Header, Metadata, NumericType, VarType};
pub use writer::{
    pandas_make_stata_column_names, pandas_prepare_df_for_stata, pandas_rename_df,
    StataWriteColumn, StataWriteSchema, StataWriter, ValueLabelMap, ValueLabels, VariableLabels,
};

use serde_json::json;
use std::path::Path;

/// Export Stata metadata as a JSON string.
pub fn metadata_json(path: impl AsRef<Path>) -> Result<String> {
    let reader = StataReader::open(path)?;
    let meta = reader.metadata();
    let variables = meta
        .variables
        .iter()
        .map(|v| {
            json!({
                "name": v.name,
                "type": format!("{:?}", v.var_type),
                "format": v.format,
                "label": v.label,
                "value_label_name": v.value_label_name,
            })
        })
        .collect::<Vec<_>>();
    let v = json!({
        "byte_order": format!("{:?}", meta.byte_order),
        "row_count": meta.row_count,
        "data_label": meta.data_label,
        "timestamp": meta.timestamp,
        "data_offset": meta.data_offset,
        "strls_offset": meta.strls_offset,
        "value_labels_offset": meta.value_labels_offset,
        "encoding": meta.encoding.name(),
        "variables": variables,
    });
    Ok(v.to_string())
}
