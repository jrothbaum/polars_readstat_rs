//! Pure Rust SAS7BDAT reader with Polars and Arrow output
//!
//! This library reads SAS7BDAT files and outputs Polars DataFrames or Apache Arrow RecordBatches.
//! It supports all format variants (32/64-bit, big/little endian) and
//! all compression types (None, RLE, RDC).

pub mod sas;
pub mod spss;
pub mod stata;

pub use sas::arrow_output as sas_arrow_output;
pub use sas::polars_output as sas_polars_output;
pub use spss::arrow_output as spss_arrow_output;
pub use spss::polars_output as spss_polars_output;
pub use stata::arrow_output as stata_arrow_output;
pub use stata::polars_output as stata_polars_output;
pub(crate) use sas::buffer;
pub(crate) use sas::constants;
pub(crate) use sas::data;
pub(crate) use sas::decompressor;
pub(crate) use sas::encoding;
pub(crate) use sas::error;
pub(crate) use sas::page;
pub(crate) use sas::pipeline;
pub(crate) use sas::polars_output;
pub(crate) use sas::types;
pub(crate) use sas::value;

pub use sas::header;
pub use sas::metadata;
pub use sas::reader;

pub use sas::{Error, Result, Sas7bdatReader};
pub use sas::{Compression, Endian, Format, Platform};

pub use sas::scan_sas7bdat;

pub use spss::{Error as SpssError, Result as SpssResult, SpssReader, scan_sav};
pub use stata::{Error as StataError, Result as StataResult, StataReader, scan_dta};

use std::path::Path;

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub threads: Option<usize>,
    pub chunk_size: Option<usize>,
    pub missing_string_as_null: Option<bool>,
    pub value_labels_as_strings: Option<bool>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            threads: None,
            chunk_size: None,
            missing_string_as_null: Some(true),
            value_labels_as_strings: Some(true),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStatFormat {
    Sas,
    Stata,
    Spss,
}

fn detect_format(path: &Path) -> Option<ReadStatFormat> {
    let ext = path.extension().and_then(|s| s.to_str())?.to_ascii_lowercase();
    match ext.as_str() {
        "sas7bdat" | "sas7bcat" => Some(ReadStatFormat::Sas),
        "dta" => Some(ReadStatFormat::Stata),
        "sav" | "zsav" => Some(ReadStatFormat::Spss),
        _ => None,
    }
}

/// Format-agnostic scan that dispatches by file extension.
pub fn readstat_scan(
    path: impl AsRef<Path>,
    opts: Option<ScanOptions>,
    format: Option<ReadStatFormat>,
) -> polars::prelude::PolarsResult<polars::prelude::LazyFrame> {
    let path = path.as_ref();
    let opts = opts.unwrap_or_default();
    let format = format.or_else(|| detect_format(path))
        .ok_or_else(|| polars::prelude::PolarsError::ComputeError("unknown file extension".into()))?;

    match format {
        ReadStatFormat::Sas => sas::scan_sas7bdat(
            path,
            opts,
        ),
        ReadStatFormat::Stata => stata::scan_dta(
            path,
            opts,
        ),
        ReadStatFormat::Spss => spss::scan_sav(
            path,
            opts,
        ),
    }
}

/// Format-agnostic schema (delegates to AnonymousScan).
pub fn readstat_schema(
    path: impl AsRef<Path>,
    opts: Option<ScanOptions>,
    format: Option<ReadStatFormat>,
) -> polars::prelude::PolarsResult<polars::prelude::SchemaRef> {
    readstat_scan(path, opts, format)?.collect_schema()
}

/// Format-agnostic metadata JSON (dispatches by extension).
pub fn readstat_metadata_json(
    path: impl AsRef<Path>,
    format: Option<ReadStatFormat>,
) -> std::result::Result<String, String> {
    let path = path.as_ref();
    let format = format.or_else(|| detect_format(path)).ok_or("unknown file extension".to_string())?;
    match format {
        ReadStatFormat::Sas => sas::metadata_json(path).map_err(|e| e.to_string()),
        ReadStatFormat::Stata => stata::metadata_json(path).map_err(|e| e.to_string()),
        ReadStatFormat::Spss => spss::metadata_json(path).map_err(|e| e.to_string()),
    }
}
