//! Pure Rust SAS7BDAT reader with Polars and Arrow output
//!
//! This library reads SAS7BDAT files and outputs Polars DataFrames or Apache Arrow RecordBatches.
//! It supports all format variants (32/64-bit, big/little endian) and
//! all compression types (None, RLE, RDC).

pub mod sas;
pub mod spss;
pub mod stata;
pub(crate) mod scan_prefetch;

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
pub use sas::{
    SasValueLabelKey,
    SasValueLabelMap,
    SasValueLabels,
    SasVariableLabels,
    SasWriter,
};

pub use sas::scan_sas7bdat;

pub use spss::{Error as SpssError, Result as SpssResult, SpssReader, scan_sav};
pub use spss::{
    SpssValueLabelKey,
    SpssValueLabelMap,
    SpssValueLabels,
    SpssVariableLabels,
    SpssWriteColumn,
    SpssWriteSchema,
    SpssWriter,
};
pub use stata::{
    Error as StataError,
    Result as StataResult,
    StataReader,
    StataWriteColumn,
    StataWriteSchema,
    StataWriter,
    ValueLabelMap,
    ValueLabels,
    VariableLabels,
    compress_df,
    CompressOptions,
    pandas_make_stata_column_names,
    pandas_prepare_df_for_stata,
    pandas_rename_df,
    scan_dta,
};

use std::path::Path;
use polars::prelude::DataFrame;

#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub threads: Option<usize>,
    pub chunk_size: Option<usize>,
    pub missing_string_as_null: Option<bool>,
    pub user_missing_as_null: Option<bool>,
    pub value_labels_as_strings: Option<bool>,
    pub preserve_order: Option<bool>,
    pub compress_opts: CompressOptionsLite,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            threads: None,
            chunk_size: None,
            missing_string_as_null: Some(true),
            user_missing_as_null: Some(true),
            value_labels_as_strings: Some(true),
            preserve_order: Some(false),
            compress_opts: CompressOptionsLite::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompressOptionsLite {
    pub enabled: bool,
    pub cols: Option<Vec<String>>,
    pub compress_numeric: bool,
    pub datetime_to_date: bool,
    pub string_to_numeric: bool,
}

impl Default for CompressOptionsLite {
    fn default() -> Self {
        Self {
            enabled: false,
            cols: None,
            compress_numeric: false,
            datetime_to_date: false,
            string_to_numeric: false,
        }
    }
}

pub fn compress_df_if_enabled(
    df: &DataFrame,
    opts: &CompressOptionsLite,
) -> std::result::Result<DataFrame, String> {
    if !opts.enabled {
        return Ok(df.clone());
    }
    let mut compress_opts = CompressOptions::default();
    compress_opts.compress_numeric = opts.compress_numeric;
    compress_opts.check_date_time = opts.datetime_to_date;
    compress_opts.check_string = opts.string_to_numeric;
    compress_opts.check_string_only = false;
    compress_opts.cast_all_null_to_boolean = true;
    compress_opts.no_boolean = false;
    compress_opts.cols = opts.cols.clone();
    compress_opts.use_stata_bounds = false;
    compress_df(df, compress_opts).map_err(|e| e.to_string())
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
