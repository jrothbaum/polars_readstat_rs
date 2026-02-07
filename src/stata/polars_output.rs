use crate::stata::reader::StataReader;
use crate::stata::types::{VarType, NumericType};
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

pub fn scan_dta(
    path: impl Into<std::path::PathBuf>,
    opts: crate::ScanOptions,
) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let value_labels_as_strings = opts.value_labels_as_strings;
    let scan_ptr = Arc::new(StataScan::new(
        path,
        opts.threads,
        missing_string_as_null,
        value_labels_as_strings,
        opts.chunk_size,
    ));
    LazyFrame::anonymous_scan(scan_ptr, Default::default())
}

#[cfg(test)]
mod tests {
    use super::stata_batch_iter;
    use std::path::PathBuf;

    fn big_stata_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("stata")
            .join("data")
            .join("usa_00009.dta")
    }

    #[test]
    fn test_stata_batch_streaming() {
        let path = big_stata_path();
        if !path.exists() {
            return;
        }
        let mut iter = stata_batch_iter(path, None, true, true, Some(50_000), None, Some(120_000))
            .expect("batch iter");
        let mut batches = 0usize;
        let mut rows = 0usize;
        while let Some(batch) = iter.next() {
            let df = batch.expect("batch");
            rows += df.height();
            batches += 1;
        }
        assert!(batches >= 2);
        assert_eq!(rows, 120_000);
    }
}

pub struct StataScan {
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
}

impl StataScan {
    pub fn new(
        path: PathBuf,
        threads: Option<usize>,
        missing_string_as_null: bool,
        value_labels_as_strings: Option<bool>,
        chunk_size: Option<usize>,
    ) -> Self {
        Self { path, threads, missing_string_as_null, value_labels_as_strings, chunk_size }
    }
}

pub(crate) struct StataBatchIter {
    reader: StataReader,
    cols: Option<Vec<String>>,
    offset: usize,
    remaining: usize,
    batch_size: usize,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    chunk_size: Option<usize>,
}

impl Iterator for StataBatchIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let take = self.batch_size.min(self.remaining);
        let mut builder = self
            .reader
            .read()
            .with_offset(self.offset)
            .with_limit(take)
            .missing_string_as_null(self.missing_string_as_null)
            .value_labels_as_strings(self.value_labels_as_strings);
        if let Some(n) = self.threads {
            builder = builder.with_n_threads(n);
        }
        if let Some(n) = self.chunk_size {
            builder = builder.with_chunk_size(n);
        }
        if let Some(cols) = &self.cols {
            builder = builder.with_columns(cols.clone());
        }
        let out = builder.finish().map_err(|e| PolarsError::ComputeError(e.to_string().into()));
        self.offset += take;
        self.remaining -= take;
        Some(out)
    }
}

pub(crate) fn stata_batch_iter(
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    chunk_size: Option<usize>,
    cols: Option<Vec<String>>,
    n_rows: Option<usize>,
) -> PolarsResult<StataBatchIter> {
    let reader = StataReader::open(&path)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let total = n_rows.unwrap_or(reader.metadata().row_count as usize);
    let batch_size = chunk_size.unwrap_or(100_000).max(1);
    Ok(StataBatchIter {
        reader,
        cols,
        offset: 0,
        remaining: total,
        batch_size,
        threads,
        missing_string_as_null,
        value_labels_as_strings,
        chunk_size,
    })
}

impl AnonymousScan for StataScan {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let cols = opts.with_columns.map(|c| c.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let mut iter = stata_batch_iter(
            self.path.clone(),
            self.threads,
            self.missing_string_as_null,
            self.value_labels_as_strings.unwrap_or(true),
            self.chunk_size,
            cols,
            opts.n_rows,
        )?;

        let mut out: Option<DataFrame> = None;
        while let Some(batch) = iter.next() {
            let df = batch?;
            if let Some(acc) = out.as_mut() {
                acc.vstack_mut(&df).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            } else {
                out = Some(df);
            }
        }
        Ok(out.unwrap_or_else(DataFrame::empty))
    }

    fn schema(&self, _n_rows: Option<usize>) -> PolarsResult<SchemaRef> {
        let reader = StataReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut schema = Schema::with_capacity(reader.metadata().variables.len());
        for var in &reader.metadata().variables {
            let use_labels = self.value_labels_as_strings.unwrap_or(true);
            let dtype = if use_labels && var.value_label_name.is_some() {
                DataType::String
            } else {
                match var.var_type {
                    VarType::Numeric(NumericType::Byte) => DataType::Int8,
                    VarType::Numeric(NumericType::Int) => DataType::Int16,
                    VarType::Numeric(NumericType::Long) => DataType::Int32,
                    VarType::Numeric(NumericType::Float) => DataType::Float32,
                    VarType::Numeric(NumericType::Double) => DataType::Float64,
                    VarType::Str(_) | VarType::StrL => DataType::String,
                }
            };
            schema.with_column(var.name.as_str().into(), dtype);
        }

        Ok(Arc::new(schema))
    }
}
