use crate::spss::reader::SpssReader;
use crate::spss::types::FormatClass;
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

pub fn scan_sav(
    path: impl Into<PathBuf>,
    opts: crate::ScanOptions,
) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let user_missing_as_null = opts.user_missing_as_null.unwrap_or(true);
    let scan_ptr = Arc::new(SpssScan::new(
        path,
        opts.threads,
        missing_string_as_null,
        user_missing_as_null,
        opts.value_labels_as_strings,
        opts.chunk_size,
    ));
    LazyFrame::anonymous_scan(scan_ptr, Default::default())
}

#[cfg(test)]
mod tests {
    use super::spss_batch_iter;
    use std::path::PathBuf;

    fn small_spss_path() -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("spss")
            .join("data");
        let candidates = [
            base.join("sample.sav"),
            base.join("labelled-num.sav"),
            base.join("missing_test.sav"),
        ];
        for path in candidates {
            if path.exists() {
                return path;
            }
        }
        base.join("sample.sav")
    }

    #[test]
    fn test_spss_batch_streaming() {
        let path = small_spss_path();
        if !path.exists() {
            return;
        }
        let mut iter = spss_batch_iter(path, None, true, true, true, Some(10), None, Some(25))
            .expect("batch iter");
        let mut batches = 0usize;
        let mut rows = 0usize;
        while let Some(batch) = iter.next() {
            let df = batch.expect("batch");
            rows += df.height();
            batches += 1;
        }
        assert!(batches >= 1);
        assert!(rows <= 25);
    }
}

pub(crate) struct SpssBatchIter {
    reader: SpssReader,
    cols: Option<Vec<String>>,
    offset: usize,
    remaining: usize,
    batch_size: usize,
    threads: Option<usize>,
    missing_string_as_null: bool,
    user_missing_as_null: bool,
    value_labels_as_strings: bool,
    chunk_size: Option<usize>,
    schema: Arc<Schema>,
}

impl Iterator for SpssBatchIter {
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
            .user_missing_as_null(self.user_missing_as_null)
            .value_labels_as_strings(self.value_labels_as_strings);
        builder = builder.with_schema(self.schema.clone());
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

pub(crate) fn spss_batch_iter(
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    user_missing_as_null: bool,
    value_labels_as_strings: bool,
    chunk_size: Option<usize>,
    cols: Option<Vec<String>>,
    n_rows: Option<usize>,
) -> PolarsResult<SpssBatchIter> {
    let reader = SpssReader::open(&path)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let schema = Arc::new(build_schema(reader.metadata(), value_labels_as_strings));
    let total = n_rows.unwrap_or(reader.metadata().row_count as usize);
    let batch_size = chunk_size.unwrap_or(100_000).max(1);
    Ok(SpssBatchIter {
        reader,
        cols,
        offset: 0,
        remaining: total,
        batch_size,
        threads,
        missing_string_as_null,
        user_missing_as_null,
        value_labels_as_strings,
        chunk_size,
        schema,
    })
}

pub struct SpssScan {
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    user_missing_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
}

impl SpssScan {
    pub fn new(
        path: PathBuf,
        threads: Option<usize>,
        missing_string_as_null: bool,
        user_missing_as_null: bool,
        value_labels_as_strings: Option<bool>,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            path,
            threads,
            missing_string_as_null,
            user_missing_as_null,
            value_labels_as_strings,
            chunk_size,
        }
    }
}

impl AnonymousScan for SpssScan {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let cols = opts.with_columns.map(|c| c.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let mut iter = spss_batch_iter(
            self.path.clone(),
            self.threads,
            self.missing_string_as_null,
            self.user_missing_as_null,
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
        let reader = SpssReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        Ok(Arc::new(build_schema(
            reader.metadata(),
            self.value_labels_as_strings.unwrap_or(true),
        )))
    }
}

fn build_schema(
    metadata: &crate::spss::types::Metadata,
    value_labels_as_strings: bool,
) -> Schema {
    let mut schema = Schema::with_capacity(metadata.variables.len());
    for var in &metadata.variables {
        let dtype = if value_labels_as_strings && var.value_label.is_some() {
            DataType::String
        } else {
            match var.var_type {
                crate::spss::types::VarType::Numeric => match var.format_class {
                    Some(FormatClass::Date) => DataType::Date,
                    Some(FormatClass::DateTime) => DataType::Datetime(TimeUnit::Milliseconds, None),
                    Some(FormatClass::Time) => DataType::Time,
                    None => DataType::Float64,
                },
                crate::spss::types::VarType::Str => DataType::String,
            }
        };
        schema.with_column(var.name.as_str().into(), dtype);
    }
    schema
}
