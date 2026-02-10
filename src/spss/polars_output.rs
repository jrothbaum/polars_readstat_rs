use crate::spss::data::read_data_columns_uncompressed;
use crate::spss::reader::SpssReader;
use crate::spss::types::FormatClass;
use polars::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::sync::Arc;

pub fn scan_sav(
    path: impl Into<PathBuf>,
    opts: crate::ScanOptions,
) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let user_missing_as_null = opts.user_missing_as_null.unwrap_or(true);
    let preserve_order = opts.preserve_order.unwrap_or(false);
    let scan_ptr = Arc::new(SpssScan::new(
        path,
        opts.threads,
        missing_string_as_null,
        user_missing_as_null,
        opts.value_labels_as_strings,
        opts.chunk_size,
        preserve_order,
        opts.compress_opts,
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
        let mut iter = spss_batch_iter(path, None, true, true, true, Some(10), false, None, Some(25))
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

pub(crate) type SpssBatchIter = Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send>;

struct ParallelSpssBatchIter {
    rx: Receiver<(usize, PolarsResult<DataFrame>)>,
    handle: Option<JoinHandle<()>>,
    preserve_order: bool,
    buffer: BTreeMap<usize, PolarsResult<DataFrame>>,
    next_idx: usize,
    total_chunks: usize,
}

impl Iterator for ParallelSpssBatchIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.preserve_order {
            return self.rx.recv().ok().map(|(_, df)| df);
        }
        if self.next_idx >= self.total_chunks {
            return None;
        }
        loop {
            if let Some(item) = self.buffer.remove(&self.next_idx) {
                self.next_idx += 1;
                return Some(item);
            }
            match self.rx.recv() {
                Ok((idx, df)) => {
                    self.buffer.insert(idx, df);
                }
                Err(_) => {
                    if let Some(item) = self.buffer.remove(&self.next_idx) {
                        self.next_idx += 1;
                        return Some(item);
                    }
                    return None;
                }
            }
        }
    }
}

impl Drop for ParallelSpssBatchIter {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct SerialSpssBatchIter {
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

impl Iterator for SerialSpssBatchIter {
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
    preserve_order: bool,
    cols: Option<Vec<String>>,
    n_rows: Option<usize>,
) -> PolarsResult<SpssBatchIter> {
    let reader = SpssReader::open(&path)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let schema = Arc::new(build_schema(reader.metadata(), value_labels_as_strings));
    let total = n_rows.unwrap_or(reader.metadata().row_count as usize);
    let batch_size = chunk_size.unwrap_or(100_000).max(1);

    let n_threads = threads.unwrap_or_else(|| {
        let cur = rayon::current_num_threads();
        cur.min(4).max(1)
    });
    if reader.compression() == 0 && n_threads > 1 && total >= 1000 {
        let n_chunks = (total + batch_size - 1) / batch_size;
        let (tx, rx) = mpsc::channel::<(usize, PolarsResult<DataFrame>)>();
        let path = Arc::new(path);
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.endian();
        let cols_idx = cols.as_ref().map(|names| {
            names
                .iter()
                .map(|name| {
                    metadata
                        .variables
                        .iter()
                        .position(|v| v.name == *name)
                        .ok_or_else(|| PolarsError::ColumnNotFound(name.clone().into()))
                })
                .collect::<Result<Vec<_>, _>>()
        }).transpose()?;
        let missing_null = missing_string_as_null;
        let user_missing = user_missing_as_null;
        let labels_as_strings = value_labels_as_strings;

        let handle = std::thread::spawn(move || {
            let pool = ThreadPoolBuilder::new().num_threads(n_threads).build();
            if let Ok(pool) = pool {
                pool.install(|| {
                    (0..n_chunks).into_par_iter().for_each_with(tx, |sender, i| {
                        let start = i * batch_size;
                        if start >= total {
                            return;
                        }
                        let end = (total).min(start + batch_size);
                        let cnt = end - start;
                        let result = read_data_columns_uncompressed(
                            &path,
                            &metadata,
                            endian,
                            cols_idx.as_deref(),
                            start,
                            cnt,
                            missing_null,
                            user_missing,
                            labels_as_strings,
                        )
                        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                        .and_then(columns_to_df);
                        let _ = sender.send((i, result));
                    });
                });
            }
        });

        return Ok(Box::new(ParallelSpssBatchIter {
            rx,
            handle: Some(handle),
            preserve_order,
            buffer: BTreeMap::new(),
            next_idx: 0,
            total_chunks: n_chunks,
        }));
    }

    let _ = preserve_order;
    Ok(Box::new(SerialSpssBatchIter {
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
    }))
}

fn columns_to_df(cols: Vec<Series>) -> PolarsResult<DataFrame> {
    let columns = cols.into_iter().map(Column::from).collect::<Vec<_>>();
    DataFrame::new(columns)
}

pub struct SpssScan {
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    user_missing_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
    preserve_order: bool,
    compress_opts: crate::CompressOptionsLite,
}

impl SpssScan {
    pub fn new(
        path: PathBuf,
        threads: Option<usize>,
        missing_string_as_null: bool,
        user_missing_as_null: bool,
        value_labels_as_strings: Option<bool>,
        chunk_size: Option<usize>,
        preserve_order: bool,
        compress_opts: crate::CompressOptionsLite,
    ) -> Self {
        Self {
            path,
            threads,
            missing_string_as_null,
            user_missing_as_null,
            value_labels_as_strings,
            chunk_size,
            preserve_order,
            compress_opts,
        }
    }
}

impl AnonymousScan for SpssScan {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let cols = opts.with_columns.map(|c| c.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let iter = spss_batch_iter(
            self.path.clone(),
            self.threads,
            self.missing_string_as_null,
            self.user_missing_as_null,
            self.value_labels_as_strings.unwrap_or(true),
            self.chunk_size,
            self.preserve_order,
            cols,
            opts.n_rows,
        )?;

        let prefetch = crate::scan_prefetch::spawn_prefetcher(iter.map(|batch| batch));
        let mut out: Option<DataFrame> = None;
        while let Some(df) = prefetch.next()? {
            if let Some(acc) = out.as_mut() {
                acc.vstack_mut(&df).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            } else {
                out = Some(df);
            }
        }
        let df = out.unwrap_or_else(DataFrame::empty);
        if self.compress_opts.enabled {
            let compressed = crate::compress_df_if_enabled(&df, &self.compress_opts)
                .map_err(|e| PolarsError::ComputeError(e.into()))?;
            Ok(compressed)
        } else {
            Ok(df)
        }
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
