use crate::stata::data::{build_shared_decode, read_data_frame_range};
use crate::stata::reader::StataReader;
use crate::stata::types::{NumericType, VarType};
use polars::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::thread::JoinHandle;

pub fn scan_dta(
    path: impl Into<std::path::PathBuf>,
    opts: crate::ScanOptions,
) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let value_labels_as_strings = opts.value_labels_as_strings;
    let preserve_order = opts.preserve_order.unwrap_or(false);
    let scan_ptr = Arc::new(StataScan::new(
        path,
        opts.threads,
        missing_string_as_null,
        value_labels_as_strings,
        opts.chunk_size,
        preserve_order,
        opts.compress_opts,
    ));
    LazyFrame::anonymous_scan(scan_ptr, Default::default())
}

#[cfg(test)]
mod tests {
    use super::stata_batch_iter;
    use std::path::PathBuf;

    fn small_stata_path() -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("stata")
            .join("data");
        let candidates = [
            base.join("sample.dta"),
            base.join("missing_test.dta"),
            base.join("sample_pyreadstat.dta"),
        ];
        for path in candidates {
            if path.exists() {
                return path;
            }
        }
        base.join("sample.dta")
    }

    #[test]
    fn test_stata_batch_streaming() {
        let path = small_stata_path();
        if !path.exists() {
            return;
        }
        let mut iter = stata_batch_iter(path, None, true, true, Some(10), false, None, Some(25))
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

pub struct StataScan {
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: Option<bool>,
    chunk_size: Option<usize>,
    preserve_order: bool,
    compress_opts: crate::CompressOptionsLite,
}

impl StataScan {
    pub fn new(
        path: PathBuf,
        threads: Option<usize>,
        missing_string_as_null: bool,
        value_labels_as_strings: Option<bool>,
        chunk_size: Option<usize>,
        preserve_order: bool,
        compress_opts: crate::CompressOptionsLite,
    ) -> Self {
        Self {
            path,
            threads,
            missing_string_as_null,
            value_labels_as_strings,
            chunk_size,
            preserve_order,
            compress_opts,
        }
    }
}

pub(crate) type StataBatchIter = Box<dyn Iterator<Item = PolarsResult<DataFrame>> + Send>;

struct ParallelStataBatchIter {
    rx: Receiver<(usize, PolarsResult<DataFrame>)>,
    handle: Option<JoinHandle<()>>,
    preserve_order: bool,
    buffer: BTreeMap<usize, PolarsResult<DataFrame>>,
    next_idx: usize,
    total_chunks: usize,
}

impl Iterator for ParallelStataBatchIter {
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

impl Drop for ParallelStataBatchIter {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct SerialStataBatchIter {
    reader: StataReader,
    cols: Option<Vec<String>>,
    time_formats: Vec<(String, StataTimeFormatKind)>,
    offset: usize,
    remaining: usize,
    batch_size: usize,
    threads: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    chunk_size: Option<usize>,
}

impl Iterator for SerialStataBatchIter {
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
        let out = builder
            .finish()
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
            .and_then(|mut df| {
                apply_stata_time_formats(&mut df, &self.time_formats)?;
                Ok(df)
            });
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
    preserve_order: bool,
    cols: Option<Vec<String>>,
    n_rows: Option<usize>,
) -> PolarsResult<StataBatchIter> {
    let reader =
        StataReader::open(&path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let total = n_rows.unwrap_or(reader.metadata().row_count as usize);
    let batch_size = chunk_size.unwrap_or(100_000).max(1);
    let selected = cols
        .as_ref()
        .map(|c| c.iter().cloned().collect::<HashSet<_>>());
    let col_indices = cols
        .as_ref()
        .map(|names| {
            names
                .iter()
                .map(|name| {
                    reader
                        .metadata()
                        .variables
                        .iter()
                        .position(|v| v.name == *name)
                        .ok_or_else(|| PolarsError::ColumnNotFound(name.clone().into()))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let mut time_formats = Vec::new();
    for var in &reader.metadata().variables {
        if let Some(kind) = stata_time_format_kind(var.format.as_deref(), &var.var_type) {
            if selected
                .as_ref()
                .map(|s| s.contains(&var.name))
                .unwrap_or(true)
            {
                time_formats.push((var.name.clone(), kind));
            }
        }
    }

    let n_threads = threads.unwrap_or_else(|| {
        let cur = rayon::current_num_threads();
        cur.min(4).max(1)
    });
    if n_threads > 1 && total >= 1000 {
        let n_chunks = (total + batch_size - 1) / batch_size;
        let (tx, rx) = mpsc::channel::<(usize, PolarsResult<DataFrame>)>();
        let path = Arc::new(path);
        let metadata = Arc::new(reader.metadata().clone());
        let endian = reader.header().endian;
        let version = reader.header().version;
        let cols_idx = col_indices.clone();
        let formats = Arc::new(time_formats);
        let missing_null = missing_string_as_null;
        let labels_as_strings = value_labels_as_strings;
        let shared = build_shared_decode(&path, &metadata, endian, version, labels_as_strings)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let shared = Arc::new(shared);

        let handle = std::thread::spawn(move || {
            let pool = ThreadPoolBuilder::new().num_threads(n_threads).build();
            if let Ok(pool) = pool {
                pool.install(|| {
                    (0..n_chunks)
                        .into_par_iter()
                        .for_each_with(tx, |sender, i| {
                            let start = i * batch_size;
                            if start >= total {
                                return;
                            }
                            let end = (total).min(start + batch_size);
                            let cnt = end - start;
                            let result = read_data_frame_range(
                                &path,
                                &metadata,
                                endian,
                                version,
                                cols_idx.as_deref(),
                                start,
                                cnt,
                                missing_null,
                                labels_as_strings,
                                &shared,
                            )
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                            .and_then(|mut df| {
                                apply_stata_time_formats(&mut df, &formats)?;
                                Ok(df)
                            });
                            let _ = sender.send((i, result));
                        });
                });
            }
        });

        return Ok(Box::new(ParallelStataBatchIter {
            rx,
            handle: Some(handle),
            preserve_order,
            buffer: BTreeMap::new(),
            next_idx: 0,
            total_chunks: n_chunks,
        }));
    }

    let _ = preserve_order;
    Ok(Box::new(SerialStataBatchIter {
        reader,
        cols,
        time_formats,
        offset: 0,
        remaining: total,
        batch_size,
        threads,
        missing_string_as_null,
        value_labels_as_strings,
        chunk_size,
    }))
}

impl AnonymousScan for StataScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let cols = opts
            .with_columns
            .map(|c| c.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let iter = stata_batch_iter(
            self.path.clone(),
            self.threads,
            self.missing_string_as_null,
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
                acc.vstack_mut(&df)
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
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
        let reader = StataReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut schema = Schema::with_capacity(reader.metadata().variables.len());
        for var in &reader.metadata().variables {
            let use_labels = self.value_labels_as_strings.unwrap_or(true);
            let dtype = if use_labels && var.value_label_name.is_some() {
                DataType::String
            } else {
                if let Some(kind) = stata_time_format_kind(var.format.as_deref(), &var.var_type) {
                    match kind {
                        StataTimeFormatKind::Date => DataType::Date,
                        StataTimeFormatKind::DateTime => {
                            DataType::Datetime(TimeUnit::Milliseconds, None)
                        }
                        StataTimeFormatKind::Time { .. } => DataType::Time,
                    }
                } else {
                    match var.var_type {
                        VarType::Numeric(NumericType::Byte) => DataType::Int8,
                        VarType::Numeric(NumericType::Int) => DataType::Int16,
                        VarType::Numeric(NumericType::Long) => DataType::Int32,
                        VarType::Numeric(NumericType::Float) => DataType::Float32,
                        VarType::Numeric(NumericType::Double) => DataType::Float64,
                        VarType::Str(_) | VarType::StrL => DataType::String,
                    }
                }
            };
            schema.with_column(var.name.as_str().into(), dtype);
        }

        Ok(Arc::new(schema))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StataTimeFormatKind {
    Date,
    DateTime,
    Time { null_on_datetime: bool },
}

fn allow_date(var_type: &VarType) -> bool {
    matches!(
        var_type,
        VarType::Numeric(NumericType::Byte)
            | VarType::Numeric(NumericType::Int)
            | VarType::Numeric(NumericType::Long)
            | VarType::Numeric(NumericType::Float)
            | VarType::Numeric(NumericType::Double)
    )
}

fn allow_datetime(var_type: &VarType) -> bool {
    matches!(
        var_type,
        VarType::Numeric(NumericType::Long)
            | VarType::Numeric(NumericType::Float)
            | VarType::Numeric(NumericType::Double)
    )
}

pub(crate) fn stata_time_format_kind(
    format: Option<&str>,
    var_type: &VarType,
) -> Option<StataTimeFormatKind> {
    let fmt = format?.trim();
    if fmt.starts_with("%t") {
        let mut chars = fmt.chars();
        chars.next();
        chars.next();
        let unit = chars.next()?;
        return match unit {
            'c' | 'C' => {
                let rest: String = chars.collect();
                if rest.is_empty() {
                    if allow_datetime(var_type) {
                        Some(StataTimeFormatKind::DateTime)
                    } else {
                        None
                    }
                } else {
                    if allow_datetime(var_type) {
                        let has_date_tokens = rest
                            .chars()
                            .any(|c| matches!(c, 'C' | 'c' | 'Y' | 'y' | 'N' | 'n' | 'D' | 'd'));
                        Some(StataTimeFormatKind::Time {
                            null_on_datetime: has_date_tokens,
                        })
                    } else {
                        None
                    }
                }
            }
            'd' | 'w' | 'm' | 'q' | 'h' | 'y' => {
                if allow_date(var_type) {
                    Some(StataTimeFormatKind::Date)
                } else {
                    None
                }
            }
            _ => None,
        };
    }

    if fmt.starts_with('%') {
        let mut chars = fmt.chars();
        chars.next();
        let unit = chars.next()?;
        return match unit {
            'c' | 'C' => {
                if allow_datetime(var_type) {
                    Some(StataTimeFormatKind::DateTime)
                } else {
                    None
                }
            }
            'd' | 'w' | 'm' | 'q' | 'h' | 'y' => {
                if allow_date(var_type) {
                    Some(StataTimeFormatKind::Date)
                } else {
                    None
                }
            }
            _ => None,
        };
    }

    None
}

pub(crate) fn apply_stata_time_formats(
    df: &mut DataFrame,
    formats: &[(String, StataTimeFormatKind)],
) -> PolarsResult<()> {
    if formats.is_empty() {
        return Ok(());
    }
    let offset_days: i64 = 3653;
    let offset_ms: i64 = offset_days * 86_400_000;
    let day_ms: i64 = 86_400_000;
    let mut exprs = Vec::with_capacity(formats.len());
    for (name, kind) in formats {
        let dtype_ok = df
            .column(name)
            .map(|s| s.dtype().is_numeric())
            .unwrap_or(false);
        if !dtype_ok {
            continue;
        }
        let expr = match kind {
            StataTimeFormatKind::Date => (col(name).cast(DataType::Int64) - lit(offset_days))
                .cast(DataType::Date)
                .alias(name),
            StataTimeFormatKind::DateTime => (col(name).cast(DataType::Int64) - lit(offset_ms))
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .alias(name),
            StataTimeFormatKind::Time { null_on_datetime } => {
                if *null_on_datetime {
                    lit(NULL).cast(DataType::Time).alias(name)
                } else {
                    ((col(name).cast(DataType::Int64) % lit(day_ms) + lit(day_ms)) % lit(day_ms)
                        * lit(1_000_000i64))
                    .cast(DataType::Time)
                    .alias(name)
                }
            }
        };
        exprs.push(expr);
    }
    let df_owned = std::mem::take(df);
    *df = df_owned
        .lazy()
        .with_columns(exprs)
        .collect()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    Ok(())
}
