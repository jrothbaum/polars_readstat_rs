use crate::constants::{
    DATETIME_FORMATS, DATE_FORMATS, SAS_EPOCH_OFFSET_DAYS, SECONDS_PER_DAY, TIME_FORMATS,
};
use crate::error::{Error, Result};
use crate::reader::Sas7bdatReader;
use crate::types::{Column as SasColumn, ColumnType, Metadata};
use crate::value::Value;
use polars::prelude::*;
use std::cmp::min;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;

/// Build a Polars DataFrame from rows of parsed values
pub struct DataFrameBuilder {
    columns_meta: Vec<SasColumn>,
    buffers: Vec<ColumnBuffer>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ColumnKind {
    Numeric,
    Date,
    DateTime,
    Time,
    Character,
}

enum ColumnBuffer {
    Numeric(PrimitiveChunkedBuilder<Float64Type>),
    Date(PrimitiveChunkedBuilder<Int32Type>),
    DateTime(PrimitiveChunkedBuilder<Int64Type>),
    Time(PrimitiveChunkedBuilder<Int64Type>),
    Character(StringChunkedBuilder),
}

impl DataFrameBuilder {
    pub fn new(metadata: Metadata, capacity: usize) -> Self {
        let columns_meta = metadata.columns;
        let buffers = columns_meta
            .iter()
            .map(|col| ColumnBuffer::with_capacity(kind_for_column(col), &col.name, capacity))
            .collect();
        Self {
            columns_meta,
            buffers,
        }
    }

    pub fn new_with_columns(
        metadata: &Metadata,
        column_indices: &[usize],
        capacity: usize,
    ) -> Self {
        let columns_meta: Vec<SasColumn> = column_indices
            .iter()
            .map(|&idx| metadata.columns[idx].clone())
            .collect();
        let buffers = columns_meta
            .iter()
            .map(|col| ColumnBuffer::with_capacity(kind_for_column(col), &col.name, capacity))
            .collect();
        Self {
            columns_meta,
            buffers,
        }
    }

    pub fn add_row_ref(&mut self, row: &[Value]) -> Result<()> {
        if row.len() != self.columns_meta.len() {
            return Err(Error::ColumnCountMismatch {
                expected: self.columns_meta.len(),
                actual: row.len(),
            });
        }
        for (idx, value) in row.iter().enumerate() {
            let column = &self.columns_meta[idx];
            let buffer = &mut self.buffers[idx];
            push_value_ref(buffer, column, value);
        }
        Ok(())
    }

    pub(crate) fn add_numeric_value_mask_kind(
        &mut self,
        col_pos: usize,
        kind: ColumnKind,
        value: f64,
        is_missing: bool,
    ) {
        let buffer = &mut self.buffers[col_pos];
        match kind {
            ColumnKind::Numeric => {
                if let ColumnBuffer::Numeric(builder) = buffer {
                    if is_missing {
                        builder.append_null();
                    } else {
                        builder.append_value(value);
                    }
                } else {
                    debug_assert!(false, "Expected numeric buffer");
                }
            }
            ColumnKind::Date => {
                if let ColumnBuffer::Date(builder) = buffer {
                    if is_missing {
                        builder.append_null();
                    } else {
                        builder.append_value(to_date_value(value));
                    }
                } else {
                    debug_assert!(false, "Expected date buffer");
                }
            }
            ColumnKind::DateTime => {
                if let ColumnBuffer::DateTime(builder) = buffer {
                    if is_missing {
                        builder.append_null();
                    } else {
                        builder.append_value(to_datetime_value(value));
                    }
                } else {
                    debug_assert!(false, "Expected datetime buffer");
                }
            }
            ColumnKind::Time => {
                if let ColumnBuffer::Time(builder) = buffer {
                    if is_missing {
                        builder.append_null();
                    } else {
                        builder.append_value(to_time_value(value));
                    }
                } else {
                    debug_assert!(false, "Expected time buffer");
                }
            }
            ColumnKind::Character => {
                if let ColumnBuffer::Character(builder) = buffer {
                    builder.append_null();
                } else {
                    debug_assert!(false, "Expected character buffer");
                }
            }
        }
    }

    pub fn build(self) -> Result<DataFrame> {
        let mut columns = Vec::with_capacity(self.columns_meta.len());
        for (_column, buffer) in self.columns_meta.iter().zip(self.buffers.into_iter()) {
            let series = match buffer {
                ColumnBuffer::Numeric(builder) => builder.finish().into_series(),
                ColumnBuffer::Date(builder) => {
                    builder.finish().into_series().cast(&DataType::Date)?
                }
                ColumnBuffer::DateTime(builder) => builder
                    .finish()
                    .into_series()
                    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?,
                ColumnBuffer::Time(builder) => {
                    builder.finish().into_series().cast(&DataType::Time)?
                }
                ColumnBuffer::Character(builder) => builder.finish().into_series(),
            };
            columns.push(series.into());
        }
        DataFrame::new_infer_height(columns).map_err(|e| e.into())
    }
}

impl ColumnBuffer {
    fn with_capacity(kind: ColumnKind, name: &str, capacity: usize) -> Self {
        match kind {
            ColumnKind::Numeric => ColumnBuffer::Numeric(
                PrimitiveChunkedBuilder::<Float64Type>::new(name.into(), capacity),
            ),
            ColumnKind::Date => ColumnBuffer::Date(PrimitiveChunkedBuilder::<Int32Type>::new(
                name.into(),
                capacity,
            )),
            ColumnKind::DateTime => ColumnBuffer::DateTime(
                PrimitiveChunkedBuilder::<Int64Type>::new(name.into(), capacity),
            ),
            ColumnKind::Time => ColumnBuffer::Time(PrimitiveChunkedBuilder::<Int64Type>::new(
                name.into(),
                capacity,
            )),
            ColumnKind::Character => {
                ColumnBuffer::Character(StringChunkedBuilder::new(name.into(), capacity))
            }
        }
    }
}

pub(crate) fn kind_for_column(column: &SasColumn) -> ColumnKind {
    match column.col_type {
        ColumnType::Character => ColumnKind::Character,
        ColumnType::Numeric => {
            // IMPORTANT: Check DATETIME before DATE since "DATETIME" starts with "DATE"
            if is_datetime_format(&column.format) {
                ColumnKind::DateTime
            } else if is_date_format(&column.format) {
                ColumnKind::Date
            } else if is_time_format(&column.format) {
                ColumnKind::Time
            } else {
                ColumnKind::Numeric
            }
        }
    }
}

fn push_value_ref(buffer: &mut ColumnBuffer, column: &SasColumn, value: &Value) {
    match (buffer, value, column.col_type) {
        (ColumnBuffer::Numeric(builder), Value::Numeric(v), ColumnType::Numeric) => {
            builder.append_option(*v)
        }
        (ColumnBuffer::Date(builder), Value::Numeric(v), ColumnType::Numeric) => {
            builder.append_option(to_date(*v))
        }
        (ColumnBuffer::DateTime(builder), Value::Numeric(v), ColumnType::Numeric) => {
            builder.append_option(to_datetime(*v))
        }
        (ColumnBuffer::Time(builder), Value::Numeric(v), ColumnType::Numeric) => {
            builder.append_option(to_time(*v))
        }
        (ColumnBuffer::Character(builder), Value::Character(v), ColumnType::Character) => {
            if let Some(s) = v {
                builder.append_value(s);
            } else {
                builder.append_null();
            }
        }
        (ColumnBuffer::Numeric(builder), _, _) => builder.append_null(),
        (ColumnBuffer::Date(builder), _, _) => builder.append_null(),
        (ColumnBuffer::DateTime(builder), _, _) => builder.append_null(),
        (ColumnBuffer::Time(builder), _, _) => builder.append_null(),
        (ColumnBuffer::Character(builder), _, _) => builder.append_null(),
    }
}

fn to_date(value: Option<f64>) -> Option<i32> {
    value.map(|sas_value| {
        let days_since_1970 = (sas_value as i32) - SAS_EPOCH_OFFSET_DAYS;
        if days_since_1970 >= -135080 && days_since_1970 <= 156935 {
            days_since_1970
        } else {
            (sas_value / SECONDS_PER_DAY as f64) as i32 - SAS_EPOCH_OFFSET_DAYS
        }
    })
}

fn to_date_value(sas_value: f64) -> i32 {
    let days_since_1970 = (sas_value as i32) - SAS_EPOCH_OFFSET_DAYS;
    if days_since_1970 >= -135080 && days_since_1970 <= 156935 {
        days_since_1970
    } else {
        (sas_value / SECONDS_PER_DAY as f64) as i32 - SAS_EPOCH_OFFSET_DAYS
    }
}

fn to_datetime(value: Option<f64>) -> Option<i64> {
    value.map(|sas_seconds| {
        let unix_seconds = sas_seconds - (SAS_EPOCH_OFFSET_DAYS as f64 * SECONDS_PER_DAY as f64);
        (unix_seconds * 1_000_000.0) as i64
    })
}

fn to_datetime_value(sas_seconds: f64) -> i64 {
    let unix_seconds = sas_seconds - (SAS_EPOCH_OFFSET_DAYS as f64 * SECONDS_PER_DAY as f64);
    (unix_seconds * 1_000_000.0) as i64
}

fn to_time(value: Option<f64>) -> Option<i64> {
    value.map(|sas_seconds| (sas_seconds * 1_000_000_000.0) as i64)
}

fn to_time_value(sas_seconds: f64) -> i64 {
    (sas_seconds * 1_000_000_000.0) as i64
}

/// Check if format string indicates a date column
fn is_date_format(format: &str) -> bool {
    if format.is_empty() {
        return false;
    }
    let upper = format.to_uppercase();
    DATE_FORMATS.iter().any(|&fmt| upper.starts_with(fmt))
}

/// Check if format string indicates a datetime column
fn is_datetime_format(format: &str) -> bool {
    if format.is_empty() {
        return false;
    }
    let upper = format.to_uppercase();
    DATETIME_FORMATS.iter().any(|&fmt| upper.starts_with(fmt))
}

/// Check if format string indicates a time column
fn is_time_format(format: &str) -> bool {
    if format.is_empty() {
        return false;
    }
    let upper = format.to_uppercase();
    TIME_FORMATS.iter().any(|&fmt| upper.starts_with(fmt))
}

// --- Anonymous Scan Implementation ---

pub struct SasScan {
    path: PathBuf,
    num_threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    compress_opts: crate::CompressOptionsLite,
}

impl SasScan {
    pub fn new(
        path: PathBuf,
        threads: Option<usize>,
        missing_string_as_null: bool,
        chunk_size: Option<usize>,
        compress_opts: crate::CompressOptionsLite,
    ) -> Self {
        Self {
            path,
            num_threads: threads,
            missing_string_as_null,
            chunk_size,
            compress_opts,
        }
    }
}

enum ChunkMessage {
    Data { idx: usize, df: DataFrame },
    Done,
    Err(String),
}

pub(crate) struct SasBatchIter {
    rx: mpsc::Receiver<ChunkMessage>,
    buffer: BTreeMap<usize, DataFrame>,
    next_idx: usize,
    completed: usize,
    total_workers: usize,
    handles: Vec<JoinHandle<()>>,
}

impl Iterator for SasBatchIter {
    type Item = PolarsResult<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(df) = self.buffer.remove(&self.next_idx) {
                self.next_idx += 1;
                return Some(Ok(df));
            }

            if self.completed == self.total_workers {
                return None;
            }

            match self.rx.recv() {
                Ok(ChunkMessage::Data { idx, df }) => {
                    self.buffer.insert(idx, df);
                }
                Ok(ChunkMessage::Done) => {
                    self.completed += 1;
                }
                Ok(ChunkMessage::Err(e)) => {
                    return Some(Err(PolarsError::ComputeError(e.into())));
                }
                Err(_) => {
                    return None;
                }
            }
        }
    }
}

impl Drop for SasBatchIter {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

pub(crate) fn sas_batch_iter(
    path: PathBuf,
    threads: Option<usize>,
    missing_string_as_null: bool,
    chunk_size: Option<usize>,
    col_indices: Option<Vec<usize>>,
    n_rows: Option<usize>,
) -> PolarsResult<SasBatchIter> {
    let reader =
        Sas7bdatReader::open(&path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let total = n_rows.unwrap_or(reader.metadata().row_count as usize);
    let batch_size = chunk_size
        .unwrap_or(crate::pipeline::DEFAULT_PIPELINE_CHUNK_SIZE)
        .max(1);
    let total_chunks = total.div_ceil(batch_size);

    let col_names: Option<Vec<String>> = col_indices.as_ref().map(|indices| {
        indices
            .iter()
            .map(|&i| reader.metadata().columns[i].name.clone())
            .collect()
    });

    if total_chunks == 0 {
        let (_tx, rx) = mpsc::channel::<ChunkMessage>();
        return Ok(SasBatchIter {
            rx,
            buffer: BTreeMap::new(),
            next_idx: 0,
            completed: 0,
            total_workers: 0,
            handles: Vec::new(),
        });
    }

    let default_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1);
    let n_workers = min(
        threads.unwrap_or(default_threads).max(1),
        total_chunks.max(1),
    );
    let (tx, rx) = mpsc::channel::<ChunkMessage>();
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(n_workers);

    let base_chunks = total_chunks / n_workers;
    let extra_chunks = total_chunks % n_workers;
    let mut worker_ranges: Vec<(usize, usize)> = Vec::with_capacity(n_workers);
    let mut next_chunk = 0usize;
    for worker_idx in 0..n_workers {
        let worker_chunk_count = base_chunks + usize::from(worker_idx < extra_chunks);
        if worker_chunk_count == 0 {
            continue;
        }
        let start_chunk = next_chunk;
        let end_chunk = start_chunk + worker_chunk_count;
        worker_ranges.push((start_chunk, end_chunk));
        next_chunk = end_chunk;
    }

    for (start_chunk, end_chunk) in worker_ranges {
        let tx = tx.clone();
        let path = path.clone();
        let col_names = col_names.clone();
        let missing_string_as_null = missing_string_as_null;
        let handle = std::thread::spawn(move || {
            let reader = match Sas7bdatReader::open(&path) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(ChunkMessage::Err(e.to_string()));
                    return;
                }
            };

            let start_row = start_chunk * batch_size;
            if start_row >= total {
                let _ = tx.send(ChunkMessage::Done);
                return;
            }
            let worker_rows = min(total - start_row, (end_chunk - start_chunk) * batch_size);
            let mut builder = reader
                .read()
                .with_offset(start_row)
                .with_limit(worker_rows)
                .missing_string_as_null(missing_string_as_null)
                .sequential();
            if let Some(cols) = col_names {
                builder = builder.with_columns(cols);
            }
            let worker_df = match builder.finish() {
                Ok(df) => df,
                Err(e) => {
                    let _ = tx.send(ChunkMessage::Err(e.to_string()));
                    return;
                }
            };

            for out_idx in start_chunk..end_chunk {
                let local_offset = (out_idx - start_chunk) * batch_size;
                if local_offset >= worker_df.height() {
                    break;
                }
                let local_take = min(batch_size, worker_df.height() - local_offset);
                let df = worker_df.slice(local_offset as i64, local_take);
                let _ = tx.send(ChunkMessage::Data { idx: out_idx, df });
            }
            let _ = tx.send(ChunkMessage::Done);
        });
        handles.push(handle);
    }
    drop(tx);

    Ok(SasBatchIter {
        rx,
        buffer: BTreeMap::new(),
        next_idx: 0,
        completed: 0,
        total_workers: n_workers,
        handles,
    })
}

impl AnonymousScan for SasScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let reader = Sas7bdatReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // Resolve Column Names -> Indices
        let col_indices = if let Some(cols) = opts.with_columns {
            let mut indices = Vec::with_capacity(cols.len());
            for name in cols.iter() {
                let idx = reader
                    .metadata()
                    .columns
                    .iter()
                    .position(|c| c.name == name.as_str())
                    .ok_or_else(|| {
                        // .to_string() creates an owned String, which satisfies the 'static requirement
                        let err_msg = name.as_str().to_string();
                        PolarsError::ColumnNotFound(err_msg.into())
                    })?;
                indices.push(idx);
            }
            Some(indices)
        } else {
            None
        };

        let mut iter = sas_batch_iter(
            self.path.clone(),
            self.num_threads,
            self.missing_string_as_null,
            self.chunk_size,
            col_indices,
            opts.n_rows,
        )?;

        let mut out: Option<DataFrame> = None;
        while let Some(batch) = iter.next() {
            let df = batch?;
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

    // FIX: method signature updated to include Option<usize>
    fn schema(&self, _n_rows: Option<usize>) -> PolarsResult<SchemaRef> {
        let reader = Sas7bdatReader::open(&self.path)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // FIX: Schema::new() replaced with with_capacity
        let mut schema = Schema::with_capacity(reader.metadata().columns.len());

        for col in &reader.metadata().columns {
            let dtype = match col.col_type {
                ColumnType::Numeric => {
                    // Check format to determine date/time types
                    // IMPORTANT: Check DATETIME before DATE since "DATETIME" starts with "DATE"
                    if is_datetime_format(&col.format) {
                        DataType::Datetime(TimeUnit::Microseconds, None)
                    } else if is_date_format(&col.format) {
                        DataType::Date
                    } else if is_time_format(&col.format) {
                        DataType::Time
                    } else {
                        DataType::Float64
                    }
                }
                ColumnType::Character => DataType::String,
            };
            schema.with_column(col.name.as_str().into(), dtype);
        }
        Ok(Arc::new(schema))
    }
}

pub fn scan_sas7bdat(
    path: impl Into<std::path::PathBuf>,
    opts: crate::ScanOptions,
) -> PolarsResult<LazyFrame> {
    let path = path.into();
    let missing_string_as_null = opts.missing_string_as_null.unwrap_or(true);
    let scan_ptr = Arc::new(SasScan::new(
        path,
        opts.threads,
        missing_string_as_null,
        opts.chunk_size,
        opts.compress_opts,
    ));
    LazyFrame::anonymous_scan(scan_ptr, Default::default())
}

#[cfg(test)]
mod tests {
    use super::sas_batch_iter;
    use std::path::PathBuf;

    fn small_sas_path() -> PathBuf {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("sas")
            .join("data");
        let candidates = [
            base.join("data_pandas").join("test1.sas7bdat"),
            base.join("data_pandas").join("test2.sas7bdat"),
            base.join("test.sas7bdat"),
        ];
        for path in candidates {
            if path.exists() {
                return path;
            }
        }
        base.join("data_pandas").join("test1.sas7bdat")
    }

    #[test]
    fn test_sas_batch_streaming() {
        let path = small_sas_path();
        if !path.exists() {
            return;
        }
        let mut iter =
            sas_batch_iter(path, None, true, Some(10), None, Some(25)).expect("batch iter");
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
