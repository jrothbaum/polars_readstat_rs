use crate::stata::data::{build_shared_decode, read_data_frame, read_data_frame_range, read_data_frame_range_with_indicators};
use std::collections::HashSet;
use crate::stata::error::Result;
use crate::stata::header::read_header;
use crate::stata::metadata::read_metadata;
use crate::stata::polars_output::{apply_stata_time_formats, stata_time_format_kind};
use crate::stata::types::{Header, Metadata};
use polars::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// Main reader for Stata .dta files
pub struct StataReader {
    path: PathBuf,
    header: Header,
    metadata: Metadata,
}

#[derive(Debug, Clone)]
pub struct OpenProfile {
    pub header_ms: f64,
    pub metadata_ms: f64,
}

#[derive(Debug, Clone)]
pub struct ReadProfile {
    pub parse_rows_ms: f64,
    pub parse_values_ms: f64,
    pub add_row_ms: f64,
    pub build_df_ms: f64,
    pub total_ms: f64,
}

impl StataReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let header = read_header(&mut file)?;
        let metadata = read_metadata(&mut file, &header)?;

        Ok(Self {
            path,
            header,
            metadata,
        })
    }

    pub fn open_with_profile(path: impl AsRef<Path>) -> Result<(Self, OpenProfile)> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let header_start = Instant::now();
        let header = read_header(&mut file)?;
        let header_ms = header_start.elapsed().as_secs_f64() * 1000.0;

        let metadata_start = Instant::now();
        let metadata = read_metadata(&mut file, &header)?;
        let metadata_ms = metadata_start.elapsed().as_secs_f64() * 1000.0;

        Ok((
            Self {
                path,
                header,
                metadata,
            },
            OpenProfile {
                header_ms,
                metadata_ms,
            },
        ))
    }

    /// Entry point for reading. Returns a builder to configure the operation.
    pub fn read(&self) -> ReadBuilder<'_> {
        ReadBuilder::new(self)
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    pub fn header(&self) -> &Header {
        &self.header
    }

    fn execute_read(&self, _opts: ReadBuilder) -> Result<DataFrame> {
        let col_indices = resolve_column_indices(&self.metadata, _opts.columns.as_deref())?;
        let limit = _opts
            .limit
            .unwrap_or(self.metadata.row_count.saturating_sub(_opts.offset as u64) as usize);

        // Informative nulls: serial path using the indicator-aware reader
        if let Some(ref null_opts) = _opts.informative_nulls {
            let var_names: Vec<&str> = self
                .metadata
                .variables
                .iter()
                .map(|v| v.name.as_str())
                .collect();
            let eligible: Vec<&str> = self
                .metadata
                .variables
                .iter()
                .filter(|v| {
                    matches!(v.var_type, crate::stata::types::VarType::Numeric(_))
                })
                .map(|v| v.name.as_str())
                .collect();
            let pairs = crate::informative_null_pairs(&var_names, &eligible, null_opts);
            crate::check_informative_null_collisions(&var_names, &pairs)?;
            let suffix = match &null_opts.mode {
                crate::InformativeNullMode::SeparateColumn { suffix } => suffix.clone(),
                _ => "_null".to_string(),
            };
            let indicator_set: HashSet<String> =
                pairs.iter().map(|(m, _)| m.clone()).collect();
            let shared = build_shared_decode(
                &self.path,
                &self.metadata,
                self.header.endian,
                self.header.version,
                _opts.value_labels_as_strings,
            )?;
            let mut df = read_data_frame_range_with_indicators(
                &self.path,
                &self.metadata,
                self.header.endian,
                self.header.version,
                col_indices.as_deref(),
                _opts.offset,
                limit,
                _opts.missing_string_as_null,
                _opts.value_labels_as_strings,
                &shared,
                &indicator_set,
                null_opts.use_value_labels,
                &suffix,
            )?;
            if df.height() > 0 {
                let mut formats = Vec::new();
                for var in &self.metadata.variables {
                    if let Some(kind) =
                        stata_time_format_kind(var.format.as_deref(), &var.var_type)
                    {
                        formats.push((var.name.clone(), kind));
                    }
                }
                if !formats.is_empty() {
                    apply_stata_time_formats(&mut df, &formats)?;
                }
            }
            if let Some(schema) = _opts.schema {
                df = cast_dataframe(df, &schema)?;
            }
            df = crate::apply_informative_null_mode(df, &null_opts.mode, &pairs)?;
            return Ok(df);
        }

        let mut df = if _opts.parallel && limit > 0 {
            self.read_parallel(
                _opts.offset,
                limit,
                _opts.num_threads,
                _opts.chunk_size,
                col_indices.as_deref(),
                _opts.missing_string_as_null,
                _opts.value_labels_as_strings,
            )?
        } else {
            read_data_frame(
                &self.path,
                &self.metadata,
                self.header.endian,
                self.header.version,
                col_indices.as_deref(),
                _opts.offset,
                limit,
                _opts.missing_string_as_null,
                _opts.value_labels_as_strings,
            )?
        };

        if df.height() > 0 {
            let mut formats = Vec::new();
            for var in &self.metadata.variables {
                if let Some(kind) = stata_time_format_kind(var.format.as_deref(), &var.var_type) {
                    formats.push((var.name.clone(), kind));
                }
            }
            if !formats.is_empty() {
                apply_stata_time_formats(&mut df, &formats)?;
            }
        }

        if let Some(schema) = _opts.schema {
            df = cast_dataframe(df, &schema)?;
        }

        Ok(df)
    }

    fn execute_read_profiled(&self, _opts: ReadBuilder) -> Result<(DataFrame, ReadProfile)> {
        let start = Instant::now();
        let df = self.execute_read(_opts)?;
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        Ok((
            df,
            ReadProfile {
                parse_rows_ms: 0.0,
                parse_values_ms: 0.0,
                add_row_ms: 0.0,
                build_df_ms: total_ms,
                total_ms,
            },
        ))
    }
}

/// Fluent Builder for configuring read operations
pub struct ReadBuilder<'a> {
    reader: &'a StataReader,
    columns: Option<Vec<String>>,
    schema: Option<Arc<Schema>>,
    offset: usize,
    limit: Option<usize>,
    parallel: bool,
    num_threads: Option<usize>,
    chunk_size: Option<usize>,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    informative_nulls: Option<crate::InformativeNullOpts>,
}

impl<'a> ReadBuilder<'a> {
    fn new(reader: &'a StataReader) -> Self {
        Self {
            reader,
            columns: None,
            schema: None,
            offset: 0,
            limit: None,
            parallel: true,
            num_threads: None,
            chunk_size: None,
            missing_string_as_null: true,
            value_labels_as_strings: true,
            informative_nulls: None,
        }
    }

    pub fn with_columns(mut self, cols: Vec<String>) -> Self {
        self.columns = Some(cols);
        self
    }
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }
    pub fn with_n_threads(mut self, n: usize) -> Self {
        self.num_threads = Some(n);
        self
    }
    pub fn with_chunk_size(mut self, n: usize) -> Self {
        self.chunk_size = Some(n);
        self
    }
    pub fn sequential(mut self) -> Self {
        self.parallel = false;
        self
    }
    pub fn missing_string_as_null(mut self, v: bool) -> Self {
        self.missing_string_as_null = v;
        self
    }
    pub fn value_labels_as_strings(mut self, v: bool) -> Self {
        self.value_labels_as_strings = v;
        self
    }
    pub fn informative_nulls(mut self, v: Option<crate::InformativeNullOpts>) -> Self {
        self.informative_nulls = v;
        self
    }

    pub fn finish(self) -> Result<DataFrame> {
        self.reader.execute_read(self)
    }

    pub fn finish_profiled(self) -> Result<(DataFrame, ReadProfile)> {
        self.reader.execute_read_profiled(self)
    }
}

impl StataReader {
    const DEFAULT_CHUNK_SIZE: usize = 100_000;

    fn read_parallel(
        &self,
        offset: usize,
        count: usize,
        threads: Option<usize>,
        chunk_size: Option<usize>,
        cols: Option<&[usize]>,
        missing_string_as_null: bool,
        value_labels_as_strings: bool,
    ) -> Result<DataFrame> {
        let n_threads = threads.unwrap_or_else(|| {
            let cur = rayon::current_num_threads();
            cur.min(4).max(1)
        });
        if n_threads <= 1 || count < 1000 {
            return read_data_frame(
                &self.path,
                &self.metadata,
                self.header.endian,
                self.header.version,
                cols,
                offset,
                count,
                missing_string_as_null,
                value_labels_as_strings,
            );
        }

        let mut chunk_size = chunk_size.unwrap_or(Self::DEFAULT_CHUNK_SIZE);
        if chunk_size < 1_000 {
            chunk_size = 1_000;
        }
        if chunk_size > count {
            chunk_size = count;
        }
        let n_chunks = (count + chunk_size - 1) / chunk_size;

        let shared = build_shared_decode(
            &self.path,
            &self.metadata,
            self.header.endian,
            self.header.version,
            value_labels_as_strings,
        )?;
        let shared = std::sync::Arc::new(shared);

        let pool = ThreadPoolBuilder::new()
            .num_threads(n_threads)
            .build()
            .map_err(|e| {
                crate::stata::error::Error::ParseError(format!("thread pool error: {}", e))
            })?;

        let mut dfs: Vec<(usize, DataFrame)> = pool.install(|| {
            (0..n_chunks)
                .into_par_iter()
                .map(|i| {
                    let start = offset + i * chunk_size;
                    let end = (offset + count).min(start + chunk_size);
                    let cnt = end - start;
                    read_data_frame_range(
                        &self.path,
                        &self.metadata,
                        self.header.endian,
                        self.header.version,
                        cols,
                        start,
                        cnt,
                        missing_string_as_null,
                        value_labels_as_strings,
                        &shared,
                    )
                    .map(|df| (i, df))
                })
                .collect::<Result<Vec<_>>>()
        })?;

        dfs.sort_by_key(|(i, _)| *i);
        let frames = dfs.into_iter().map(|(_, df)| df).collect::<Vec<_>>();
        combine_dataframes(frames)
    }
}

fn combine_dataframes(mut dfs: Vec<DataFrame>) -> Result<DataFrame> {
    if dfs.is_empty() {
        return Ok(DataFrame::empty());
    }
    let mut main = dfs.remove(0);
    for df in dfs {
        main.vstack_mut(&df)?;
    }
    Ok(main)
}

fn resolve_column_indices(
    metadata: &Metadata,
    cols: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    let Some(cols) = cols else {
        return Ok(None);
    };
    let mut indices = Vec::with_capacity(cols.len());
    for name in cols {
        let idx = metadata
            .variables
            .iter()
            .position(|v| v.name == *name)
            .ok_or_else(|| {
                crate::stata::error::Error::ParseError(format!("Unknown column: {}", name))
            })?;
        indices.push(idx);
    }
    Ok(Some(indices))
}

fn cast_dataframe(mut df: DataFrame, schema: &Schema) -> Result<DataFrame> {
    for (name, dtype) in schema.iter() {
        if let Ok(col) = df.column(name) {
            let casted = col.as_materialized_series().cast(dtype).map_err(|e| {
                crate::stata::error::Error::ParseError(format!("Cast failed for {}: {}", name, e))
            })?;
            df.replace(name, casted.into())?;
        }
    }
    Ok(df)
}
