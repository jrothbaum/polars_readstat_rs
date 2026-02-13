use crate::data::{DataReader, DataSubheader};
use crate::error::Result;
use crate::header::{check_header, read_header};
use crate::metadata::read_metadata;
use crate::page::PageReader;
use crate::pipeline::DEFAULT_PIPELINE_CHUNK_SIZE;
use crate::polars_output::DataFrameBuilder;
use crate::polars_output::{kind_for_column, ColumnKind};
use crate::types::{ColumnType, Compression, Endian, Format, Header, Metadata};
use crate::value::{decode_numeric_bytes_mask, parse_row_values_into, ValueParser};
use polars::prelude::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// Main reader for SAS7BDAT files
pub struct Sas7bdatReader {
    path: PathBuf,
    header: Header,
    metadata: Metadata,
    endian: Endian,
    format: Format,
    initial_data_subheaders: Vec<DataSubheader>,
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

impl Sas7bdatReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let (endian, format) = check_header(&mut file)?;
        let header = read_header(&mut file, endian, format)?;

        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(header.header_length as u64))?;
        let (metadata, initial_data_subheaders) = read_metadata(file, &header, endian, format)?;

        Ok(Self {
            path,
            header,
            metadata,
            endian,
            format,
            initial_data_subheaders,
        })
    }

    pub fn open_with_profile(path: impl AsRef<Path>) -> Result<(Self, OpenProfile)> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;

        let header_start = Instant::now();
        let (endian, format) = check_header(&mut file)?;
        let header = read_header(&mut file, endian, format)?;
        let header_ms = header_start.elapsed().as_secs_f64() * 1000.0;

        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(header.header_length as u64))?;
        let metadata_start = Instant::now();
        let (metadata, initial_data_subheaders) = read_metadata(file, &header, endian, format)?;
        let metadata_ms = metadata_start.elapsed().as_secs_f64() * 1000.0;

        Ok((
            Self {
                path,
                header,
                metadata,
                endian,
                format,
                initial_data_subheaders,
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
    pub fn endian(&self) -> Endian {
        self.endian
    }
    pub fn format(&self) -> Format {
        self.format
    }
    pub fn initial_data_subheaders(&self) -> &[DataSubheader] {
        &self.initial_data_subheaders
    }

    /// The single internal execution path for all read operations
    fn execute_read(&self, opts: ReadBuilder) -> Result<DataFrame> {
        let col_indices = resolve_column_indices(&self.metadata, opts.columns.as_deref())?;
        let limit = opts
            .limit
            .unwrap_or(self.metadata.row_count.saturating_sub(opts.offset));

        let missing_null = opts.missing_string_as_null;

        // Logic switch: Use pipeline, parallel chunks, or sequential
        let mut df = if opts.use_pipeline {
            crate::pipeline::read_pipeline(
                &self.path,
                &self.header,
                &self.metadata,
                self.endian,
                self.format,
                opts.num_threads.unwrap_or_else(rayon::current_num_threads),
                opts.pipeline_chunk_size
                    .unwrap_or(DEFAULT_PIPELINE_CHUNK_SIZE),
                opts.limit,
                col_indices.as_deref(),
                missing_null,
                &self.initial_data_subheaders,
            )?
        } else if opts.parallel && self.metadata.compression == Compression::None {
            self.read_parallel(
                opts.offset,
                limit,
                opts.num_threads,
                opts.chunk_size,
                col_indices.as_deref(),
                missing_null,
            )?
        } else {
            read_batch_selected(
                &self.path,
                &self.header,
                &self.metadata,
                self.endian,
                self.format,
                opts.offset,
                limit,
                col_indices.as_deref(),
                missing_null,
                &self.initial_data_subheaders,
            )?
        };

        if let Some(schema) = opts.schema {
            df = cast_dataframe(df, &schema)?;
        }

        Ok(df)
    }

    /// Profiled read path (forces sequential, non-pipeline execution)
    fn execute_read_profiled(&self, opts: ReadBuilder) -> Result<(DataFrame, ReadProfile)> {
        let col_indices = resolve_column_indices(&self.metadata, opts.columns.as_deref())?;
        let limit = opts
            .limit
            .unwrap_or(self.metadata.row_count.saturating_sub(opts.offset));
        let missing_null = opts.missing_string_as_null;

        let (mut df, mut profile) = read_batch_selected_profiled(
            &self.path,
            &self.header,
            &self.metadata,
            self.endian,
            self.format,
            opts.offset,
            limit,
            col_indices.as_deref(),
            missing_null,
            &self.initial_data_subheaders,
        )?;

        if let Some(schema) = opts.schema {
            let cast_start = Instant::now();
            df = cast_dataframe(df, &schema)?;
            profile.build_df_ms += cast_start.elapsed().as_secs_f64() * 1000.0;
            profile.total_ms = profile.parse_rows_ms + profile.build_df_ms;
        }

        Ok((df, profile))
    }

    fn read_parallel(
        &self,
        offset: usize,
        count: usize,
        threads: Option<usize>,
        chunk_size: Option<usize>,
        cols: Option<&[usize]>,
        missing_string_as_null: bool,
    ) -> Result<DataFrame> {
        let n_threads = threads.unwrap_or_else(rayon::current_num_threads);
        let mut chunk_size = chunk_size.unwrap_or_else(|| (count / n_threads).max(1000).min(5000));
        if chunk_size < 1000 {
            chunk_size = 1000;
        }
        if chunk_size > count {
            chunk_size = count;
        }
        let n_chunks = (count + chunk_size - 1) / chunk_size;

        let path = Arc::new(self.path.clone());
        let header = Arc::new(self.header.clone());
        let metadata = Arc::new(self.metadata.clone());
        let cols_arc = cols.map(|c| Arc::new(c.to_vec()));
        let initial_data_subheaders = self.initial_data_subheaders.clone();

        let dfs = (0..n_chunks)
            .into_par_iter()
            .map(|i| {
                let start = offset + (i * chunk_size);
                let chunk_count = chunk_size.min(offset + count - start);
                read_batch_selected(
                    &path,
                    &header,
                    &metadata,
                    self.endian,
                    self.format,
                    start,
                    chunk_count,
                    cols_arc.as_deref().map(|v| v.as_slice()),
                    missing_string_as_null,
                    &initial_data_subheaders,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        combine_dataframes(dfs)
    }
}

/// Fluent Builder for configuring read operations
pub struct ReadBuilder<'a> {
    reader: &'a Sas7bdatReader,
    columns: Option<Vec<String>>,
    schema: Option<Arc<Schema>>,
    offset: usize,
    limit: Option<usize>,
    parallel: bool,
    use_pipeline: bool,
    num_threads: Option<usize>,
    chunk_size: Option<usize>,
    pipeline_chunk_size: Option<usize>,
    missing_string_as_null: bool,
}

impl<'a> ReadBuilder<'a> {
    fn new(reader: &'a Sas7bdatReader) -> Self {
        Self {
            reader,
            columns: None,
            schema: None,
            offset: 0,
            limit: None,
            parallel: true,
            use_pipeline: false,
            num_threads: None,
            chunk_size: None,
            pipeline_chunk_size: None,
            missing_string_as_null: true,
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
    pub fn pipeline(mut self) -> Self {
        self.use_pipeline = true;
        self
    }
    pub fn pipeline_chunk_size(mut self, chunk_size: usize) -> Self {
        self.pipeline_chunk_size = Some(chunk_size);
        self
    }
    pub fn missing_string_as_null(mut self, v: bool) -> Self {
        self.missing_string_as_null = v;
        self
    }

    pub fn finish(self) -> Result<DataFrame> {
        self.reader.execute_read(self)
    }

    pub fn finish_profiled(self) -> Result<(DataFrame, ReadProfile)> {
        self.reader.execute_read_profiled(self)
    }
}

// --- Internal Helpers (Reduced to single implementations) ---

fn read_batch_selected(
    path: &Path,
    header: &Header,
    metadata: &Metadata,
    endian: Endian,
    format: Format,
    skip: usize,
    count: usize,
    col_indices: Option<&[usize]>,
    missing_string_as_null: bool,
    initial_data_subheaders: &[DataSubheader],
) -> Result<DataFrame> {
    let mut file = BufReader::new(File::open(path)?);
    file.seek(SeekFrom::Start(header.header_length as u64))?;

    let page_reader = PageReader::new(file, header.clone(), endian, format);
    let mut data_reader = DataReader::new(
        page_reader,
        metadata.clone(),
        endian,
        format,
        initial_data_subheaders.to_vec(),
    )?;

    data_reader.skip_rows(skip)?;

    let mut builder = match col_indices {
        Some(idx) => DataFrameBuilder::new_with_columns(metadata, idx, count),
        None => DataFrameBuilder::new(metadata.clone(), count),
    };

    let numeric_only = match col_indices {
        Some(indices) => indices
            .iter()
            .all(|&idx| metadata.columns[idx].col_type == ColumnType::Numeric),
        None => false,
    };
    let numeric_plan: Option<Vec<NumericColumnPlan>> = if numeric_only {
        col_indices.map(|indices| {
            indices
                .iter()
                .enumerate()
                .map(|(pos, &idx)| {
                    let col = &metadata.columns[idx];
                    NumericColumnPlan {
                        pos,
                        start: col.offset,
                        end: col.offset + col.length,
                        kind: kind_for_column(col),
                    }
                })
                .collect()
        })
    } else {
        None
    };
    let numeric_max_end = numeric_plan
        .as_ref()
        .map(|plan| plan.iter().map(|p| p.end).max().unwrap_or(0));
    let parser = ValueParser::new(endian, metadata.encoding_byte, missing_string_as_null);
    let mut row_values: Vec<crate::value::Value> = Vec::with_capacity(
        col_indices
            .map(|v| v.len())
            .unwrap_or(metadata.columns.len()),
    );

    for _ in 0..count {
        if let Some(row_bytes) = data_reader.read_row()? {
            if numeric_only {
                if let Some(plans) = numeric_plan.as_ref() {
                    if let Some(max_end) = numeric_max_end {
                        if row_bytes.len() < max_end {
                            return Err(crate::error::Error::BufferOutOfBounds {
                                offset: max_end,
                                length: 0,
                            });
                        }
                    }
                    for plan in plans {
                        let (value, is_missing) =
                            decode_numeric_bytes_mask(endian, &row_bytes[plan.start..plan.end]);
                        builder.add_numeric_value_mask_kind(plan.pos, plan.kind, value, is_missing);
                    }
                }
            } else {
                parse_row_values_into(&parser, &row_bytes, metadata, col_indices, &mut row_values)?;
                builder.add_row_ref(&row_values)?;
            }
        } else {
            break;
        }
    }
    builder.build()
}

fn read_batch_selected_profiled(
    path: &Path,
    header: &Header,
    metadata: &Metadata,
    endian: Endian,
    format: Format,
    skip: usize,
    count: usize,
    col_indices: Option<&[usize]>,
    missing_string_as_null: bool,
    initial_data_subheaders: &[DataSubheader],
) -> Result<(DataFrame, ReadProfile)> {
    let total_start = Instant::now();
    let mut file = BufReader::new(File::open(path)?);
    file.seek(SeekFrom::Start(header.header_length as u64))?;

    let page_reader = PageReader::new(file, header.clone(), endian, format);
    let mut data_reader = DataReader::new(
        page_reader,
        metadata.clone(),
        endian,
        format,
        initial_data_subheaders.to_vec(),
    )?;

    data_reader.skip_rows(skip)?;

    let mut builder = match col_indices {
        Some(idx) => DataFrameBuilder::new_with_columns(metadata, idx, count),
        None => DataFrameBuilder::new(metadata.clone(), count),
    };

    let numeric_only = match col_indices {
        Some(indices) => indices
            .iter()
            .all(|&idx| metadata.columns[idx].col_type == ColumnType::Numeric),
        None => false,
    };
    let numeric_plan: Option<Vec<NumericColumnPlan>> = if numeric_only {
        col_indices.map(|indices| {
            indices
                .iter()
                .enumerate()
                .map(|(pos, &idx)| {
                    let col = &metadata.columns[idx];
                    NumericColumnPlan {
                        pos,
                        start: col.offset,
                        end: col.offset + col.length,
                        kind: kind_for_column(col),
                    }
                })
                .collect()
        })
    } else {
        None
    };
    let numeric_max_end = numeric_plan
        .as_ref()
        .map(|plan| plan.iter().map(|p| p.end).max().unwrap_or(0));
    let parser = ValueParser::new(endian, metadata.encoding_byte, missing_string_as_null);
    let mut row_values: Vec<crate::value::Value> = Vec::with_capacity(
        col_indices
            .map(|v| v.len())
            .unwrap_or(metadata.columns.len()),
    );

    let parse_start = Instant::now();
    let mut parse_values_ms = 0.0f64;
    let mut add_row_ms = 0.0f64;
    for _ in 0..count {
        if let Some(row_bytes) = data_reader.read_row()? {
            if numeric_only {
                if let Some(plans) = numeric_plan.as_ref() {
                    if let Some(max_end) = numeric_max_end {
                        if row_bytes.len() < max_end {
                            return Err(crate::error::Error::BufferOutOfBounds {
                                offset: max_end,
                                length: 0,
                            });
                        }
                    }
                    for plan in plans {
                        let values_start = Instant::now();
                        let (value, is_missing) =
                            decode_numeric_bytes_mask(endian, &row_bytes[plan.start..plan.end]);
                        parse_values_ms += values_start.elapsed().as_secs_f64() * 1000.0;

                        let add_start = Instant::now();
                        builder.add_numeric_value_mask_kind(plan.pos, plan.kind, value, is_missing);
                        add_row_ms += add_start.elapsed().as_secs_f64() * 1000.0;
                    }
                }
            } else {
                let values_start = Instant::now();
                parse_row_values_into(&parser, &row_bytes, metadata, col_indices, &mut row_values)?;
                parse_values_ms += values_start.elapsed().as_secs_f64() * 1000.0;

                let add_start = Instant::now();
                builder.add_row_ref(&row_values)?;
                add_row_ms += add_start.elapsed().as_secs_f64() * 1000.0;
            }
        } else {
            break;
        }
    }
    let parse_rows_ms = parse_start.elapsed().as_secs_f64() * 1000.0;

    let build_start = Instant::now();
    let df = builder.build()?;
    let build_df_ms = build_start.elapsed().as_secs_f64() * 1000.0;

    let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
    Ok((
        df,
        ReadProfile {
            parse_rows_ms,
            parse_values_ms,
            add_row_ms,
            build_df_ms,
            total_ms,
        },
    ))
}

fn resolve_column_indices(
    metadata: &Metadata,
    columns: Option<&[String]>,
) -> Result<Option<Vec<usize>>> {
    columns
        .map(|names| {
            names
                .iter()
                .map(|name| {
                    metadata
                        .columns
                        .iter()
                        .position(|c| c.name == *name)
                        .ok_or_else(|| {
                            crate::error::Error::ParseError(format!("Column '{}' not found", name))
                        })
                })
                .collect()
        })
        .transpose()
}

fn combine_dataframes(mut dfs: Vec<DataFrame>) -> Result<DataFrame> {
    if dfs.is_empty() {
        return Ok(DataFrame::empty());
    }
    let mut main_df = dfs.remove(0);
    for df in dfs {
        main_df.vstack_mut(&df)?;
    }
    Ok(main_df)
}

fn cast_dataframe(mut df: DataFrame, schema: &Schema) -> Result<DataFrame> {
    for (name, dtype) in schema.iter() {
        if let Ok(col) = df.column(name) {
            let casted = col.as_materialized_series().cast(dtype).map_err(|e| {
                crate::error::Error::ParseError(format!("Cast failed for {}: {}", name, e))
            })?;
            df.replace(name, casted.into())?;
        }
    }
    Ok(df)
}
struct NumericColumnPlan {
    pos: usize,
    start: usize,
    end: usize,
    kind: ColumnKind,
}
