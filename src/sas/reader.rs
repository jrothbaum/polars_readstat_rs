use crate::data::{DataReader, DataSubheader};
use crate::error::Result;
use crate::header::{check_header, read_header};
use crate::metadata::read_metadata;
use crate::page::PageReader;
use crate::pipeline::DEFAULT_PIPELINE_CHUNK_SIZE;
use crate::polars_output::{ColumnPlan, DataFrameBuilder};
use crate::types::{Compression, Endian, Format, Header, Metadata};
use polars::prelude::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// Entry in the data page index for fast parallel seeking
#[derive(Debug, Clone)]
struct DataPageEntry {
    /// Page number (0-indexed from start of file pages, after header)
    page_number: usize,
    /// Cumulative row count at the START of this page
    row_start: usize,
    /// Number of data rows on this page
    row_count: usize,
}

/// Main reader for SAS7BDAT files
pub struct Sas7bdatReader {
    path: PathBuf,
    header: Header,
    metadata: Metadata,
    endian: Endian,
    format: Format,
    initial_data_subheaders: Vec<DataSubheader>,
    /// 0-based index of the first DATA page (page right after all metadata pages).
    /// Used for analytical parallel seek without scanning the file.
    first_data_page: usize,
    /// Number of data rows that live on MIX pages before the first DATA page.
    /// These are returned by the DataReader before any DATA-page rows.
    mix_data_rows: usize,
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
        let (metadata, initial_data_subheaders, first_data_page, mix_data_rows) =
            read_metadata(file, &header, endian, format)?;

        Ok(Self {
            path,
            header,
            metadata,
            endian,
            format,
            initial_data_subheaders,
            first_data_page,
            mix_data_rows,
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
        let (metadata, initial_data_subheaders, first_data_page, mix_data_rows) =
            read_metadata(file, &header, endian, format)?;
        let metadata_ms = metadata_start.elapsed().as_secs_f64() * 1000.0;

        Ok((
            Self {
                path,
                header,
                metadata,
                endian,
                format,
                initial_data_subheaders,
                first_data_page,
                mix_data_rows,
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

        // Build page index analytically (one 6-byte read to validate, then pure arithmetic)
        let page_index = compute_page_index(
            &self.path,
            &self.header,
            &self.metadata,
            self.endian,
            self.format,
            self.first_data_page,
            self.mix_data_rows,
        );

        let path = Arc::new(self.path.clone());
        let header = Arc::new(self.header.clone());
        let metadata = Arc::new(self.metadata.clone());
        let cols_arc = cols.map(|c| Arc::new(c.to_vec()));
        let page_index = Arc::new(page_index);

        let dfs = (0..n_chunks)
            .into_par_iter()
            .map(|i| {
                let start = offset + (i * chunk_size);
                let chunk_count = chunk_size.min(offset + count - start);
                read_batch_with_page_index(
                    &path,
                    &header,
                    &metadata,
                    self.endian,
                    self.format,
                    start,
                    chunk_count,
                    cols_arc.as_deref().map(|v| v.as_slice()),
                    missing_string_as_null,
                    &page_index,
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

    let plans = ColumnPlan::build_plans(metadata, col_indices, endian, missing_string_as_null);

    for _ in 0..count {
        if let Some(row_bytes) = data_reader.read_row_borrowed()? {
            builder.add_row_raw(row_bytes, &plans);
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

    let plans = ColumnPlan::build_plans(metadata, col_indices, endian, missing_string_as_null);

    let parse_start = Instant::now();
    let parse_values_ms = 0.0f64;
    let add_row_ms = 0.0f64;
    for _ in 0..count {
        if let Some(row_bytes) = data_reader.read_row_borrowed()? {
            builder.add_row_raw(row_bytes, &plans);
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

/// Build a page index analytically — zero I/O, pure arithmetic from header/metadata.
///
/// For uncompressed SAS files all pages after the metadata section are DATA pages
/// with a constant rows-per-page value derived from page_length and row_length.
/// We simply enumerate them without touching the file.
fn compute_page_index(
    path: &Path,
    header: &Header,
    metadata: &Metadata,
    endian: Endian,
    format: Format,
    first_data_page: usize,
    mix_data_rows: usize,
) -> Vec<DataPageEntry> {
    let row_length = metadata.row_length;
    if row_length == 0 {
        return Vec::new();
    }

    let page_bit_offset: usize = match format {
        Format::Bit64 => 32,
        Format::Bit32 => 16,
    };
    let data_start = page_bit_offset + 8;
    let rows_per_page = header.page_length.saturating_sub(data_start) / row_length;
    if rows_per_page == 0 {
        return Vec::new();
    }

    // Validate: read the actual block_count from the first DATA page header.
    // If it doesn't match our analytical rows_per_page the assumption is broken
    // and we return an empty index so all workers fall back to skip_rows.
    if let Ok(actual_block_count) = read_first_data_page_block_count(
        path, header, endian, page_bit_offset, first_data_page,
    ) {
        if actual_block_count != rows_per_page {
            return Vec::new();
        }
    } else {
        return Vec::new();
    }

    // DATA-page rows start after any MIX-page data rows.
    let total_rows = metadata.row_count;
    let data_page_rows = total_rows.saturating_sub(mix_data_rows);
    let n_data_pages = (data_page_rows + rows_per_page - 1) / rows_per_page;

    let mut entries = Vec::with_capacity(n_data_pages);
    for i in 0..n_data_pages {
        let row_start = mix_data_rows + i * rows_per_page;
        let row_count = rows_per_page.min(total_rows - row_start);
        entries.push(DataPageEntry {
            page_number: first_data_page + i,
            row_start,
            row_count,
        });
    }
    entries
}

/// Read the block_count field from the first DATA page header.
/// This is a single seek + 6-byte read — essentially free.
fn read_first_data_page_block_count(
    path: &Path,
    header: &Header,
    endian: Endian,
    page_bit_offset: usize,
    first_data_page: usize,
) -> crate::error::Result<usize> {
    use std::io::Read;
    let byte_offset = header.header_length as u64
        + first_data_page as u64 * header.page_length as u64
        + page_bit_offset as u64;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(byte_offset))?;
    let mut buf = [0u8; 6];
    file.read_exact(&mut buf)?;
    // page_type: buf[0..2], block_count: buf[2..4]
    let block_count = match endian {
        Endian::Little => u16::from_le_bytes([buf[2], buf[3]]),
        Endian::Big => u16::from_be_bytes([buf[2], buf[3]]),
    };
    Ok(block_count as usize)
}

/// Read a batch of rows using the page index for direct seeking.
/// Instead of skip_rows (which reads through all preceding pages),
/// this seeks directly to the page containing the target row.
/// Falls back to skip_rows if target_row falls in the MIX-data prefix
/// (i.e., before the first DATA page in the index).
fn read_batch_with_page_index(
    path: &Path,
    header: &Header,
    metadata: &Metadata,
    endian: Endian,
    format: Format,
    target_row: usize,
    count: usize,
    col_indices: Option<&[usize]>,
    missing_string_as_null: bool,
    page_index: &[DataPageEntry],
) -> Result<DataFrame> {
    // Binary search for the DATA page containing target_row.
    let page_idx = page_index
        .partition_point(|p| p.row_start + p.row_count <= target_row);

    // If the target row falls before the first DATA-page entry (i.e., in the
    // MIX-data prefix) or the index is empty, fall back to the sequential
    // skip_rows path which handles MIX pages correctly.
    let use_index = page_idx < page_index.len()
        && target_row >= page_index.first().map_or(0, |e| e.row_start);

    if !use_index {
        return read_batch_selected(
            path,
            header,
            metadata,
            endian,
            format,
            target_row,
            count,
            col_indices,
            missing_string_as_null,
            &[],
        );
    }

    let entry = &page_index[page_idx];
    let within_page_skip = target_row - entry.row_start;

    // Seek directly to the target page
    let page_byte_offset =
        header.header_length as u64 + (entry.page_number as u64 * header.page_length as u64);
    let mut file = BufReader::new(File::open(path)?);
    file.seek(SeekFrom::Start(page_byte_offset))?;

    let page_reader = PageReader::new(file, header.clone(), endian, format);
    let mut data_reader = DataReader::new(
        page_reader,
        metadata.clone(),
        endian,
        format,
        Vec::new(), // seeked past metadata, no initial data subheaders
    )?;

    // Set current_row so the row-count guard fires at the right place
    data_reader.set_current_row(entry.row_start);

    // Skip only within-page rows (no page reads needed)
    data_reader.skip_rows(within_page_skip)?;

    let mut builder = match col_indices {
        Some(idx) => DataFrameBuilder::new_with_columns(metadata, idx, count),
        None => DataFrameBuilder::new(metadata.clone(), count),
    };

    let plans = ColumnPlan::build_plans(metadata, col_indices, endian, missing_string_as_null);

    for _ in 0..count {
        if let Some(row_bytes) = data_reader.read_row_borrowed()? {
            builder.add_row_raw(row_bytes, &plans);
        } else {
            break;
        }
    }
    builder.build()
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
