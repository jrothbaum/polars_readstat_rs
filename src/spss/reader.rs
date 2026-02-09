use crate::spss::error::{Error, Result};
use crate::spss::types::{Header, Metadata};
use crate::spss::header::read_header;
use crate::spss::metadata::read_metadata;
use crate::spss::data::{read_data_frame, read_data_columns_uncompressed, profile_print, profile_reset};
use polars::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::fs::File;
use std::io::BufReader;

pub struct SpssReader {
    path: PathBuf,
    header: Header,
    metadata: Metadata,
}

impl SpssReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
        let header = read_header(&mut reader)?;
        let metadata = read_metadata(&mut reader, &header)?;
        Ok(Self { path, header, metadata })
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn read(&self) -> ReadBuilder<'_> {
        ReadBuilder::new(self)
    }
}

pub struct ReadBuilder<'a> {
    reader: &'a SpssReader,
    columns: Option<Vec<String>>,
    schema: Option<Arc<Schema>>,
    offset: usize,
    limit: Option<usize>,
    parallel: bool,
    num_threads: Option<usize>,
    chunk_size: Option<usize>,
    missing_string_as_null: bool,
    user_missing_as_null: bool,
    value_labels_as_strings: bool,
}

impl<'a> ReadBuilder<'a> {
    fn new(reader: &'a SpssReader) -> Self {
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
            user_missing_as_null: true,
            value_labels_as_strings: true,
        }
    }

    pub fn with_columns(mut self, cols: Vec<String>) -> Self { self.columns = Some(cols); self }
    pub fn with_limit(mut self, limit: usize) -> Self { self.limit = Some(limit); self }
    pub fn with_offset(mut self, offset: usize) -> Self { self.offset = offset; self }
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self { self.schema = Some(schema); self }
    pub fn with_n_threads(mut self, n: usize) -> Self { self.num_threads = Some(n); self }
    pub fn with_chunk_size(mut self, n: usize) -> Self { self.chunk_size = Some(n); self }
    pub fn sequential(mut self) -> Self { self.parallel = false; self }
    pub fn missing_string_as_null(mut self, v: bool) -> Self { self.missing_string_as_null = v; self }
    pub fn user_missing_as_null(mut self, v: bool) -> Self { self.user_missing_as_null = v; self }
    pub fn value_labels_as_strings(mut self, v: bool) -> Self { self.value_labels_as_strings = v; self }

    pub fn finish(self) -> Result<DataFrame> {
        profile_reset();
        let limit = self.limit.unwrap_or(self.reader.metadata.row_count as usize);
        let cols = resolve_column_indices(&self.reader.metadata, self.columns.as_deref())?;

            let mut df = if self.parallel && limit > 0 {
            self.reader.read_parallel(
                self.offset,
                limit,
                self.num_threads,
                self.chunk_size,
                cols.as_deref(),
                self.missing_string_as_null,
                self.user_missing_as_null,
                self.value_labels_as_strings,
            )?
        } else {
            read_data_frame(
                &self.reader.path,
                &self.reader.metadata,
                self.reader.header.endian,
                self.reader.header.compression,
                self.reader.header.bias,
                cols.as_deref(),
                self.offset,
                limit,
                self.missing_string_as_null,
                self.user_missing_as_null,
                self.value_labels_as_strings,
            )?
        };

        if let Some(schema) = self.schema {
            df = cast_dataframe(df, &schema)?;
        }

        profile_print();
        Ok(df)
    }
}

impl SpssReader {
    const DEFAULT_CHUNK_SIZE: usize = 100_000;

    fn read_parallel(
        &self,
        offset: usize,
        count: usize,
        threads: Option<usize>,
        chunk_size: Option<usize>,
        cols: Option<&[usize]>,
        missing_string_as_null: bool,
        user_missing_as_null: bool,
        value_labels_as_strings: bool,
    ) -> Result<DataFrame> {
        if self.header.compression != 0 {
            return read_data_frame(
                &self.path,
                &self.metadata,
                self.header.endian,
                self.header.compression,
                self.header.bias,
                cols,
                offset,
                count,
                missing_string_as_null,
                user_missing_as_null,
                value_labels_as_strings,
            );
        }

        let n_threads = threads.unwrap_or_else(|| {
            let cur = rayon::current_num_threads();
            cur.min(4).max(1)
        });
        if n_threads <= 1 || count < 1000 {
            return read_data_frame(
                &self.path,
                &self.metadata,
                self.header.endian,
                self.header.compression,
                self.header.bias,
                cols,
                offset,
                count,
                missing_string_as_null,
                user_missing_as_null,
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

        let pool = ThreadPoolBuilder::new().num_threads(n_threads).build()
            .map_err(|e| Error::ParseError(format!("thread pool error: {}", e)))?;

        let mut dfs: Vec<(usize, Vec<Series>)> = pool.install(|| {
            (0..n_chunks)
                .into_par_iter()
                .map(|i| {
                    let start = offset + i * chunk_size;
                    let end = (offset + count).min(start + chunk_size);
                    let cnt = end - start;
                    read_data_columns_uncompressed(
                        &self.path,
                        &self.metadata,
                        self.header.endian,
                        cols,
                        start,
                        cnt,
                        missing_string_as_null,
                        user_missing_as_null,
                        value_labels_as_strings,
                    )
                    .map(|df| (i, df))
                })
                .collect::<Result<Vec<_>>>()
        })?;

        dfs.sort_by_key(|(i, _)| *i);
        let chunks = dfs.into_iter().map(|(_, df)| df).collect::<Vec<_>>();
        combine_column_chunks(chunks)
    }
}

fn combine_column_chunks(chunks: Vec<Vec<Series>>) -> Result<DataFrame> {
    if chunks.is_empty() {
        return Ok(DataFrame::empty());
    }
    if chunks.len() == 1 {
        let cols = chunks.into_iter().next().unwrap();
        let columns = cols.into_iter().map(Column::from).collect::<Vec<_>>();
        return DataFrame::new(columns).map_err(|e| Error::ParseError(e.to_string()));
    }

    let ncols = chunks[0].len();
    let mut columns: Vec<Vec<Series>> = (0..ncols)
        .map(|_| Vec::with_capacity(chunks.len()))
        .collect();

    for cols in chunks {
        if cols.len() != ncols {
            return Err(Error::ParseError("column count mismatch while combining frames".to_string()));
        }
        for (i, col) in cols.into_iter().enumerate() {
            columns[i].push(col);
        }
    }

    let combined: Vec<Column> = columns
        .into_par_iter()
        .map(|parts| {
            let mut iter = parts.into_iter();
            let mut out = iter
                .next()
                .ok_or_else(|| Error::ParseError("missing column parts".to_string()))?;
            for s in iter {
                out.append(&s)
                    .map_err(|e| Error::ParseError(format!("append failed: {}", e)))?;
            }
            Ok(Column::from(out))
        })
        .collect::<Result<Vec<_>>>()?;

    DataFrame::new(combined).map_err(|e| Error::ParseError(e.to_string()))
}

fn resolve_column_indices(metadata: &Metadata, cols: Option<&[String]>) -> Result<Option<Vec<usize>>> {
    cols.map(|cols| {
        cols.iter().map(|name| {
            metadata
                .variables
                .iter()
                .position(|v| v.name == *name)
                .ok_or_else(|| Error::ParseError(format!("Unknown column: {}", name)))
        }).collect::<Result<Vec<_>>>()
    }).transpose()
}

fn cast_dataframe(mut df: DataFrame, schema: &Schema) -> Result<DataFrame> {
    for (name, dtype) in schema.iter() {
        if let Ok(col) = df.column(name) {
            let casted = col
                .as_materialized_series()
                .cast(dtype)
                .map_err(|e| Error::ParseError(format!("Cast failed for {}: {}", name, e)))?;
            df.replace(name, casted)
                .map_err(|e| Error::ParseError(format!("Replace failed for {}: {}", name, e)))?;
        }
    }
    Ok(df)
}
