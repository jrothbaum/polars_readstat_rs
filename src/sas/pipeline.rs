use crate::data::{DataReader, DataSubheader};
use crate::error::Result;
use crate::page::PageReader;
use crate::polars_output::DataFrameBuilder;
use crate::polars_output::{kind_for_column, ColumnKind};
use crate::types::{ColumnType, Endian, Format, Header, Metadata};
use crate::value::{decode_numeric_bytes_mask, parse_row_values_into, Value, ValueParser};
use polars::prelude::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

pub const DEFAULT_PIPELINE_CHUNK_SIZE: usize = 8192;

pub fn read_pipeline(
    path: &Path,
    header: &Header,
    metadata: &Metadata,
    endian: Endian,
    format: Format,
    num_threads: usize,
    chunk_size: usize,
    max_rows: Option<usize>,
    col_indices: Option<&[usize]>,
    missing_string_as_null: bool,
    initial_data_subheaders: &[DataSubheader],
) -> Result<DataFrame> {
    // We send (chunk_idx, Vec<row_bytes>) so workers can build columnar chunks
    let (row_tx, row_rx) = mpsc::sync_channel::<(usize, Vec<Vec<u8>>)>(100);
    let (result_tx, result_rx) = mpsc::channel::<(usize, DataFrame)>();

    let row_rx = Arc::new(Mutex::new(row_rx));
    let col_indices_arc = col_indices.map(|c| Arc::new(c.to_vec()));

    // 1. PRODUCER
    let path_buf = path.to_path_buf();
    let header_clone = header.clone();
    let metadata_clone = metadata.clone();
    let initial_data_subheaders_clone = initial_data_subheaders.to_vec();
    thread::spawn(move || {
        let file = File::open(&path_buf).ok()?;
        let mut reader = BufReader::new(file);
        reader
            .seek(SeekFrom::Start(header_clone.header_length as u64))
            .ok()?;
        let page_reader = PageReader::new(reader, header_clone, endian, format);
        let mut data_reader = DataReader::new(
            page_reader,
            metadata_clone,
            endian,
            format,
            initial_data_subheaders_clone,
        )
        .ok()?;

        let mut count = 0usize;
        let mut chunk_idx = 0usize;
        let mut chunk: Vec<Vec<u8>> = Vec::with_capacity(chunk_size);
        let limit = max_rows;
        while let Ok(Some(row_bytes)) = data_reader.read_row() {
            chunk.push(row_bytes);
            count += 1;
            if chunk.len() >= chunk_size {
                if row_tx
                    .send((chunk_idx, std::mem::take(&mut chunk)))
                    .is_err()
                {
                    break;
                }
                chunk_idx += 1;
            }
            if let Some(limit) = limit {
                if count >= limit {
                    break;
                }
            }
        }
        if !chunk.is_empty() {
            let _ = row_tx.send((chunk_idx, chunk));
        }
        Some(())
    });

    // 2. WORKERS
    for _ in 0..num_threads {
        let rx = Arc::clone(&row_rx);
        let tx = result_tx.clone();
        let metadata = metadata.clone();
        let cols = col_indices_arc.clone();
        thread::spawn(move || {
            while let Ok((chunk_idx, rows)) = {
                let lock = rx.lock().unwrap();
                lock.recv()
            } {
                let col_idx_slice = cols.as_deref().map(|v| v.as_slice());
                let mut builder = match col_idx_slice {
                    Some(idx) => DataFrameBuilder::new_with_columns(&metadata, idx, rows.len()),
                    None => DataFrameBuilder::new(metadata.clone(), rows.len()),
                };
                let numeric_only = match col_idx_slice {
                    Some(indices) => indices
                        .iter()
                        .all(|&idx| metadata.columns[idx].col_type == ColumnType::Numeric),
                    None => false,
                };
                struct NumericColumnPlan {
                    pos: usize,
                    start: usize,
                    end: usize,
                    kind: ColumnKind,
                }

                let numeric_plan: Option<Vec<NumericColumnPlan>> = if numeric_only {
                    col_idx_slice.map(|indices| {
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
                let parser =
                    ValueParser::new(endian, metadata.encoding_byte, missing_string_as_null);
                let mut row_values: Vec<Value> = Vec::with_capacity(
                    col_idx_slice
                        .map(|v| v.len())
                        .unwrap_or(metadata.columns.len()),
                );
                let mut ok = true;
                for row_bytes in rows {
                    if numeric_only {
                        if let Some(plans) = numeric_plan.as_ref() {
                            if let Some(max_end) = numeric_max_end {
                                if row_bytes.len() < max_end {
                                    ok = false;
                                    break;
                                }
                            }
                            for plan in plans {
                                let (value, is_missing) = decode_numeric_bytes_mask(
                                    endian,
                                    &row_bytes[plan.start..plan.end],
                                );
                                builder.add_numeric_value_mask_kind(
                                    plan.pos, plan.kind, value, is_missing,
                                );
                            }
                            if !ok {
                                break;
                            }
                        }
                    } else {
                        match parse_row_values_into(
                            &parser,
                            &row_bytes,
                            &metadata,
                            col_idx_slice,
                            &mut row_values,
                        ) {
                            Ok(()) => {
                                if builder.add_row_ref(&row_values).is_err() {
                                    ok = false;
                                    break;
                                }
                            }
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    }
                }
                if ok {
                    if let Ok(df) = builder.build() {
                        if tx.send((chunk_idx, df)).is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }
    drop(result_tx);

    // 3. CONSUMER (With Re-ordering)
    let mut reorder_buffer: BTreeMap<usize, DataFrame> = BTreeMap::new();
    let mut next_chunk = 0usize;
    let mut out: Option<DataFrame> = None;

    while let Ok((chunk_idx, df)) = result_rx.recv() {
        reorder_buffer.insert(chunk_idx, df);
        while let Some(df) = reorder_buffer.remove(&next_chunk) {
            if let Some(ref mut existing) = out {
                existing.vstack_mut(&df)?;
            } else {
                out = Some(df);
            }
            next_chunk += 1;
        }
    }

    Ok(out.unwrap_or_else(DataFrame::empty))
}
