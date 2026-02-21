use crate::data::{DataReader, DataSubheader};
use crate::error::Result;
use crate::page::PageReader;
use crate::polars_output::{ColumnPlan, DataFrameBuilder};
use crate::types::{Endian, Format, Header, Metadata};
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
    // We send (chunk_idx, flat_bytes, row_count) — one allocation per chunk instead of per row
    let (row_tx, row_rx) = mpsc::sync_channel::<(usize, Vec<u8>, usize)>(100);
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
            metadata_clone.clone(),
            endian,
            format,
            initial_data_subheaders_clone,
        )
        .ok()?;

        let row_length = metadata_clone.row_length;
        let limit = max_rows;
        let mut total_count = 0usize;
        let mut chunk_idx = 0usize;

        loop {
            let remaining = limit.map(|l| l.saturating_sub(total_count));
            if remaining == Some(0) {
                break;
            }
            let this_chunk_size = remaining.map(|r| r.min(chunk_size)).unwrap_or(chunk_size);
            let mut flat_chunk = vec![0u8; this_chunk_size * row_length];
            let mut chunk_count = 0usize;

            while chunk_count < this_chunk_size {
                match data_reader.read_row_borrowed() {
                    Ok(Some(row_slice)) => {
                        let start = chunk_count * row_length;
                        flat_chunk[start..start + row_length].copy_from_slice(row_slice);
                        chunk_count += 1;
                    }
                    _ => break,
                }
            }

            if chunk_count == 0 {
                break;
            }
            total_count += chunk_count;
            flat_chunk.truncate(chunk_count * row_length);
            if row_tx.send((chunk_idx, flat_chunk, chunk_count)).is_err() {
                break;
            }
            chunk_idx += 1;
            if chunk_count < this_chunk_size {
                break; // EOF reached mid-chunk
            }
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
            // Build plans once per worker thread
            let col_idx_slice = cols.as_deref().map(|v| v.as_slice());
            let plans =
                ColumnPlan::build_plans(&metadata, col_idx_slice, endian, missing_string_as_null);
            let row_length = metadata.row_length;

            while let Ok((chunk_idx, flat_buf, row_count)) = {
                let lock = rx.lock().unwrap();
                lock.recv()
            } {
                let mut builder = match col_idx_slice {
                    Some(idx) => DataFrameBuilder::new_with_columns(&metadata, idx, row_count),
                    None => DataFrameBuilder::new(metadata.clone(), row_count),
                };
                for i in 0..row_count {
                    let start = i * row_length;
                    builder.add_row_raw(&flat_buf[start..start + row_length], &plans);
                }
                if let Ok(df) = builder.build() {
                    if tx.send((chunk_idx, df)).is_err() {
                        break;
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
