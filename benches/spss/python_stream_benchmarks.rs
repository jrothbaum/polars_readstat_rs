use criterion::{black_box, criterion_group, criterion_main, Criterion};
use glob::glob;
use polars::prelude::DataFrame;
use polars_readstat_rs::SpssReader;
use std::cmp::min;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::JoinHandle;

enum ChunkMessage {
    Data { idx: usize, df: DataFrame },
    Done,
    Err(String),
}

fn find_test_files() -> Vec<PathBuf> {
    let pattern = format!("{}/tests/spss/data/**/*.sav", env!("CARGO_MANIFEST_DIR"));
    glob(&pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect()
}

fn find_test_file(pattern: &str) -> Option<PathBuf> {
    let files = find_test_files();
    files
        .into_iter()
        .find(|p| p.to_string_lossy().contains(pattern))
}

fn find_large_file() -> Option<PathBuf> {
    find_test_file("GSS2024").or_else(|| find_test_file("sample_large"))
}

fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1)
}

// Rust-only benchmark for the Python streaming usage pattern.
fn stream_spss_python_like(
    path: &Path,
    batch_size: usize,
    threads: usize,
    missing_string_as_null: bool,
    value_labels_as_strings: bool,
    n_rows: Option<usize>,
    window_chunks: usize,
) -> Result<usize, String> {
    let meta_reader = SpssReader::open(path).map_err(|e| e.to_string())?;
    let total_rows = n_rows
        .unwrap_or(meta_reader.metadata().row_count as usize)
        .min(meta_reader.metadata().row_count as usize);
    let total_chunks = total_rows.div_ceil(batch_size.max(1));
    if total_chunks == 0 {
        return Ok(0);
    }

    let n_workers = min(threads.max(1), total_chunks.max(1));
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
        let path = path.to_path_buf();
        let batch_size = batch_size;
        let total_rows = total_rows;
        let missing_string_as_null = missing_string_as_null;
        let value_labels_as_strings = value_labels_as_strings;
        let window_chunks = window_chunks;

        let handle = std::thread::spawn(move || {
            let reader = match SpssReader::open(&path) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(ChunkMessage::Err(e.to_string()));
                    return;
                }
            };

            let mut chunk_idx = start_chunk;
            while chunk_idx < end_chunk {
                let window_end = (chunk_idx + window_chunks).min(end_chunk);
                let offset = chunk_idx * batch_size;
                if offset >= total_rows {
                    break;
                }
                let window_rows = (window_end - chunk_idx) * batch_size;
                let take = (total_rows - offset).min(window_rows);

                let window_df = match reader
                    .read()
                    .with_offset(offset)
                    .with_limit(take)
                    .missing_string_as_null(missing_string_as_null)
                    .value_labels_as_strings(value_labels_as_strings)
                    .sequential()
                    .finish()
                {
                    Ok(df) => df,
                    Err(e) => {
                        let _ = tx.send(ChunkMessage::Err(e.to_string()));
                        return;
                    }
                };

                let mut sent = 0usize;
                for out_idx in chunk_idx..window_end {
                    let local_offset = (out_idx - chunk_idx) * batch_size;
                    if local_offset >= window_df.height() {
                        break;
                    }
                    let local_take = min(batch_size, window_df.height() - local_offset);
                    let df = window_df.slice(local_offset as i64, local_take);
                    let _ = tx.send(ChunkMessage::Data { idx: out_idx, df });
                    sent += 1;
                }
                if sent == 0 {
                    break;
                }

                chunk_idx = window_end;
            }

            let _ = tx.send(ChunkMessage::Done);
        });
        handles.push(handle);
    }
    drop(tx);

    let mut buffer: BTreeMap<usize, DataFrame> = BTreeMap::new();
    let mut next_idx = 0usize;
    let mut completed = 0usize;
    let mut total_out_rows = 0usize;
    let mut maybe_err: Option<String> = None;

    while completed < n_workers {
        if let Some(df) = buffer.remove(&next_idx) {
            total_out_rows += df.height();
            next_idx += 1;
            continue;
        }

        match rx.recv() {
            Ok(ChunkMessage::Data { idx, df }) => {
                buffer.insert(idx, df);
            }
            Ok(ChunkMessage::Done) => {
                completed += 1;
            }
            Ok(ChunkMessage::Err(e)) => {
                maybe_err = Some(e);
                break;
            }
            Err(_) => {
                break;
            }
        }
    }

    for handle in handles {
        let _ = handle.join();
    }

    if let Some(err) = maybe_err {
        return Err(err);
    }
    if total_out_rows != total_rows {
        return Err(format!(
            "streamed row count mismatch: got {}, expected {}",
            total_out_rows, total_rows
        ));
    }
    Ok(total_out_rows)
}

fn bench_python_stream_large_file(c: &mut Criterion) {
    if let Some(file) = find_large_file() {
        c.bench_function("python_stream_large_file", |b| {
            b.iter(|| {
                let rows = stream_spss_python_like(
                    black_box(&file),
                    black_box(100_000),
                    black_box(default_threads()),
                    black_box(false),
                    black_box(false),
                    black_box(None),
                    black_box(8),
                )
                .expect("python-like stream should succeed");
                black_box(rows);
            });
        });
    }
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(20))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_python_stream_large_file,
}

criterion_main!(benches);
