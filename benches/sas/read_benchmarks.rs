use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use polars_readstat_rs::reader::Sas7bdatReader;
use std::path::PathBuf;
use glob::glob;

fn find_test_files() -> Vec<PathBuf> {
    let pattern = format!(
        "{}/tests/sas/data/**/*.sas7bdat",
        env!("CARGO_MANIFEST_DIR")
    );

    glob(&pattern)
        .expect("Failed to read glob pattern")
        .filter_map(Result::ok)
        .collect()
}

fn find_test_file(pattern: &str) -> Option<PathBuf> {
    let files = find_test_files();
    files.into_iter().find(|p| p.to_string_lossy().contains(pattern))
}

fn bench_open_file(c: &mut Criterion) {
    if let Some(file) = find_test_file("test1") {
        c.bench_function("open_test1", |b| {
            b.iter(|| {
                Sas7bdatReader::open(black_box(&file)).unwrap()
            });
        });
    }
}

fn bench_read_small_file(c: &mut Criterion) {
    if let Some(file) = find_test_file("test1") {
        c.bench_function("read_small_file", |b| {
            b.iter(|| {
                let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                reader.read().finish().unwrap()
            });
        });
    }
}

fn bench_read_large_file(c: &mut Criterion) {
    // Look for a larger numeric file
    if let Some(file) = find_test_file("numeric_1000000") {
        c.bench_function("read_large_file", |b| {
            b.iter(|| {
                let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                reader.read().finish().unwrap()
            });
        });
    }
}

fn bench_read_compressed(c: &mut Criterion) {
    if let Some(file) = find_test_file("compressed") {
        c.bench_function("read_compressed", |b| {
            b.iter(|| {
                let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                reader.read().finish().unwrap()
            });
        });
    }
}

fn bench_batch_reading(c: &mut Criterion) {
    if let Some(file) = find_test_file("test1") {
        let reader = Sas7bdatReader::open(&file).unwrap();
        let total_rows = reader.metadata().row_count;

        let mut group = c.benchmark_group("batch_reading");

        for batch_size in [10, 100, 1000].iter() {
            if *batch_size <= total_rows {
                group.bench_with_input(
                    BenchmarkId::from_parameter(batch_size),
                    batch_size,
                    |b, &size| {
                        b.iter(|| {
                            let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                            reader.read().with_offset(black_box(0)).with_limit(black_box(size)).finish().unwrap()
                        });
                    },
                );
            }
        }
        group.finish();
    }
}

fn bench_parallel_reading(c: &mut Criterion) {
    if let Some(file) = find_test_file("numeric_1000000") {
        let mut group = c.benchmark_group("parallel_reading");

        for threads in [1, 2, 4, 8].iter() {
            group.bench_with_input(
                BenchmarkId::from_parameter(threads),
                threads,
                |b, &num_threads| {
                    b.iter(|| {
                        let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                        reader.read().with_n_threads(black_box(num_threads)).finish().unwrap()
                    });
                },
            );
        }
        group.finish();
    }
}

fn bench_metadata_access(c: &mut Criterion) {
    if let Some(file) = find_test_file("test1") {
        let reader = Sas7bdatReader::open(&file).unwrap();

        c.bench_function("metadata_row_count", |b| {
            b.iter(|| {
                black_box(reader.metadata().row_count)
            });
        });

        c.bench_function("metadata_columns", |b| {
            b.iter(|| {
                black_box(&reader.metadata().columns)
            });
        });
    }
}

fn bench_multiple_files(c: &mut Criterion) {
    let files = find_test_files();
    let sample: Vec<_> = files.iter().take(5).collect();

    if !sample.is_empty() {
        c.bench_function("open_multiple_files", |b| {
            b.iter(|| {
                for file in &sample {
                    let _reader = Sas7bdatReader::open(black_box(file)).unwrap();
                }
            });
        });

        c.bench_function("read_multiple_files", |b| {
            b.iter(|| {
                for file in &sample {
                    let reader = Sas7bdatReader::open(black_box(file)).unwrap();
                    let _df = reader.read().finish().unwrap();
                }
            });
        });
    }
}

fn bench_file_types(c: &mut Criterion) {
    let files = find_test_files();

    // Categorize files
    let numeric: Vec<_> = files.iter()
        .filter(|p| p.to_string_lossy().contains("numeric"))
        .take(2)
        .collect();

    let compressed: Vec<_> = files.iter()
        .filter(|p| p.to_string_lossy().contains("compressed") ||
                     p.to_string_lossy().contains("rdc"))
        .take(2)
        .collect();

    let mut group = c.benchmark_group("file_types");

    if !numeric.is_empty() {
        group.bench_function("numeric_files", |b| {
            b.iter(|| {
                for file in &numeric {
                    let reader = Sas7bdatReader::open(black_box(file)).unwrap();
                    let _df = reader.read().finish().unwrap();
                }
            });
        });
    }

    if !compressed.is_empty() {
        group.bench_function("compressed_files", |b| {
            b.iter(|| {
                for file in &compressed {
                    let reader = Sas7bdatReader::open(black_box(file)).unwrap();
                    let _df = reader.read().finish().unwrap();
                }
            });
        });
    }

    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(2))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets =
        bench_open_file,
        bench_read_small_file,
        bench_read_large_file,
        bench_read_compressed,
        bench_batch_reading,
        bench_parallel_reading,
        bench_metadata_access,
        bench_multiple_files,
        bench_file_types,
}

criterion_main!(benches);
