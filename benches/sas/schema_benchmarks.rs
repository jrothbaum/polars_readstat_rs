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

fn bench_default_vs_sequential_read(c: &mut Criterion) {
    if let Some(file) = find_test_file("numeric_10000") {
        let mut group = c.benchmark_group("default_vs_sequential");

        // Benchmark default read (parallel)
        group.bench_function("default_read", |b| {
            b.iter(|| {
                let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                reader.read().finish().unwrap()
            });
        });

        // Benchmark sequential read
        group.bench_function("sequential_read", |b| {
            b.iter(|| {
                let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                reader.read().sequential().finish().unwrap()
            });
        });

        group.finish();
    }
}

fn bench_thread_scaling(c: &mut Criterion) {
    if let Some(file) = find_test_file("numeric_10000") {
        let mut group = c.benchmark_group("thread_scaling");

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

fn bench_batch_size_impact(c: &mut Criterion) {
    if let Some(file) = find_test_file("numeric_10000") {
        let mut group = c.benchmark_group("batch_size_reading");

        for batch_size in [100, 1000, 10000].iter() {
            group.bench_with_input(
                BenchmarkId::from_parameter(batch_size),
                batch_size,
                |b, &size| {
                    b.iter(|| {
                        let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                        let total_rows = reader.metadata().row_count;

                        let mut offset = 0;
                        let mut count = 0;
                        while offset < total_rows {
                            let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                            let read_size = std::cmp::min(size, total_rows - offset);
                            let _batch = reader.read()
                                .with_offset(black_box(offset))
                                .with_limit(black_box(read_size))
                                .finish()
                                .unwrap();
                            offset += read_size;
                            count += 1;
                        }
                        black_box(count)
                    });
                },
            );
        }

        group.finish();
    }
}

fn bench_large_file_reads(c: &mut Criterion) {
    if let Some(file) = find_test_file("numeric_100000") {
        let mut group = c.benchmark_group("large_file_reads");

        // Default read
        group.bench_function("large_default", |b| {
            b.iter(|| {
                let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                reader.read().finish().unwrap()
            });
        });

        // Sequential read
        group.bench_function("large_sequential", |b| {
            b.iter(|| {
                let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                reader.read().sequential().finish().unwrap()
            });
        });

        // Pipeline read
        group.bench_function("large_pipeline", |b| {
            b.iter(|| {
                let reader = Sas7bdatReader::open(black_box(&file)).unwrap();
                reader.read().pipeline().finish().unwrap()
            });
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_default_vs_sequential_read,
    bench_thread_scaling,
    bench_batch_size_impact,
    bench_large_file_reads,
);

criterion_main!(benches);
