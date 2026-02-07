use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use glob::glob;
use stata_reader::SpssReader;
use std::path::PathBuf;

fn find_test_files() -> Vec<PathBuf> {
    let pattern = format!(
        "{}/tests/spss/data/**/*.sav",
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

fn find_large_file() -> Option<PathBuf> {
    find_test_file("GSS2024").or_else(|| find_test_file("sample_large"))
}

fn bench_default_vs_sequential_read(c: &mut Criterion) {
    if let Some(file) = find_test_file("sample") {
        let mut group = c.benchmark_group("default_vs_sequential");

        group.bench_function("default_read", |b| {
            b.iter(|| {
                let reader = SpssReader::open(black_box(&file)).unwrap();
                reader.read().finish().unwrap()
            });
        });

        group.bench_function("sequential_read", |b| {
            b.iter(|| {
                let reader = SpssReader::open(black_box(&file)).unwrap();
                reader.read().finish().unwrap()
            });
        });

        group.finish();
    }
}

fn bench_batch_size_impact(c: &mut Criterion) {
    if let Some(file) = find_test_file("sample") {
        let mut group = c.benchmark_group("batch_size_reading");

        for batch_size in [100, 1000, 10000].iter() {
            group.bench_with_input(
                BenchmarkId::from_parameter(batch_size),
                batch_size,
                |b, &size| {
                    b.iter(|| {
                        let reader = SpssReader::open(black_box(&file)).unwrap();
                        let total_rows = reader.metadata().row_count as usize;

                        let mut offset = 0usize;
                        let mut count = 0usize;
                        while offset < total_rows {
                            let reader = SpssReader::open(black_box(&file)).unwrap();
                            let read_size = std::cmp::min(size, total_rows - offset);
                            let _batch = reader
                                .read()
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
    if let Some(file) = find_large_file() {
        let mut group = c.benchmark_group("large_file_reads");

        group.bench_function("large_default", |b| {
            b.iter(|| {
                let reader = SpssReader::open(black_box(&file)).unwrap();
                reader.read().finish().unwrap()
            });
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_default_vs_sequential_read,
    bench_batch_size_impact,
    bench_large_file_reads,
);

criterion_main!(benches);
