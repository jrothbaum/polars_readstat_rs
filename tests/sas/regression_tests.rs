mod common;

use stata_reader::reader::Sas7bdatReader;
use stata_reader::{scan_sas7bdat, ScanOptions};
use common::{test_data_path, all_sas_files};
use polars::prelude::*;

/// Regression test: verify specific data values using the Lazy API
#[test]
fn test_test1_data_values() {
    let path = test_data_path("test1.sas7bdat");
    if !path.exists() { return; }

    // Use the new AnonymousScan entry point
    let df = scan_sas7bdat(path, Default::default())
        .expect("Failed to create LazyFrame")
        .collect()
        .expect("Failed to collect LazyFrame");

    assert_eq!(df.height(), 10, "test1 should have 10 rows");
    assert!(df.width() >= 4, "test1 should have at least 4 columns");

    let col_names = df.get_column_names();
    assert!(col_names.iter().any(|&n| n == "Column1"), "Should have Column1");

    // In modern Polars, we use .column() which returns a Series
    let col1 = df.column("Column1").unwrap();
    if col1.dtype() == &DataType::Float64 {
        let values = col1.f64().unwrap();
        assert!(values.get(0).is_some(), "First row should have Column1 value");
    }
}

/// Regression test: verify that Projection Pushdown (column selection) works
#[test]
fn test_column_projection() {
    let path = test_data_path("test1.sas7bdat");
    if !path.exists() { return; }

    // This specifically tests that your SasScan resolves names to indices correctly
    let df = scan_sas7bdat(path, Default::default())
        .unwrap()
        .select([col("Column1")])
        .collect()
        .unwrap();

    assert_eq!(df.width(), 1);
    assert_eq!(df.get_column_names()[0], "Column1");
}

/// Regression test: verify limit pushdown
#[test]
fn test_limit_pushdown() {
    let path = test_data_path("test1.sas7bdat");
    if !path.exists() { return; }

    // This tests that opts.n_rows is passed correctly to your pipeline
    let df = scan_sas7bdat(path, Default::default())
        .unwrap()
        .limit(5)
        .collect()
        .unwrap();

    assert_eq!(df.height(), 5);
}

/// Regression test: verify compressed file decompression via Pipeline
#[test]
fn test_compressed_data_validity() {
    let files = all_sas_files();
    let compressed: Vec<_> = files.iter()
        .filter(|p| {
            let s = p.to_string_lossy();
            s.contains("compressed") || s.contains("rdc")
        })
        .collect();

    for file in compressed {
        // The Lazy API/Pipeline automatically handles decompression
        // because the I/O thread uses your DataReader
        let mut opts = ScanOptions::default();
        opts.threads = Some(1);
        let df = scan_sas7bdat(file.to_path_buf(), opts)
            .unwrap()
            .collect()
            .unwrap();

        assert!(df.height() > 0, "Compressed file {} read 0 rows", file.display());
    }
}

// NOTE: test_arrow_stream_integration removed - stata_reader::arrow_output::read_to_arrow
// no longer exists. Only read_to_arrow_ffi remains in the arrow_output module.

/// Regression test: verify metadata accessibility
#[test]
fn test_metadata_accuracy() {
    let files: Vec<_> = all_sas_files().into_iter()
        .filter(|f| std::fs::metadata(f).map(|m| m.len() < 500 * 1024 * 1024).unwrap_or(false))
        .collect();
    let sample = files.iter().take(5);

    for file in sample {
        let reader = Sas7bdatReader::open(file).unwrap();
        let meta = reader.metadata();

        // Ensure the scan produces the same dimensions metadata claims
        let df = scan_sas7bdat(file.to_path_buf(), Default::default())
            .unwrap()
            .collect()
            .unwrap();

        assert_eq!(df.height(), meta.row_count, "Row mismatch in {}", file.display());
        assert_eq!(df.width(), meta.column_count, "Col mismatch in {}", file.display());
    }
}
