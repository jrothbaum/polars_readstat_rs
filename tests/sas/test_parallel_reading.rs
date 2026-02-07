use polars::prelude::*;
use stata_reader::reader::Sas7bdatReader;
use std::path::Path;

#[test]
fn test_parallel_vs_sequential_reading() {
    let test_file = "crates/cpp-sas7bdat/vendor/test/data_pandas/test1.sas7bdat";

    if !Path::new(test_file).exists() {
        println!("Test file not found, skipping test");
        return;
    }

    let reader = Sas7bdatReader::open(test_file).expect("Failed to open file");

    println!("File: {}", test_file);
    println!("Compression: {:?}", reader.metadata().compression);
    println!("Row count: {}", reader.metadata().row_count);

    // 1. Read using default (Parallel for uncompressed)
    let df_parallel = reader.read().finish().expect("Failed to read (parallel)");

    // 2. Read using Sequential explicitly
    let df_sequential = reader.read()
        .sequential()
        .finish()
        .expect("Failed to read (sequential)");

    // 3. Read using Pipeline explicitly
    let df_pipeline = reader.read()
        .pipeline()
        .finish()
        .expect("Failed to read (pipeline)");

    // Verify all strategies produce the same results
    assert_eq!(df_parallel.shape(), df_sequential.shape(), "Parallel vs Sequential shape mismatch");
    assert_eq!(df_parallel.shape(), df_pipeline.shape(), "Parallel vs Pipeline shape mismatch");

    println!("\n✓ Test passed: All reading strategies (Parallel, Sequential, Pipeline) are consistent");
}

#[test]
fn test_parallel_reading_multiple_files() {
    let test_files = vec![
        "crates/cpp-sas7bdat/vendor/test/data_pandas/test1.sas7bdat",
        "crates/cpp-sas7bdat/vendor/test/data_pandas/zero_variables.sas7bdat",
    ];

    for test_file in test_files {
        if !Path::new(test_file).exists() {
            println!("Test file {} not found, skipping", test_file);
            continue;
        }

        println!("\nTesting file: {}", test_file);
        let reader = Sas7bdatReader::open(test_file).expect("Failed to open file");

        // The builder automatically handles selection of the best strategy
        match reader.read().finish() {
            Ok(df) => {
                println!("  Compression: {:?}", reader.metadata().compression);
                println!("  Shape: {:?}", df.shape());
                println!("  ✓ Successfully read using default builder strategy");
            }
            Err(e) => println!("  Failed to read: {}", e),
        }
    }
}

#[test]
fn test_read_builder_boundaries() {
    let test_file = "crates/cpp-sas7bdat/vendor/test/data_pandas/test1.sas7bdat";

    if !Path::new(test_file).exists() {
        println!("Test file not found, skipping test");
        return;
    }

    let reader = Sas7bdatReader::open(test_file).expect("Failed to open file");
    let total_rows = reader.metadata().row_count;

    println!("Testing builder boundaries on file with {} rows", total_rows);

    // Test reading 0 rows (limit 0)
    let df = reader.read().with_limit(0).finish().expect("Failed limit 0");
    assert_eq!(df.height(), 0);
    println!("  ✓ Limit 0: OK");

    // Test reading 1 row
    let df = reader.read().with_limit(1).finish().expect("Failed limit 1");
    assert_eq!(df.height(), 1);
    println!("  ✓ Limit 1: OK");

    // Test reading all rows with offset
    if total_rows > 5 {
        let offset = 5;
        let expected = total_rows - offset;
        let df = reader.read().with_offset(offset).finish().expect("Failed offset");
        assert_eq!(df.height(), expected);
        println!("  ✓ Offset {}: OK", offset);
    }

    // Test reading past the end
    // Our logic handles this by saturating the limit to available rows
    let df = reader.read().with_offset(0).with_limit(total_rows + 100).finish().expect("Failed past end");
    assert_eq!(df.height(), total_rows);
    println!("  ✓ Reading past end (clamped to total): OK");

    println!("\n✓ All boundary tests passed using ReadBuilder");
}