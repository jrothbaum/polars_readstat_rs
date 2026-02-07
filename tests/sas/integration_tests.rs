mod common;

use stata_reader::reader::Sas7bdatReader;
use common::{all_sas_files, test_data_path};
use std::sync::Arc;

#[test]
fn test_all_files_can_be_opened() {
    let files = all_sas_files();
    println!("Testing {} files...", files.len());

    let mut failed = Vec::new();
    let mut succeeded = 0;

    for file in &files {
        match Sas7bdatReader::open(file) {
            Ok(_) => succeeded += 1,
            Err(e) => failed.push((file.clone(), e)),
        }
    }

    println!("Success: {}/{}", succeeded, files.len());
    assert_eq!(failed.len(), 0, "{} files failed to open", failed.len());
}

#[test]
fn test_all_files_can_read_metadata() {
    let files = all_sas_files();
    let mut failed = Vec::new();

    for file in &files {
        match Sas7bdatReader::open(file) {
            Ok(reader) => {
                let metadata = reader.metadata();
                if metadata.row_count == 0 && metadata.column_count == 0 {
                    failed.push((file.clone(), "Zero rows and columns".to_string()));
                }
            }
            Err(e) => failed.push((file.clone(), e.to_string())),
        }
    }

    assert_eq!(failed.len(), 0, "{} files failed metadata check", failed.len());
}

#[test]
fn test_all_files_can_read_data() {
    let files = all_sas_files();
    let mut failed = Vec::new();
    let mut skipped = 0;

    for file in &files {
        if file.to_string_lossy().contains("zero_variables.sas7bdat") {
            skipped += 1;
            continue;
        }

        // Limit check for test speed
        if let Ok(m) = std::fs::metadata(file) {
            if m.len() > 500 * 1024 * 1024 { // Skip > 500MB for general CI
                skipped += 1;
                continue;
            }
        }

        match Sas7bdatReader::open(file) {
            Ok(reader) => {
                // Using new builder pattern
                match reader.read().finish() {
                    Ok(df) => {
                        assert_eq!(df.height(), reader.metadata().row_count);
                        assert_eq!(df.width(), reader.metadata().column_count);
                    }
                    Err(e) => failed.push((file.clone(), e.to_string())),
                }
            }
            Err(e) => failed.push((file.clone(), e.to_string())),
        }
    }
    assert_eq!(failed.len(), 0, "{} files failed to read data", failed.len());
}

#[test]
fn test_batch_reading_matches_full_read() {
    let test_file = test_data_path("test1.sas7bdat");
    if !test_file.exists() { return; }

    let reader = Sas7bdatReader::open(&test_file).unwrap();
    let full_df = reader.read().finish().unwrap();

    let mid = full_df.height() / 2;
    
    // Batch 1 using Builder
    let b1 = reader.read()
        .with_offset(0)
        .with_limit(mid)
        .finish().unwrap();

    // Batch 2 using Builder
    let b2 = reader.read()
        .with_offset(mid)
        .with_limit(full_df.height() - mid)
        .finish().unwrap();

    assert_eq!(b1.height() + b2.height(), full_df.height());
}

#[test]
fn test_parallel_and_pipeline_match() {
    let test_file = test_data_path("test1.sas7bdat");
    if !test_file.exists() { return; }

    let reader = Sas7bdatReader::open(&test_file).unwrap();

    // 1. Parallel (default)
    let df_par = reader.read().finish().unwrap();

    // 2. Sequential
    let df_seq = reader.read().sequential().finish().unwrap();

    // 3. Pipeline
    let df_pipe = reader.read().pipeline().finish().unwrap();

    assert_eq!(df_par.height(), df_seq.height());
    assert_eq!(df_par.height(), df_pipe.height());
}

#[test]
fn test_column_selection_builder() {
    let test_file = test_data_path("psam_p17.sas7bdat");
    if !test_file.exists() { return; }

    let reader = Sas7bdatReader::open(&test_file).unwrap();
    let selected = vec!["RT".to_string(), "AGEP".to_string()];

    let df = reader.read()
        .with_columns(selected.clone())
        .finish()
        .unwrap();

    assert_eq!(df.width(), 2);
    assert!(df.column("RT").is_ok());
    assert!(df.column("AGEP").is_ok());
}

#[test]
fn test_large_file_streaming_pipeline() {
    let files = all_sas_files();

    for file in &files {
        if let Ok(metadata) = std::fs::metadata(file) {
            if metadata.len() > 100 * 1024 * 1024 && metadata.len() < 1024 * 1024 * 1024 { // 100MB-1GB (skip 11GB+ files)
                let reader = Sas7bdatReader::open(file).unwrap();
                
                // For large files, test the Pipeline with a limit
                // This validates the I/O + Worker logic without filling RAM
                let df = reader.read()
                    .pipeline()
                    .with_limit(5000)
                    .finish()
                    .unwrap();

                assert_eq!(df.height(), 5000);
                println!("✓ Pipeline limit test passed for {}", file.display());
            }
        }
    }
}

#[test]
fn test_error_on_missing_column() {
    let test_file = test_data_path("test1.sas7bdat");
    if !test_file.exists() { return; }

    let reader = Sas7bdatReader::open(&test_file).unwrap();
    let res = reader.read()
        .with_columns(vec!["TOTALLY_REAL_COLUMN".into()])
        .finish();

    assert!(res.is_err());
}