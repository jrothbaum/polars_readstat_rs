use polars_arrow::ffi::ArrowArrayStreamReader;
use std::path::PathBuf;
use stata_reader::sas::arrow_output::read_to_arrow_stream_ffi as sas_stream;
use stata_reader::stata::arrow_output::read_to_arrow_stream_ffi as stata_stream;
use stata_reader::spss::arrow_output::read_to_arrow_stream_ffi as spss_stream;

fn big_sas_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("sas")
        .join("data")
        .join("psam_p17.sas7bdat")
}

fn big_stata_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("stata")
        .join("data")
        .join("usa_00009.dta")
}

fn big_spss_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("spss")
        .join("data")
        .join("ess_data.sav")
}

#[test]
fn test_sas_arrow_stream_batches() {
    let path = big_sas_path();
    if !path.exists() {
        return;
    }
    let stream = sas_stream(&path, None, true, None, 50_000).expect("stream");
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(Box::from_raw(stream)) }.expect("reader");
    let mut batches = 0usize;
    let mut rows = 0usize;
    unsafe {
        while let Some(batch) = reader.next() {
            let batch = batch.expect("batch");
            rows += batch.len();
            batches += 1;
            if batches >= 2 {
                break;
            }
        }
    }
    assert!(batches >= 2);
    assert!(rows > 0);
}

#[test]
fn test_stata_arrow_stream_batches() {
    let path = big_stata_path();
    if !path.exists() {
        return;
    }
    let stream = stata_stream(&path, None, true, Some(true), None, 50_000).expect("stream");
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(Box::from_raw(stream)) }.expect("reader");
    let mut batches = 0usize;
    let mut rows = 0usize;
    unsafe {
        while let Some(batch) = reader.next() {
            let batch = batch.expect("batch");
            rows += batch.len();
            batches += 1;
            if batches >= 2 {
                break;
            }
        }
    }
    assert!(batches >= 2);
    assert!(rows > 0);
}

#[test]
fn test_spss_arrow_stream_batches() {
    let path = big_spss_path();
    if !path.exists() {
        return;
    }
    let stream = spss_stream(&path, None, true, Some(true), None, 50_000).expect("stream");
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(Box::from_raw(stream)) }.expect("reader");
    let mut batches = 0usize;
    let mut rows = 0usize;
    unsafe {
        while let Some(batch) = reader.next() {
            let batch = batch.expect("batch");
            rows += batch.len();
            batches += 1;
            if batches >= 2 {
                break;
            }
        }
    }
    assert!(batches >= 2);
    assert!(rows > 0);
}
