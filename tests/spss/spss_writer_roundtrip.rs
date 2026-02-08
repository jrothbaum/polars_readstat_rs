use polars::prelude::*;
use polars_readstat_rs::{SpssReader, SpssWriter};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    path.push(format!("{prefix}_{pid}_{nanos}.{ext}"));
    path
}

#[test]
fn test_spss_roundtrip_basic() {
    let df = DataFrame::new(vec![
        Series::new("id".into(), &[1i32, 2, 3]).into_column(),
        Series::new("name".into(), &["alice", "bob", "carol"]).into_column(),
    ]).unwrap();

    let path = temp_path("spss_roundtrip", "sav");
    SpssWriter::new(&path).write_df(&df).unwrap();

    let out = SpssReader::open(&path).unwrap().read().finish().unwrap();
    assert_eq!(out.shape(), df.shape());

    let _ = fs::remove_file(&path);
}
