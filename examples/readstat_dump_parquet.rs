use polars::io::parquet::write::ParquetWriter;
use polars::prelude::{col, PolarsResult};
use polars_readstat_rs::{readstat_scan, ScanOptions};
use std::fs::{self, File};
use std::path::PathBuf;

fn main() -> PolarsResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: readstat_dump_parquet <input> <output> [rows] [cols]");
        std::process::exit(1);
    }

    let input = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);
    let rows: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(0);
    let cols: usize = args.get(4).and_then(|v| v.parse().ok()).unwrap_or(0);

    let mut opts = ScanOptions::default();
    // Match polars_readstat defaults for regression comparisons
    opts.missing_string_as_null = Some(false);
    opts.user_missing_as_null = Some(false);
    opts.value_labels_as_strings = Some(false);
    let mut lf = readstat_scan(&input, Some(opts), None)?;

    if cols > 0 {
        let schema = lf.collect_schema()?;
        let names: Vec<String> = schema
            .iter_names()
            .take(cols)
            .map(|s| s.to_string())
            .collect();
        let exprs = names.iter().map(|s| col(s.as_str())).collect::<Vec<_>>();
        lf = lf.select(exprs);
    }

    if rows > 0 {
        lf = lf.limit(rows as u32);
    }

    let mut df = lf.collect()?;

    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(polars::prelude::PolarsError::from)?;
    }

    let file = File::create(&output).map_err(polars::prelude::PolarsError::from)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}
