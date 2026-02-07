use polars::prelude::*;
use stata_reader::{scan_sav, ScanOptions};

fn main() -> PolarsResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: spss_scan <file> [threads] [chunk_size] [missing_string_as_null] [value_labels_as_strings]");
        std::process::exit(1);
    }

    let path = args[1].clone();
    let threads = args.get(2).and_then(|s| s.parse::<usize>().ok());
    let chunk_size = args.get(3).and_then(|s| s.parse::<usize>().ok());
    let missing_string_as_null = args
        .get(4)
        .map(|s| s == "true" || s == "1")
        .unwrap_or(true);
    let value_labels_as_strings = args
        .get(5)
        .map(|s| s == "true" || s == "1")
        .unwrap_or(true);

    let opts = ScanOptions {
        threads,
        chunk_size,
        missing_string_as_null: Some(missing_string_as_null),
        value_labels_as_strings: Some(value_labels_as_strings),
    };
    let df = scan_sav(path, opts)?.collect()?;
    println!("rows={} cols={}", df.height(), df.width());
    Ok(())
}
