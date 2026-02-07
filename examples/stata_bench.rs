use std::path::PathBuf;
use std::time::Instant;

use polars_readstat_rs::StataReader;

/// Usage:
///   stata_bench <file> <n_rows|full> [--columns a,b,c] [--col-count N] [--threads N]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let file = args.next().ok_or("missing file")?;
    let rows = args.next().ok_or("missing rows")?;

    let mut columns: Option<Vec<String>> = None;
    let mut col_count: Option<usize> = None;
    let mut threads: Option<usize> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--columns" => {
                let list = args.next().ok_or("missing --columns value")?;
                let cols = list.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                columns = Some(cols);
            }
            "--col-count" => {
                let v = args.next().ok_or("missing --col-count value")?;
                col_count = Some(v.parse()?);
            }
            "--threads" => {
                let v = args.next().ok_or("missing --threads value")?;
                threads = Some(v.parse()?);
            }
            _ => return Err(format!("unknown arg: {}", arg).into()),
        }
    }

    let reader = StataReader::open(PathBuf::from(&file))?;
    let mut builder = reader.read().value_labels_as_strings(false);
    if let Some(n) = threads {
        builder = builder.with_n_threads(n);
    }

    if let Some(cols) = columns {
        builder = builder.with_columns(cols);
    } else if let Some(n) = col_count {
        let names: Vec<String> = reader
            .metadata()
            .variables
            .iter()
            .take(n)
            .map(|v| v.name.clone())
            .collect();
        builder = builder.with_columns(names);
    }

    if rows != "full" {
        builder = builder.with_limit(rows.parse()?);
    }

    let start = Instant::now();
    let _df = builder.finish()?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!("elapsed_ms={:.3}", elapsed_ms);

    Ok(())
}
