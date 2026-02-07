use stata_reader::SpssReader;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let no_labels = if let Some(pos) = args.iter().position(|v| v == "--no-labels") {
        args.remove(pos);
        true
    } else {
        false
    };
    let mut args = args.into_iter();
    let path = args
        .next()
        .ok_or("Usage: spss_bench_read <file> <rows>|full [--no-labels]")?;
    let rows_arg = args.next().unwrap_or_else(|| "full".to_string());
    let rows = if rows_arg == "full" {
        None
    } else {
        Some(rows_arg.parse::<usize>()?)
    };

    let file = PathBuf::from(path);
    let start = Instant::now();
    let reader = SpssReader::open(&file)?;
    let mut read = reader.read();
    if let Some(limit) = rows {
        read = read.with_limit(limit);
    }
    if no_labels {
        read = read.value_labels_as_strings(false);
    }
    let _df = read.finish()?;
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    println!("elapsed_ms={elapsed:.3}");
    Ok(())
}
