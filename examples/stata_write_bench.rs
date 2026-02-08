use polars_readstat_rs::{StataReader, StataResult, StataWriter};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn temp_path(prefix: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    path.push(format!("{prefix}_{pid}_{nanos}.dta"));
    path
}

fn main() -> StataResult<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: stata_write_bench <path> [limit] [n_threads]");
        std::process::exit(2);
    }
    let path = &args[1];
    let limit = if args.len() > 2 {
        args[2].parse::<usize>().unwrap_or(100_000)
    } else {
        100_000
    };
    let parallel_threads = if args.len() > 3 {
        args[3].parse::<usize>().unwrap_or(4)
    } else {
        4
    };

    let reader = StataReader::open(path)?;
    let df = reader.read().with_limit(limit).finish()?;

    let out_serial = temp_path("stata_write_serial");
    let out_parallel = temp_path("stata_write_parallel");

    let start = Instant::now();
    StataWriter::new(&out_serial)
        .with_n_threads(1)
        .write_df(&df)?;
    let serial_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    StataWriter::new(&out_parallel)
        .with_n_threads(parallel_threads)
        .write_df(&df)?;
    let parallel_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("serial_threads=1 parallel_threads={parallel_threads} serial_ms={serial_ms:.2} parallel_ms={parallel_ms:.2}");

    let _ = std::fs::remove_file(out_serial);
    let _ = std::fs::remove_file(out_parallel);
    Ok(())
}
