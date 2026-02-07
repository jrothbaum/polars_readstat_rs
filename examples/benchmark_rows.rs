use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use stata_reader::Sas7bdatReader;

/// Usage:
///   benchmark_rows <file> <n_rows|full> [--columns a,b,c] [--col-count N] [--pipeline] [--chunk-size N] [--profile]
///   benchmark_rows --list <filelist> <n_rows|full> [--columns a,b,c] [--col-count N] [--pipeline] [--chunk-size N] [--profile]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);

    let mut list_file: Option<PathBuf> = None;
    let mut columns: Option<Vec<String>> = None;
    let mut col_count: Option<usize> = None;
    let mut pipeline = false;
    let mut chunk_size: Option<usize> = None;
    let mut profile = false;
    let mut positional: Vec<String> = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--list" => {
                let v = args.next().ok_or("missing --list value")?;
                list_file = Some(PathBuf::from(v));
            }
            "--columns" => {
                let list = args.next().ok_or("missing --columns value")?;
                let cols = list
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>();
                columns = Some(cols);
            }
            "--col-count" => {
                let v = args.next().ok_or("missing --col-count value")?;
                col_count = Some(v.parse()?);
            }
            "--pipeline" => pipeline = true,
            "--chunk-size" => {
                let v = args.next().ok_or("missing --chunk-size value")?;
                chunk_size = Some(v.parse()?);
            }
            "--profile" => profile = true,
            _ => positional.push(arg),
        }
    }

    let rows = if list_file.is_some() {
        if positional.len() != 1 {
            return Err("expected <n_rows|full> with --list".into());
        }
        positional.remove(0)
    } else {
        if positional.len() != 2 {
            return Err("expected <file> <n_rows|full>".into());
        }
        positional.remove(1)
    };

    if let Some(list) = list_file {
        if profile {
            let first = first_file_from_list(&list)?;
            run_profile(&first, &rows, columns, col_count, pipeline, chunk_size)?;
            return Ok(());
        }
        let start = Instant::now();
        for path in files_from_list(&list)? {
            run_read(&path, &rows, columns.clone(), col_count, pipeline, chunk_size)?;
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!("elapsed_ms={:.3}", elapsed_ms);
        return Ok(());
    }

    let file = PathBuf::from(positional.remove(0));
    if profile {
        run_profile(&file, &rows, columns, col_count, pipeline, chunk_size)?;
        return Ok(());
    }
    let start = Instant::now();
    run_read(&file, &rows, columns, col_count, pipeline, chunk_size)?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!("elapsed_ms={:.3}", elapsed_ms);
    Ok(())
}

fn run_read(
    path: &PathBuf,
    rows: &str,
    columns: Option<Vec<String>>,
    col_count: Option<usize>,
    pipeline: bool,
    chunk_size: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = Sas7bdatReader::open(path)?;
    let mut builder = reader.read();
    if pipeline {
        builder = builder.pipeline();
        if let Some(size) = chunk_size {
            builder = builder.pipeline_chunk_size(size);
        }
    }

    if let Some(cols) = columns {
        builder = builder.with_columns(cols);
    } else if let Some(n) = col_count {
        let names: Vec<String> = reader
            .metadata()
            .columns
            .iter()
            .take(n)
            .map(|c| c.name.clone())
            .collect();
        builder = builder.with_columns(names);
    }

    if rows != "full" {
        builder = builder.with_limit(rows.parse()?);
    }

    let _df = builder.finish()?;
    Ok(())
}

fn run_profile(
    path: &PathBuf,
    rows: &str,
    columns: Option<Vec<String>>,
    col_count: Option<usize>,
    pipeline: bool,
    chunk_size: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = Sas7bdatReader::open(path)?;
    let mut builder = reader.read();
    if pipeline {
        builder = builder.pipeline();
        if let Some(size) = chunk_size {
            builder = builder.pipeline_chunk_size(size);
        }
    }

    if let Some(cols) = columns {
        builder = builder.with_columns(cols);
    } else if let Some(n) = col_count {
        let names: Vec<String> = reader
            .metadata()
            .columns
            .iter()
            .take(n)
            .map(|c| c.name.clone())
            .collect();
        builder = builder.with_columns(names);
    }

    if rows != "full" {
        builder = builder.with_limit(rows.parse()?);
    }

    let (_df, prof) = builder.finish_profiled()?;
    println!(
        "parse_rows_ms={:.3} parse_values_ms={:.3} add_row_ms={:.3} build_df_ms={:.3} total_ms={:.3}",
        prof.parse_rows_ms, prof.parse_values_ms, prof.add_row_ms, prof.build_df_ms, prof.total_ms
    );
    Ok(())
}

fn files_from_list(list: &PathBuf) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let file = File::open(list)?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(PathBuf::from(trimmed));
    }
    Ok(out)
}

fn first_file_from_list(list: &PathBuf) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let files = files_from_list(list)?;
    files
        .into_iter()
        .next()
        .ok_or_else(|| "empty --list file".into())
}
