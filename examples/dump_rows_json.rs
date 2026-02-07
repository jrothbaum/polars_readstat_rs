use stata_reader::Sas7bdatReader;

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Dumps first N rows of a SAS file as JSON for comparison testing.
/// Usage: dump_rows_json <file> <n_rows>
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: dump_rows_json <file> <n_rows>");
        std::process::exit(1);
    }

    let file = &args[1];
    let n_rows: usize = args[2].parse()?;

    let reader = Sas7bdatReader::open(file)?;
    let metadata = reader.metadata();

    let col_names: Vec<&str> = metadata.columns.iter().map(|c| c.name.as_str()).collect();

    let sample = reader.read().with_limit(1).finish()?;
    let col_types: Vec<&str> = sample
        .get_columns()
        .iter()
        .map(|c| match c.dtype() {
            polars::prelude::DataType::String => "character",
            _ => "numeric",
        })
        .collect();

    let df = reader.read().with_limit(n_rows).finish()?;

    print!("{{\"columns\":[");
    for (i, name) in col_names.iter().enumerate() {
        if i > 0 { print!(","); }
        print!("\"{}\"", name);
    }
    print!("],\"col_types\":[");
    for (i, ct) in col_types.iter().enumerate() {
        if i > 0 { print!(","); }
        print!("\"{}\"", ct);
    }
    print!("],\"rows\":[");

    for row_idx in 0..df.height() {
        if row_idx > 0 { print!(","); }
        print!("[");
        for (col_idx, col) in df.get_columns().iter().enumerate() {
            if col_idx > 0 { print!(","); }
            let series = col.as_materialized_series();
            let val = series.get(row_idx).unwrap();
            match val {
                polars::prelude::AnyValue::Null => print!("null"),
                polars::prelude::AnyValue::Float64(v) => print!("{}", v),
                polars::prelude::AnyValue::Float32(v) => print!("{}", v),
                polars::prelude::AnyValue::Int8(v) => print!("{}", v),
                polars::prelude::AnyValue::Int16(v) => print!("{}", v),
                polars::prelude::AnyValue::Int32(v) => print!("{}", v),
                polars::prelude::AnyValue::Int64(v) => print!("{}", v),
                polars::prelude::AnyValue::String(s) => print!("\"{}\"", json_escape(s)),
                other => print!("\"{}\"", json_escape(&other.to_string())),
            }
        }
        print!("]");
    }
    println!("]}}");

    Ok(())
}
