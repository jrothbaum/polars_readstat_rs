use polars_readstat_rs::SpssReader;
use polars_readstat_rs::spss::VarType;

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

/// Dumps first N rows of an SPSS file as JSON for comparison testing.
/// Usage: spss_dump_rows_json <file> <n_rows> [n_cols]
/// If n_cols is 0 or omitted, all columns are included.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: spss_dump_rows_json <file> <n_rows> [n_cols]");
        std::process::exit(1);
    }

    let file = &args[1];
    let n_rows: usize = args[2].parse()?;
    let n_cols: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);

    let reader = SpssReader::open(file)?;
    let metadata = reader.metadata();

    let cols_to_read = if n_cols == 0 {
        metadata.variables.len()
    } else {
        n_cols.min(metadata.variables.len())
    };

    let col_names: Vec<&str> = metadata.variables.iter().take(cols_to_read).map(|c| c.name.as_str()).collect();
    let mut missing_ranges = Vec::with_capacity(cols_to_read);
    let mut missing_values: Vec<Vec<f64>> = Vec::with_capacity(cols_to_read);
    let mut missing_strings: Vec<Vec<String>> = Vec::with_capacity(cols_to_read);
    for var in metadata.variables.iter().take(cols_to_read) {
        if var.var_type == VarType::Numeric {
            missing_ranges.push(var.missing_range);
            missing_values.push(var.missing_doubles.clone());
            missing_strings.push(Vec::new());
        } else {
            missing_ranges.push(false);
            missing_values.push(Vec::new());
            missing_strings.push(var.missing_strings.clone());
        }
    }

    let sample = reader.read()
        .with_limit(1)
        .value_labels_as_strings(false)
        .finish()?;
    let col_types: Vec<&str> = sample
        .get_columns()
        .iter()
        .take(cols_to_read)
        .map(|c| match c.dtype() {
            polars::prelude::DataType::String => "character",
            _ => "numeric",
        })
        .collect();

    let selected: Vec<String> = col_names.iter().map(|s| s.to_string()).collect();
    let df = reader.read()
        .with_columns(selected)
        .with_limit(n_rows)
        .value_labels_as_strings(false)
        .finish()?;

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
    print!("],\"missing_range\":[");
    for (i, v) in missing_ranges.iter().enumerate() {
        if i > 0 { print!(","); }
        print!("{}", if *v { "true" } else { "false" });
    }
    print!("],\"missing_values\":[");
    for (i, vals) in missing_values.iter().enumerate() {
        if i > 0 { print!(","); }
        print!("[");
        for (j, v) in vals.iter().enumerate() {
            if j > 0 { print!(","); }
            print!("{}", v);
        }
        print!("]");
    }
    print!("],\"missing_strings\":[");
    for (i, vals) in missing_strings.iter().enumerate() {
        if i > 0 { print!(","); }
        print!("[");
        for (j, v) in vals.iter().enumerate() {
            if j > 0 { print!(","); }
            print!("\"{}\"", json_escape(v));
        }
        print!("]");
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
                polars::prelude::AnyValue::String(s) => {
                    print!("\"{}\"", json_escape(s));
                }
                other => print!("\"{}\"", json_escape(&other.to_string())),
            }
        }
        print!("]");
    }
    println!("]}}");

    Ok(())
}
