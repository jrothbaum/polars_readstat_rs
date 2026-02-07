use std::path::PathBuf;
use stata_reader::stata::arrow_output::{
    read_to_arrow_array_ffi, read_to_arrow_schema_ffi, read_to_arrow_stream_ffi,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: stata_arrow <file> [mode] [threads] [chunk_size] [missing_string_as_null] [value_labels_as_strings] [batch_size]");
        std::process::exit(1);
    }

    let path = PathBuf::from(&args[1]);
    let mode = args.get(2).map(|s| s.as_str()).unwrap_or("array");
    let threads = args.get(3).and_then(|s| s.parse::<usize>().ok());
    let chunk_size = args.get(4).and_then(|s| s.parse::<usize>().ok());
    let missing_string_as_null = args
        .get(5)
        .map(|s| s == "true" || s == "1")
        .unwrap_or(true);
    let value_labels_as_strings = args
        .get(6)
        .map(|s| s == "true" || s == "1")
        .unwrap_or(true);
    let batch_size = args.get(7).and_then(|s| s.parse::<usize>().ok()).unwrap_or(10000);

    match mode {
        "schema" => {
            let schema = read_to_arrow_schema_ffi(
                &path,
                threads,
                missing_string_as_null,
                Some(value_labels_as_strings),
                chunk_size,
            )
            .expect("arrow schema");
            unsafe { drop(Box::from_raw(schema)); }
            println!("ok");
        }
        "stream" => {
            let stream = read_to_arrow_stream_ffi(
                &path,
                threads,
                missing_string_as_null,
                Some(value_labels_as_strings),
                chunk_size,
                batch_size,
            )
            .expect("arrow stream");
            unsafe { drop(Box::from_raw(stream)); }
            println!("ok");
        }
        _ => {
            let (schema, array) = read_to_arrow_array_ffi(
                &path,
                threads,
                missing_string_as_null,
                Some(value_labels_as_strings),
                chunk_size,
            )
            .expect("arrow array");
            unsafe {
                drop(Box::from_raw(schema));
                drop(Box::from_raw(array));
            }
            println!("ok");
        }
    }
}
