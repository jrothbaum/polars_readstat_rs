use std::path::PathBuf;
use stata_reader::stata::metadata_json;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: stata_metadata <file>");
        std::process::exit(1);
    }
    let path = PathBuf::from(&args[1]);
    let json = metadata_json(path).expect("metadata");
    println!("{}", json);
}
