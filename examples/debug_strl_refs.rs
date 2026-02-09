use polars_readstat_rs::stata::reader::StataReader;
use polars_readstat_rs::stata::VarType;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: debug_strl_refs <file>");
        std::process::exit(1);
    }
    let path = &args[1];
    let reader = StataReader::open(path)?;
    let metadata = reader.metadata();

    let mut offset = 0usize;
    println!(
        "data_offset={:?} row_count={}",
        metadata.data_offset,
        metadata.row_count
    );
    println!("column offsets:");
    for (var, width) in metadata.variables.iter().zip(metadata.storage_widths.iter()) {
        println!("  {:<12} offset={} width={}", var.name, offset, width);
        offset += *width as usize;
    }
    println!("row_length={}", offset);

    let data = std::fs::read(path)?;
    let base = metadata.data_offset.unwrap_or(0) as usize;
    for row in 0..5usize {
        let row_off = base + row * offset;
        let mut out = Vec::new();
        for (var, width) in metadata.variables.iter().zip(metadata.storage_widths.iter()) {
            if matches!(var.var_type, VarType::StrL) {
                let w = *width as usize;
                let bytes = &data[row_off + out.len()..row_off + out.len() + w];
                println!(
                    "row {} {} bytes={}",
                    row,
                    var.name,
                    bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>()
                );
            }
            out.resize(out.len() + *width as usize, 0);
        }
    }

    Ok(())
}
