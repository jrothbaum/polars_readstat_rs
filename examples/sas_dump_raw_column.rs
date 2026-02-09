use polars_readstat_rs::sas::data::DataReader;
use polars_readstat_rs::sas::encoding;
use polars_readstat_rs::sas::page::PageReader;
use polars_readstat_rs::sas::types::ColumnType;
use polars_readstat_rs::Sas7bdatReader;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};

fn trim_sas_bytes(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\x00') {
        end -= 1;
    }
    &bytes[..end]
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Dump raw bytes for a SAS column at row range.
/// Usage: sas_dump_raw_column <file> <column> <start_row> <count>
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: sas_dump_raw_column <file> <column> <start_row> <count>");
        std::process::exit(1);
    }

    let file = &args[1];
    let col_name = &args[2];
    let start_row: usize = args[3].parse()?;
    let count: usize = args[4].parse()?;

    let reader = Sas7bdatReader::open(file)?;
    let metadata = reader.metadata().clone();
    let header = reader.header().clone();
    let endian = reader.endian();
    let format = reader.format();
    let initial_subheaders = reader.initial_data_subheaders().to_vec();

    let col = metadata
        .columns
        .iter()
        .find(|c| c.name == *col_name)
        .ok_or_else(|| format!("Column not found: {col_name}"))?;

    if col.col_type != ColumnType::Character {
        eprintln!("WARNING: column is not character");
    }

    let mut file_handle = BufReader::new(File::open(file)?);
    file_handle.seek(SeekFrom::Start(header.header_length as u64))?;
    let page_reader = PageReader::new(file_handle, header, endian, format);
    let mut data_reader = DataReader::new(
        page_reader,
        metadata.clone(),
        endian,
        format,
        initial_subheaders,
    )?;

    if start_row > 0 {
        data_reader.skip_rows(start_row)?;
    }

    let encoding = encoding::get_encoding(metadata.encoding_byte);
    println!(
        "encoding_byte={} encoding={}",
        metadata.encoding_byte,
        encoding.name()
    );
    println!(
        "column={} offset={} length={}",
        col.name, col.offset, col.length
    );

    for i in 0..count {
        let row_idx = start_row + i;
        let row_bytes = match data_reader.read_row()? {
            Some(b) => b,
            None => break,
        };
        let end = col.offset + col.length;
        if end > row_bytes.len() {
            println!("row {}: out of bounds (row_len={})", row_idx, row_bytes.len());
            continue;
        }
        let raw = &row_bytes[col.offset..end];
        let trimmed = trim_sas_bytes(raw);
        let decoded = encoding::decode_string(trimmed, metadata.encoding_byte, encoding);
        println!(
            "row {}: raw_hex={} trimmed_hex={} decoded='{}'",
            row_idx,
            hex(raw),
            hex(trimmed),
            decoded
        );
    }

    Ok(())
}
