# polars_readstat_rs

Rust library for reading SAS (`.sas7bdat`), Stata (`.dta`), and SPSS (`.sav`/`.zsav`) files with Polars.

The crate provides:
- format-specific readers (`Sas7bdatReader`, `StataReader`, `SpssReader`)
- a format-agnostic scan API (`readstat_scan`)
- metadata/schema helpers
- Arrow FFI export helpers
- Stata/SPSS writers

## Install
```toml
[dependencies]
polars_readstat_rs = "0.1"
```

## Core API

### 1) Read directly to a `DataFrame`
```rust
use polars_readstat_rs::Sas7bdatReader;

let df = Sas7bdatReader::open("file.sas7bdat")?
    .read()
    .finish()?;
```

### 2) Read with projection/offset/limit
```rust
use polars_readstat_rs::StataReader;

let df = StataReader::open("file.dta")?
    .read()
    .with_columns(vec!["serial".to_string(), "age".to_string()])
    .with_offset(1_000)
    .with_limit(5_000)
    .finish()?;
```

### 3) Format-agnostic lazy scan
```rust
use polars::prelude::*;
use polars_readstat_rs::{readstat_scan, ScanOptions};

let opts = ScanOptions {
    threads: Some(4),
    chunk_size: Some(100_000),
    ..Default::default()
};

let lf = readstat_scan("file.sav", Some(opts), None)?;
let out = lf.select([col("id"), col("income")]).collect()?;
```

### 4) Metadata and schema
```rust
use polars_readstat_rs::{readstat_metadata_json, readstat_schema};

let metadata_json = readstat_metadata_json("file.dta", None)?;
let schema = readstat_schema("file.dta", None, None)?;
```

### 5) Writing (Stata/SPSS)
```rust
use polars_readstat_rs::{StataWriter, SpssWriter};

StataWriter::new("out.dta").write_df(&df)?;
SpssWriter::new("out.sav").write_df(&df)?;
```

### 6) SPSS writer with schema and labels
```rust
use polars::prelude::*;
use polars_readstat_rs::{
    SpssValueLabelKey,
    SpssValueLabelMap,
    SpssValueLabels,
    SpssVariableLabels,
    SpssWriteColumn,
    SpssWriteSchema,
    SpssWriter,
};
use std::collections::HashMap;

let df = DataFrame::new(vec![
    Series::new("status".into(), &[1i32, 2, 3]).into_column(),
    Series::new("name".into(), &["alice", "bob", "carol"]).into_column(),
])?;

let schema = SpssWriteSchema {
    columns: vec![
        SpssWriteColumn {
            name: "status".to_string(),
            dtype: DataType::Int32,
            string_width_bytes: None,
        },
        SpssWriteColumn {
            name: "name".to_string(),
            dtype: DataType::String,
            string_width_bytes: Some(16),
        },
    ],
    row_count: Some(df.height()),
    value_labels: None,
    variable_labels: None,
};

let mut status_map: SpssValueLabelMap = HashMap::new();
status_map.insert(SpssValueLabelKey::from_f64(1.0), "one".to_string());
status_map.insert(SpssValueLabelKey::from_f64(2.0), "two".to_string());
status_map.insert(SpssValueLabelKey::from_f64(3.0), "three".to_string());
let value_labels: SpssValueLabels = HashMap::from([("status".to_string(), status_map)]);
let variable_labels: SpssVariableLabels = HashMap::from([
    ("status".to_string(), "Status Label".to_string()),
    ("name".to_string(), "Display Name".to_string()),
]);

SpssWriter::new("out.sav")
    .with_schema(schema)
    .with_value_labels(value_labels)
    .with_variable_labels(variable_labels)
    .write_df(&df)?;
```

SPSS writer behavior and current limits:
- Variable names are validated as non-empty and `<= 64` bytes.
- SPSS short names are generated automatically (ASCII, uppercase, unique, max 8 chars) when needed.
- Strings are fixed-width in bytes and limited to `<= 255` bytes per value.
- Numeric output supports integer/float/bool/date/datetime/time columns (written as SPSS numeric values).
- Value labels are currently supported for numeric variables only.
- String value labels are not currently supported.
- Output encoding is selected automatically: Windows-1252 when possible, otherwise UTF-8 with an SPSS encoding record.

## Arrow export
```rust
use polars_readstat_rs::sas_arrow_output;

let mut schema = sas_arrow_output::read_to_arrow_schema_ffi("file.sas7bdat")?;
let mut stream = sas_arrow_output::read_to_arrow_stream_ffi("file.sas7bdat", Some(100_000))?;
```

See `ARROW_EXPORT.md` for FFI details.

## Basic validation and benchmarks

Compare against Python reference outputs:
```bash
uv run tests/sas/compare_to_python.py
uv run tests/stata/compare_to_python.py --file tests/stata/data/too_big/usa_00009.dta --rows 100000
uv run tests/spss/compare_to_python.py --rows 100000
```

Read performance checks:
```bash
uv run tests/sas/bench_vs_python.py --file tests/sas/data/too_big/psam_p17.sas7bdat --rows 100000 --repeat 2
uv run tests/stata/bench_vs_python.py --file tests/stata/data/too_big/usa_00009.dta --rows 100000 --repeat 2
uv run tests/spss/bench_vs_python.py --file tests/spss/data/too_big/ess_data.sav --rows 100000 --repeat 2
```
