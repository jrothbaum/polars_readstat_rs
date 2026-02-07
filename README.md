# polars_readstat_rs

Pure Rust readers for SAS, Stata, and SPSS files with Polars and Arrow outputs.

This crate focuses on fast, batch-oriented reading and integrates with Polars `LazyFrame` scans, streaming batches via AnonymousScan, and Arrow FFI export.

This is a mostly vibe-coded repository (Claude Code and OpenAI Codex) based on [cpp-sas7bdat](https://github.com/olivia76/cpp-sas7bdat/tree/main/src) and [ReadStat](https://github.com/WizardMac/ReadStat).

## Features
- SAS `SAS7BDAT`, Stata `.dta`, SPSS `.sav/.zsav`
- Polars `LazyFrame` scan with batch streaming
- Arrow schema/array/stream export
- Configurable `chunk_size` and `threads`
- Value-label handling and missing-string behavior

## Install

From crates.io:

```toml
[dependencies]
polars_readstat_rs = "0.1"
```

From a local checkout:

```toml
[dependencies]
polars_readstat_rs = { path = "." }
```

## Related Project
The Python bindings live here (or will when I finish): `https://github.com/jrothbaum/polars_readstat`.
This Rust crate is intended to replace `readstat` and `cpp-sas7bdat` in that project so the stack is fully Rust + Python.


## Quick Start

### Read a full file into a DataFrame

```rust
use polars_readstat_rs::Sas7bdatReader;

let df = Sas7bdatReader::open("file.sas7bdat")?
    .read()
    .finish()?;
```

### Read rows and columns by name

```rust
use polars_readstat_rs::StataReader;

let df = StataReader::open("file.dta")?
    .read()
    .with_offset(1000)
    .with_limit(5000)
    .with_columns(vec!["serial".to_string(), "age".to_string()])
    .finish()?;
```

## Format-Agnostic Scan API

The scan API dispatches based on file extension.

```rust
use polars::prelude::*;
use polars_readstat_rs::{readstat_scan, ScanOptions};

let opts = ScanOptions {
    threads: Some(4),
    chunk_size: Some(100_000),
    missing_string_as_null: Some(true),
    value_labels_as_strings: Some(true),
};

let lf = readstat_scan("file.sas7bdat", Some(opts), None)?;
let df = lf
    .select([col("serial"), col("age")])
    .slice(1000, 5000)
    .collect()?;
```

## Arrow FFI Export

Each format exposes Arrow schema, array, and stream outputs.

```rust
use polars_readstat_rs::sas_arrow_output;

let mut schema = sas_arrow_output::read_to_arrow_schema_ffi("file.sas7bdat")?;
let mut stream = sas_arrow_output::read_to_arrow_stream_ffi("file.sas7bdat", Some(100_000))?;
```

See `ARROW_EXPORT.md` for details.

## Examples

Run examples under `examples/`:

```bash
cargo run --release --example sas_scan -- tests/sas/data/psam_p17.sas7bdat 1000
cargo run --release --example sas_arrow -- tests/sas/data/psam_p17.sas7bdat stream
cargo run --release --example readstat_scan_options -- tests/stata/data/usa_00009.dta --limit 100000
```

## Tests and Benchmarks

Validation scripts compare Rust output to `polars_readstat` (C++ ReadStat):

```bash
uv run tests/sas/compare_to_python.py
uv run tests/stata/compare_to_python.py --file tests/stata/data/usa_00009.dta --rows 100000
uv run tests/spss/compare_to_python.py --rows 100000
```

Benchmarks:

```bash
uv run tests/sas/bench_vs_python.py --file tests/sas/data/psam_p17.sas7bdat --rows 100000 --repeat 2
uv run tests/stata/bench_vs_python.py --file tests/stata/data/usa_00009.dta --rows 100000 --repeat 2
uv run tests/spss/bench_vs_python.py --file tests/spss/data/ess_data.sav --rows 100000 --repeat 2 --readstat-default
```

## Notes
- `readstat_scan` uses AnonymousScan and streams batches as they are produced.
- `chunk_size` controls batch size for scan and Arrow stream output.
- `threads` defaults to `min(4, num_cores)` if not set.

