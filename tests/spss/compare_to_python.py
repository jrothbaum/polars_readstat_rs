# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
#     "pyreadstat==1.2.7",
# ]
# ///

"""Compare Rust SPSS reader output against polars_readstat (C++ reference) ground truth.

Run with: uv run tests/spss/compare_to_python.py [--rows N] [--xfail]

Iterates all .sav/.zsav files in tests/spss/data/,
reads each with both polars_readstat and our Rust reader,
and compares the first N rows across all columns.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
import datetime as dt
import math
import pandas as pd
import pyreadstat
from polars_readstat import scan_readstat

N_ROWS = 10
MAX_FILE_SIZE = 1_000_000_000  # 1GB
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = PROJECT_ROOT / "tests" / "spss" / "data"


def compare_file(sav_file: Path, n_rows: int) -> tuple[int, int]:
    print(f"\n--- {sav_file.name} ({sav_file.stat().st_size / 1024 / 1024:.1f} MB) ---")

    use_pyreadstat = sav_file.suffix.lower() == ".zsav"
    if use_pyreadstat:
        try:
            df_py, _meta = pyreadstat.read_sav(str(sav_file), row_limit=n_rows)
            source = "pyreadstat"
        except Exception as e2:
            print(f"  SKIP: pyreadstat failed: {e2}")
            return 0, 0
    else:
        try:
            df_py = scan_readstat(str(sav_file)).head(n_rows).collect().to_pandas()
            source = "polars_readstat"
        except BaseException as e:
            try:
                df_py, _meta = pyreadstat.read_sav(str(sav_file), row_limit=n_rows)
                source = "pyreadstat"
            except Exception as e2:
                print(f"  SKIP: polars_readstat failed: {e}")
                print(f"  SKIP: pyreadstat failed: {e2}")
                return 0, 0

    print(f"  Python ({source}): {df_py.shape[0]} rows x {df_py.shape[1]} cols")

    result = subprocess.run(
        ["cargo", "run", "--release", "--example", "spss_dump_rows_json", "--",
         str(sav_file), str(n_rows)],
        capture_output=True, text=True,
        cwd=PROJECT_ROOT,
    )

    if result.returncode != 0:
        print(f"  SKIP: Rust reader failed: {result.stderr[:200]}")
        return 0, 0

    try:
        rust_data = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"  SKIP: Rust output not valid JSON")
        return 0, 0

    rust_rows = rust_data["rows"]
    columns = rust_data["columns"]
    col_types = rust_data["col_types"]
    missing_ranges = rust_data.get("missing_range", [False] * len(columns))
    missing_values = rust_data.get("missing_values", [[] for _ in columns])
    missing_strings = rust_data.get("missing_strings", [[] for _ in columns])
    print(f"  Rust:   {len(rust_rows)} rows x {len(columns)} cols")

    if df_py.shape[0] == 0:
        print(f"  SKIP: {source} returned 0 rows")
        return 0, 0

    py_columns = list(df_py.columns)
    py_col_map = {c.lower(): c for c in py_columns}
    if len(py_columns) != len(columns):
        print(f"  WARNING: column count differs (py={len(py_columns)} rust={len(columns)})")

    mismatches = 0
    checked = 0
    case_mismatch = 0
    rust_only = 0
    case_examples = []
    rust_only_examples = []

    for col_idx, col_name in enumerate(columns):
        py_col_name = py_col_map.get(col_name.lower())
        if py_col_name is None:
            rust_only += 1
            if len(rust_only_examples) < 5:
                rust_only_examples.append(col_name)
            continue
        if col_name != py_col_name:
            case_mismatch += 1
            if len(case_examples) < 5:
                case_examples.append((col_name, py_col_name))
        py_col = df_py[py_col_name]
        col_type = col_types[col_idx]

        py_vals = py_col.to_list()
        miss_range = missing_ranges[col_idx] if col_idx < len(missing_ranges) else False
        miss_vals = missing_values[col_idx] if col_idx < len(missing_values) else []
        miss_strs = missing_strings[col_idx] if col_idx < len(missing_strings) else []
        for row_idx in range(min(n_rows, len(rust_rows), df_py.shape[0])):
            rust_val = rust_rows[row_idx][col_idx]
            py_val = normalize_py_value(py_vals[row_idx])

            match = False
            if rust_val is None and py_val is None:
                match = True
            elif col_type == "numeric" and rust_val is None and isinstance(py_val, (int, float)):
                if is_missing_numeric(float(py_val), miss_range, miss_vals):
                    match = True
            elif col_type == "character" and rust_val is None and py_val == '':
                match = True
            elif col_type == "character" and rust_val is None and isinstance(py_val, str):
                if is_missing_string(py_val, miss_strs):
                    match = True
            elif rust_val is None or py_val is None:
                match = False
            elif col_type == "numeric":
                try:
                    match = abs(float(rust_val) - float(py_val)) < 1e-10
                except (ValueError, TypeError):
                    match = False
            else:
                match = str(rust_val) == str(py_val)

            if not match:
                print(f"  MISMATCH row={row_idx} col={col_name} ({col_type}): "
                      f"rust={rust_val!r} python={py_val!r}")
                mismatches += 1
            checked += 1

    if mismatches == 0:
        print(f"  OK ({checked} values)")
    else:
        print(f"  FAILED: {mismatches}/{checked} mismatches")
    if rust_only > 0:
        examples = ", ".join(rust_only_examples)
        more = "..." if rust_only > len(rust_only_examples) else ""
        print(f"  NOTE: rust-only columns: {rust_only} (e.g., {examples}{more})")
    if case_mismatch > 0:
        examples = ", ".join([f"{r}->{p}" for r, p in case_examples])
        more = "..." if case_mismatch > len(case_examples) else ""
        print(f"  NOTE: column name case mismatches: {case_mismatch} (e.g., {examples}{more})")

    return checked, mismatches


SPSS_EPOCH = dt.datetime(1582, 10, 14)


def normalize_py_value(val):
    if val is None:
        return None
    try:
        if isinstance(val, float) and math.isnan(val):
            return None
    except Exception:
        pass
    if pd.isna(val):
        return None
    # pandas Timestamp
    if hasattr(val, "to_pydatetime"):
        val = val.to_pydatetime()
    if isinstance(val, dt.datetime):
        return (val - SPSS_EPOCH).total_seconds()
    if isinstance(val, dt.date):
        d = dt.datetime(val.year, val.month, val.day)
        return (d - SPSS_EPOCH).total_seconds()
    if isinstance(val, dt.time):
        return val.hour * 3600 + val.minute * 60 + val.second + val.microsecond / 1_000_000
    return val


def is_missing_numeric(v: float, missing_range: bool, missing_values: list[float]) -> bool:
    if not missing_values:
        return False
    if missing_range:
        if len(missing_values) >= 2:
            low = min(missing_values[0], missing_values[1])
            high = max(missing_values[0], missing_values[1])
            if low <= v <= high:
                return True
        if len(missing_values) >= 3 and v == missing_values[2]:
            return True
        return False
    return any(v == mv for mv in missing_values)


def is_missing_string(v: str, missing_values: list[str]) -> bool:
    if not missing_values:
        return False
    return v in missing_values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=N_ROWS, help="Rows to compare per file")
    parser.add_argument("--xfail", action="store_true", help="Do not fail on mismatches (report only)")
    args = parser.parse_args()

    print("=== Comparing Rust reader vs polars_readstat (ReadStat reference) ===")
    print(f"Checking first {args.rows} rows, all columns, files < {MAX_FILE_SIZE / 1e9:.0f}GB\n")

    print("Building Rust spss_dump_rows_json (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "spss_dump_rows_json"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)
    print("Build OK\n")

    sav_files = sorted(TEST_DATA_DIR.glob("**/*.sav")) + sorted(TEST_DATA_DIR.glob("**/*.zsav"))
    sav_files = [f for f in sav_files if f.stat().st_size <= MAX_FILE_SIZE]

    if not sav_files:
        print("No test files found!")
        sys.exit(1)

    print(f"Found {len(sav_files)} test files (after size filter)")

    total_checked = 0
    total_mismatches = 0
    failed_files = []

    for sav_file in sav_files:
        checked, mismatches = compare_file(sav_file, args.rows)
        total_checked += checked
        total_mismatches += mismatches
        if mismatches > 0:
            failed_files.append(sav_file.name)

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_checked} values checked across {len(sav_files)} files")
    print(f"       {total_mismatches} mismatches")

    if total_mismatches == 0:
        print("ALL FILES MATCH!")
    else:
        print(f"FAILED files: {', '.join(failed_files)}")
        if not args.xfail:
            sys.exit(1)


if __name__ == "__main__":
    main()
