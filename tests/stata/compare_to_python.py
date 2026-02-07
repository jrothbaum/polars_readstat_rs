# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Compare Rust Stata reader output against polars_readstat (ReadStat reference).

Run with:
  uv run tests/stata/compare_to_python.py --file tests/stata/data/usa_00009.dta --rows 10
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from polars_readstat import scan_readstat

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, required=True, help="Path to .dta file")
    parser.add_argument("--rows", type=int, default=100000, help="Rows to compare")
    parser.add_argument("--cols", type=int, default=0, help="Limit columns (0 = all)")
    return parser.parse_args()


def compare_file(stata_file: Path, n_rows: int, n_cols: int) -> tuple[int, int]:
    print(f"\n--- {stata_file.name} ({stata_file.stat().st_size / 1024 / 1024:.1f} MB) ---")

    # 1. Read with polars_readstat (ground truth)
    try:
        lf = scan_readstat(str(stata_file))
        if n_cols:
            names = list(lf.collect_schema().keys())[:n_cols]
            lf = lf.select(names)
        df_py = lf.head(n_rows).collect()
    except Exception as e:
        print(f"  FAIL: polars_readstat failed: {e}")
        return 0, 1

    print(f"  Python: {df_py.shape[0]} rows x {df_py.shape[1]} cols")

    # 2. Read with Rust reader
    result = subprocess.run(
        ["cargo", "run", "--release", "--example", "stata_dump_rows_json", "--",
         str(stata_file), str(n_rows), str(n_cols)],
        capture_output=True, text=True,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print(f"  FAIL: Rust reader failed: {result.stderr[:200]}")
        return 0, 1

    try:
        rust_data = json.loads(result.stdout)
    except json.JSONDecodeError:
        print("  FAIL: Rust output not valid JSON")
        return 0, 1

    rust_rows = rust_data["rows"]
    columns = rust_data["columns"]
    col_types = rust_data["col_types"]
    print(f"  Rust:   {len(rust_rows)} rows x {len(columns)} cols")

    mismatches = 0
    checked = 0

    for col_idx, col_name in enumerate(columns):
        col_type = col_types[col_idx]
        try:
            py_col = df_py[col_name]
        except Exception:
            print(f"  MISMATCH: column '{col_name}' not found in Python output")
            mismatches += 1
            continue

        for row_idx in range(min(n_rows, len(rust_rows), df_py.shape[0])):
            rust_val = rust_rows[row_idx][col_idx]
            py_val = py_col[row_idx]

            match = False
            if rust_val is None and py_val is None:
                match = True
            elif col_type == "character" and rust_val is None and py_val == '':
                match = True
            elif rust_val is None or py_val is None:
                match = False
            elif col_type == "numeric":
                try:
                    match = abs(float(rust_val) - float(py_val)) < 1e-6
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

    return checked, mismatches


def main() -> None:
    args = parse_args()
    print("=== Comparing Rust Stata reader vs polars_readstat ===")
    print(f"Checking first {args.rows} rows\n")

    print("Building Rust stata_dump_rows_json (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "stata_dump_rows_json"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)
    print("Build OK\n")

    checked, mismatches = compare_file(args.file, args.rows, args.cols)
    print(f"\nTOTAL: {checked} values checked, {mismatches} mismatches")
    if mismatches > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
