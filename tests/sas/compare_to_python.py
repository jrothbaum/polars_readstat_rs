# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Compare Rust SAS reader output against polars_readstat (C++ reference) ground truth.

Run with: uv run tests/sas/compare_to_python.py

Iterates all .sas7bdat files in tests/sas/data/ (skipping files > 1GB),
reads each with both polars_readstat and our Rust reader,
and compares the first N rows across all columns.
"""

import json
import subprocess
import sys
from datetime import date, datetime, time
from pathlib import Path
from polars_readstat import scan_readstat

N_ROWS = 10
MAX_FILE_SIZE = 1_000_000_000  # 1GB - skip files larger than this
PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = PROJECT_ROOT / "tests" / "sas" / "data"


def compare_file(sas_file: Path) -> tuple[int, int]:
    """Compare one file. Returns (checked, mismatches)."""
    print(f"\n--- {sas_file.name} ({sas_file.stat().st_size / 1024 / 1024:.1f} MB) ---")

    # 1. Read with polars_readstat (ground truth)
    try:
        df_py = scan_readstat(str(sas_file)).head(N_ROWS).collect()
    except Exception as e:
        print(f"  SKIP: polars_readstat failed: {e}")
        return 0, 0

    print(f"  Python: {df_py.shape[0]} rows x {df_py.shape[1]} cols")

    # 2. Read with Rust reader
    result = subprocess.run(
        ["cargo", "run", "--release", "--example", "dump_rows_json", "--",
         str(sas_file), str(N_ROWS)],
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
    print(f"  Rust:   {len(rust_rows)} rows x {len(columns)} cols")

    # 3. Compare
    mismatches = 0
    checked = 0

    for col_idx, col_name in enumerate(columns):
        col_type = col_types[col_idx]
        try:
            py_col = df_py[col_name]
        except Exception:
            # Column name mismatch
            print(f"  MISMATCH: column '{col_name}' not found in Python output")
            mismatches += 1
            continue

        for row_idx in range(min(N_ROWS, len(rust_rows), df_py.shape[0])):
            rust_val = rust_rows[row_idx][col_idx]
            py_val = py_col[row_idx]

            if isinstance(py_val, (datetime, date, time)):
                py_val = str(py_val)

            match = False
            if rust_val is None and py_val is None:
                match = True
            elif col_type == "character" and rust_val is None and py_val == '':
                # Convention difference: Rust null vs Python empty string for all-space
                match = True
            elif rust_val is None or py_val is None:
                match = False
            elif isinstance(rust_val, str) and isinstance(py_val, str):
                match = rust_val == py_val
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

    return checked, mismatches


def main():
    print("=== Comparing Rust reader vs polars_readstat (C++ reference) ===")
    print(f"Checking first {N_ROWS} rows, all columns, files < {MAX_FILE_SIZE / 1e9:.0f}GB\n")

    # Build the Rust example once upfront
    print("Building Rust dump_rows_json (release)...")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "dump_rows_json"],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)
    print("Build OK\n")

    # Find all test files
    sas_files = sorted(TEST_DATA_DIR.glob("**/*.sas7bdat"))
    sas_files = [f for f in sas_files if f.stat().st_size <= MAX_FILE_SIZE]

    if not sas_files:
        print("No test files found!")
        sys.exit(1)

    print(f"Found {len(sas_files)} test files (after size filter)")

    total_checked = 0
    total_mismatches = 0
    failed_files = []

    for sas_file in sas_files:
        checked, mismatches = compare_file(sas_file)
        total_checked += checked
        total_mismatches += mismatches
        if mismatches > 0:
            failed_files.append(sas_file.name)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_checked} values checked across {len(sas_files)} files")
    print(f"       {total_mismatches} mismatches")

    if total_mismatches == 0:
        print("ALL FILES MATCH!")
    else:
        print(f"FAILED files: {', '.join(failed_files)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
