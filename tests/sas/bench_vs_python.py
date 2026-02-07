# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "polars_readstat==0.11.1",
# ]
# ///

"""Benchmark Rust SAS reader vs polars_readstat (C++ reference).

Run with:
  uv run tests/sas/bench_vs_python.py --file tests/sas/data/data_pandas/test16.sas7bdat --rows 1000 --repeat 5

Notes:
  - This measures end-to-end wall time for each reader.
  - Rust timing is measured inside the Rust example and reported back (excludes process startup).
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
import time
from pathlib import Path

from polars_readstat import scan_readstat

MAX_FILE_SIZE = 1_000_000_000  # 1GB


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, help="Path to .sas7bdat file")
    parser.add_argument("--all", action="store_true", help="Benchmark all .sas7bdat files under tests/sas/data/")
    parser.add_argument("--rows", type=int, default=1000, help="Rows to read")
    parser.add_argument("--full", action="store_true", help="Read full files (ignore --rows)")
    parser.add_argument("--repeat", type=int, default=2, help="Benchmark repetitions")
    parser.add_argument(
        "--columns",
        type=str,
        help="Comma-separated list of column names to read (applies to both readers)",
    )
    parser.add_argument("--first-k", type=int, help="Read first K columns (requires single --file)")
    parser.add_argument("--last-k", type=int, help="Read last K columns (requires single --file)")
    parser.add_argument(
        "--col-count",
        type=int,
        help="Read only the first N columns (applies to both readers)",
    )
    parser.add_argument(
        "--isolate-python",
        action="store_true",
        help="Run python benchmark in a subprocess per repetition to avoid memory growth",
    )
    parser.add_argument(
        "--rust-pipeline",
        action="store_true",
        help="Use Rust pipeline reader (optional --chunk-size)",
    )
    parser.add_argument("--chunk-size", type=int, help="Pipeline chunk size for Rust")
    parser.add_argument(
        "--rust-profile",
        action="store_true",
        help="Run a single Rust profiling pass and exit",
    )
    parser.add_argument("--child-python", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def _parse_columns(args: argparse.Namespace) -> list[str] | None:
    if args.columns:
        cols = [c.strip() for c in args.columns.split(",") if c.strip()]
        if cols:
            return cols
    return None


def _resolve_first_n_columns(path: Path, n: int) -> list[str]:
    lf = scan_readstat(str(path))
    try:
        schema = lf.collect_schema()
        return list(schema.keys())[:n]
    except Exception as exc:
        raise RuntimeError(f"Failed to resolve columns for {path}") from exc


def _resolve_last_n_columns(path: Path, n: int) -> list[str]:
    lf = scan_readstat(str(path))
    try:
        schema = lf.collect_schema()
        names = list(schema.keys())
        return names[-n:] if n <= len(names) else names
    except Exception as exc:
        raise RuntimeError(f"Failed to resolve columns for {path}") from exc
def bench_python(path: Path, rows: int, full: bool, columns: list[str] | None) -> float:
    start = time.perf_counter()
    lf = scan_readstat(str(path))
    if columns:
        lf = lf.select(columns)
    if full:
        df = lf.collect()
    else:
        df = lf.head(rows).collect()
    del df
    end = time.perf_counter()
    return end - start


def bench_rust(
    binary_path: Path,
    path: Path,
    rows: int,
    full: bool,
    columns: list[str] | None,
    col_count: int | None,
    rust_pipeline: bool,
    chunk_size: int | None,
) -> float:
    result = subprocess.run(
        [
            str(binary_path),
            str(path),
            *(["--columns", ",".join(columns)] if columns else []),
            *(["--col-count", str(col_count)] if col_count else []),
            *(["--pipeline"] if rust_pipeline else []),
            *(["--chunk-size", str(chunk_size)] if chunk_size else []),
            "full" if full else str(rows),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Rust reader failed")
    line = result.stdout.strip().splitlines()[-1]
    if not line.startswith("elapsed_ms="):
        raise RuntimeError(f"Unexpected output: {line}")
    try:
        ms = float(line.split()[0].split("=", 1)[1])
    except Exception as exc:
        raise RuntimeError(f"Failed to parse elapsed_ms from: {line}") from exc
    return ms / 1000.0


def bench_rust_list(
    binary_path: Path,
    list_file: Path,
    rows: int,
    full: bool,
    columns: list[str] | None,
    col_count: int | None,
    rust_pipeline: bool,
    chunk_size: int | None,
) -> float:
    result = subprocess.run(
        [
            str(binary_path),
            "--list",
            str(list_file),
            *(["--columns", ",".join(columns)] if columns else []),
            *(["--col-count", str(col_count)] if col_count else []),
            *(["--pipeline"] if rust_pipeline else []),
            *(["--chunk-size", str(chunk_size)] if chunk_size else []),
            "full" if full else str(rows),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Rust reader failed")
    line = result.stdout.strip().splitlines()[-1]
    if not line.startswith("elapsed_ms="):
        raise RuntimeError(f"Unexpected output: {line}")
    try:
        ms = float(line.split()[0].split("=", 1)[1])
    except Exception as exc:
        raise RuntimeError(f"Failed to parse elapsed_ms from: {line}") from exc
    return ms / 1000.0


def bench_python_subprocess(
    project_root: Path,
    path: Path | None,
    rows: int,
    full: bool,
    all_files: bool,
    columns: list[str] | None,
    col_count: int | None,
) -> float:
    cmd = [
        sys.executable,
        str(project_root / "tests" / "sas" / "bench_vs_python.py"),
        "--child-python",
        "--rows",
        str(rows),
    ]
    if full:
        cmd.append("--full")
    if columns:
        cmd.extend(["--columns", ",".join(columns)])
    if col_count:
        cmd.extend(["--col-count", str(col_count)])
    if all_files:
        cmd.append("--all")
    else:
        cmd.extend(["--file", str(path)])
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=project_root,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Python reader failed")
    line = result.stdout.strip().splitlines()[-1]
    if not line.startswith("elapsed_ms="):
        raise RuntimeError(f"Unexpected output: {line}")
    try:
        ms = float(line.split()[0].split("=", 1)[1])
    except Exception as exc:
        raise RuntimeError(f"Failed to parse elapsed_ms from: {line}") from exc
    return ms / 1000.0


def summarize(label: str, times: list[float]) -> None:
    ms = [t * 1000.0 for t in times]
    print(f"{label}:")
    print(f"  runs: {len(ms)}")
    print(f"  mean: {statistics.mean(ms):.2f} ms")
    print(f"  p50:  {statistics.median(ms):.2f} ms")
    if len(ms) >= 2:
        print(f"  stdev:{statistics.stdev(ms):.2f} ms")


def iter_all_files(project_root: Path) -> list[Path]:
    data_dir = project_root / "tests" / "sas" / "data"
    files = sorted(data_dir.rglob("*.sas7bdat"))
    return [p for p in files if p.is_file() and p.stat().st_size < MAX_FILE_SIZE]


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    list_file = project_root / "tests" / "sas" / ".bench_filelist.txt"
    binary_path = project_root / "target" / "release" / "examples" / "benchmark_rows"
    columns = _parse_columns(args)
    col_count = args.col_count
    if args.all:
        paths = iter_all_files(project_root)
        if not paths:
            raise SystemExit("No .sas7bdat files found under tests/sas/data/")
        list_file.write_text("".join(f"{p}\n" for p in paths))
    else:
        if args.file is None:
            raise SystemExit("Provide --file or use --all")
        path = args.file
        if not path.exists():
            raise SystemExit(f"File not found: {path}")
        paths = [path]

    if columns and col_count:
        raise SystemExit("Use either --columns or --col-count, not both")
    if (args.first_k or args.last_k) and (columns or col_count):
        raise SystemExit("Use --first-k/--last-k instead of --columns/--col-count")
    if args.first_k and args.last_k:
        raise SystemExit("Use either --first-k or --last-k, not both")
    if (args.first_k or args.last_k) and args.all:
        raise SystemExit("--first-k/--last-k requires a single --file")

    if args.first_k:
        columns = _resolve_first_n_columns(paths[0], args.first_k)
    elif args.last_k:
        columns = _resolve_last_n_columns(paths[0], args.last_k)

    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "benchmark_rows"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        raise RuntimeError(build.stderr.strip() or "Failed to build benchmark_rows example")
    if not binary_path.exists():
        raise RuntimeError(f"Missing built binary: {binary_path}")

    if args.child_python:
        start = time.perf_counter()
        for p in paths:
            try:
                cols = columns
                if col_count and not cols:
                    cols = _resolve_first_n_columns(p, col_count)
                if args.full:
                    df = scan_readstat(str(p)).select(cols).collect() if cols else scan_readstat(str(p)).collect()
                else:
                    df = (
                        scan_readstat(str(p)).select(cols).head(args.rows).collect()
                        if cols
                        else scan_readstat(str(p)).head(args.rows).collect()
                    )
                del df
            except Exception:
                continue
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        print(f"elapsed_ms={elapsed_ms:.3f}")
        return

    print(f"Files: {len(paths)}")
    if args.full:
        print("Rows: full")
    else:
        print(f"Rows: {args.rows}")
    print(f"Repeat: {args.repeat}")
    if args.isolate_python:
        print("Python: isolated per repetition")
    if args.rust_pipeline:
        print("Rust: pipeline")
        if args.chunk_size:
            print(f"Rust chunk size: {args.chunk_size}")
    if args.first_k:
        print(f"Columns: first {args.first_k}")
    elif args.last_k:
        print(f"Columns: last {args.last_k}")
    if columns:
        print(f"Columns: {len(columns)} specified")
    elif col_count:
        print(f"Columns: first {col_count}")
    print("")

    py_times: list[float] = []
    rust_times: list[float] = []

    if args.rust_profile:
        profile_cmd = [
            str(binary_path),
            *(["--list", str(list_file)] if args.all else [str(paths[0])]),
            *(["--columns", ",".join(columns)] if columns else []),
            *(["--col-count", str(col_count)] if col_count else []),
            *(["--pipeline"] if args.rust_pipeline else []),
            *(["--chunk-size", str(args.chunk_size)] if args.chunk_size else []),
            "--profile",
            "full" if args.full else str(args.rows),
        ]
        profile = subprocess.run(profile_cmd, cwd=project_root, capture_output=True, text=True)
        if profile.returncode != 0:
            raise RuntimeError(profile.stderr.strip() or "Rust profile run failed")
        print(profile.stdout.strip())
        return

    for i in range(args.repeat):
        if args.isolate_python:
            try:
                py_times.append(
                    bench_python_subprocess(
                        project_root,
                        paths[0] if paths else None,
                        args.rows,
                        args.full,
                        args.all,
                        columns,
                        col_count,
                    )
                )
            except Exception:
                py_times.append(float("nan"))
        else:
            start_py = time.perf_counter()
            for p in paths:
                try:
                    cols = columns
                    if col_count and not cols:
                        cols = _resolve_first_n_columns(p, col_count)
                    if args.full:
                        df = scan_readstat(str(p)).select(cols).collect() if cols else scan_readstat(str(p)).collect()
                    else:
                        df = (
                            scan_readstat(str(p)).select(cols).head(args.rows).collect()
                            if cols
                            else scan_readstat(str(p)).head(args.rows).collect()
                        )
                    del df
                except Exception:
                    continue
            py_times.append(time.perf_counter() - start_py)

        if args.all:
            rust_times.append(
                bench_rust_list(
                    binary_path,
                    list_file,
                    args.rows,
                    args.full,
                    columns,
                    col_count,
                    args.rust_pipeline,
                    args.chunk_size,
                )
            )
        else:
            try:
                rust_times.append(
                    bench_rust(
                        binary_path,
                        paths[0],
                        args.rows,
                        args.full,
                        columns,
                        col_count,
                        args.rust_pipeline,
                        args.chunk_size,
                    )
                )
            except Exception:
                rust_times.append(float("nan"))
        print(f"Run {i+1}: python={py_times[-1]*1000:.2f} ms, rust={rust_times[-1]*1000:.2f} ms")

    print("")
    summarize("polars_readstat (C++)", py_times)
    summarize("rust (benchmark_rows)", rust_times)


if __name__ == "__main__":
    main()
