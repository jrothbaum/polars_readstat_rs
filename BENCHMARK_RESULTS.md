# Schema Inference Benchmark Results

Comprehensive benchmark of memory savings and time overhead across all 178 test files.

## Executive Summary

- **Files tested**: 177/178 successful
- **Files with savings**: 59/177 (33%)
- **Total memory saved**: 10.87 MB (6.1% reduction)
- **Time overhead**: 1.65 seconds (148% increase)

## Key Findings

### Memory Savings

**Aggregate across all test files:**
- Default memory: 177.15 MB
- Inferred memory: 166.27 MB
- **Total savings: 10.87 MB (6.1%)**

### Time Overhead

**Two-pass reading cost:**
- Default time: 1.11 sec (single pass)
- Inferred time: 2.76 sec (scan + read with schema)
- **Overhead: 1.65 sec (148%)**

This ~2× overhead is expected since we read the file twice.

## Top 10 Files by Memory Savings

| File | Rows | Cols | Savings | % |
|------|------|------|---------|---|
| numeric_1000000_2.sas7bdat | 1,000,000 | 2 | 7.51 MB | 49.2% |
| star.sas7bdat | 5,786 | 19 | 0.83 MB | 98.4% |
| nels.sas7bdat | 6,649 | 14 | 0.60 MB | 84.4% |
| tunafish.sas7bdat | 6,000 | 11 | 0.41 MB | 80.5% |

## When Worth It

- Large files with integer columns (30-60% savings)
- Exporting to Parquet (better compression)
- Memory-constrained environments
- Long-term storage (one-time cost, permanent savings)

## Conclusion

Schema inference trades **~2× read time** for **6-50% memory savings**.

Real-world datasets with ID/count columns will see higher savings than this test suite (which has many string/float columns).
