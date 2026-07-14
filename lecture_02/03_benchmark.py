import time

SORTED_DIR = "/tmp/pipeline_sorted/"
country_filter = "US"
risk_threshold = 500.0

t0 = time.perf_counter()
result_baseline = scan_all_chunks(chunk_files, country_filter, risk_threshold)
baseline_time = time.perf_counter() - t0

t0 = time.perf_counter()
result_optimized = smart_scan(SORTED_DIR, country_filter, risk_threshold)
optimized_time = time.perf_counter() - t0

speedup = baseline_time / optimized_time
GB_READ_BASELINE = sum(
    os.path.getsize(p) for p in chunk_files
) / 1024**3
GB_READ_OPTIMIZED = os.path.getsize(
    os.path.join(SORTED_DIR, "country=US.parquet")
) / 1024**3
COST_PER_GB = 0.10

cost_baseline  = GB_READ_BASELINE * COST_PER_GB
cost_optimized = GB_READ_OPTIMIZED * COST_PER_GB
QUERY_BUDGET   = 0.002

print(f"Baseline  : {baseline_time:.3f}s  | files scanned: 200 | rows: {len(result_baseline):,}")
print(f"Optimized : {optimized_time:.3f}s  | files scanned: 1   | rows: {len(result_optimized):,}")
print(f"Speedup   : {speedup:.1f}x")
print(f"Est. cost per query (baseline)  : ${cost_baseline:.5f}  [budget: $0.002 — {'PASS' if cost_baseline <= QUERY_BUDGET else 'FAIL'}]")
print(f"Est. cost per query (optimized) : ${cost_optimized:.5f}  [budget: $0.002 — {'PASS' if cost_optimized <= QUERY_BUDGET else 'FAIL'}]")
