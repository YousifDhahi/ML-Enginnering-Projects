import time

start_time = time.perf_counter()

SPOT_PRICE_PER_HOUR = 0.045
BUDGET_USD = 9.00

total_rows = num_row_groups * rows_per_group
wall_time_hours = (time.perf_counter() - start_time) / 3600
estimated_cost =  wall_time_hours * SPOT_PRICE_PER_HOUR
status = "PASS" if estimated_cost <= BUDGET_USD else "FAIL"

print(f"Row groups processed : {num_row_groups}")
print(f"Rows processed       : {total_rows:,}")
print(f"Wall time            : {wall_time_hours*3600:.1f}s")
print(f"Est. spot cost       : ${estimated_cost:.4f}  [budget: ${BUDGET_USD:.2f} — {status}]")
