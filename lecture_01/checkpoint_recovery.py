import os
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

INPUT_PATH = "/tmp/fake_50gb.parquet"
OUTPUT_DIR = "/tmp/pipeline_chunks/"
CKPT_PATH = "/tmp/pipeline.ckpt"
ROWS_PER_GROUP = 250_000

os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_checkpoint():
    if not os.path.exists(CKPT_PATH):
        return set()

    with open(CKPT_PATH, "r") as f:
        return set(int(line.strip()) for line in f if line.strip())

def save_checkpoint(ckpt_set, index):
    ckpt_set.add(index)

    with open(CKPT_PATH, "w") as f:
        for idx in sorted(ckpt_set):
            f.write(f"{idx}\n")

        f.flush()
        os.fsync(f.fileno())

parquet_file = pq.ParquetFile(INPUT_PATH)
completed = load_checkpoint()

try:
    for i, batch in enumerate(parquet_file.iter_batches(batch_size=rows_per_group)):
        if i in completed:
            print(f"Skipping row group {i} (already done)")
            continue

        table = pa.table(batch.to_pydict(), schema=batch.schema)
        age_safe = pc.if_else(pc.equal(table["age"], 0), 1, table["age"])
        churn_weight = pc.if_else(pc.equal(table["churned"], 1), 1.8, 1.0)
        risk = pc.multiply(pc.divide(table["spend"], age_safe), churn_weight)

        conditions = [
            pc.and_(pc.less(table["age"], 25), pc.greater(table["spend"], 500)),
            pc.and_(pc.greater_equal(table["age"], 25), pc.greater(table["spend"], 1000)),
        ]

        segment = pc.if_else(
            conditions[0], "high_value_young",
            pc.if_else(conditions[1], "high_value_adult", "standard")
        )

        table = table.append_column("risk", risk)
        table = table.append_column("segment", segment)

        chunk_path = os.path.join(OUTPUT_DIR, f"chunk_{i:04d}.parquet")
        pq.write_table(table, chunk_path, compression="zstd")

        save_checkpoint(completed, i)

        if i % 20 == 0:
            print(f"Processed row group {i+1}")

finally:
    print("Pipeline exited.")

print("Pipeline complete.")
