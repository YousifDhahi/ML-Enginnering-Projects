import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import time
import os

num_row_groups = 200
rows_per_group = 250_000
OUTPUT_PATH = "/tmp/fake_50gb.parquet"

writer = None

for i in range(num_row_groups):
    np.random.seed(i)

    batch = pa.table({
        "user_id": pa.array(np.arange(i * rows_per_group, (i + 1) * rows_per_group, dtype=np.int32)),
        "age": pa.array(np.random.randint(1, 90, size=rows_per_group), type=pa.int8()),
        "spend": pa.array(np.random.uniform(0, 10_000, size=rows_per_group).astype(np.float32)),
        "country": pa.array(np.random.choice(["US", "UK", "FR", "JP", "DE"], rows_per_group)),
        "churned": pa.array(np.random.randint(0, 2, rows_per_group), type=pa.int8()),
    })

    if writer is None:
        writer = pq.ParquetWriter(OUTPUT_PATH, batch.schema, compression="zstd")

    writer.write_table(batch)

    if i % 20 == 0:
        print(f"Written row group {i+1}/{num_row_groups}")

writer.close()
print("Done.")
