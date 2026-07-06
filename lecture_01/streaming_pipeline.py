import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa

INPUT_PATH = "/tmp/fake_50gb.parquet"
OUTPUT_PATH = "/tmp/pipeline_output.parquet"
parquet_file = pq.ParquetFile(INPUT_PATH)
writer = None
try:
  for i, batch in enumerate(parquet_file.iter_batches(batch_size=rows_per_group)):
    table = pa.table(batch.to_pydict(), schema=batch.schema)

    age_safe = pc.if_else(pc.equal(table["age"], 0), 1, table["age"])
    churned_weight = pc.if_else(pc.equal(table["churned"], 1), 1.8, 1.0)
    risk = pc.multiply(pc.divide(table["spend"], age_safe), churned_weight)

    conditions = [
        (pc.and_(pc.less(table["age"], 25), pc.greater(table["spend"], 500))),
        (pc.and_(pc.greater_equal(table["age"], 25), pc.greater(table["spend"], 1000))),
    ]
    segment = pc.if_else(
        conditions[0], "high_value_young",
        pc.if_else(conditions[1], "high_value_old", "standard")
    )
    table = table.append_column("risk", risk)
    table = table.append_column("segment", segment)


    if writer is None:
            writer = pq.ParquetWriter(
                OUTPUT_PATH,
                table.schema,
                compression="zstd"
            )

    writer.write_table(table)

    if i % 20 == 0:
            print(f"Processed row group {i + 1}")

finally:
    if writer is not None:
        writer.close()

print("Pipeline complete.")
