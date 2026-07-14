def repartition_by_country(chunk_files, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    countries = ["US", "UK", "FR", "JP", "DE"]

    for country in countries:
        writer = None
        try:
            for path in chunk_files:
                table = pq.read_table(path, columns=["user_id", "age", "spend", "country", "churned", "risk", "segment"])
                mask = pc.equal(table["country"], country)
                filtered = table.filter(mask)

                if len(filtered) == 0:
                    continue

                if writer is None:
                    writer = pq.ParquetWriter(
                        os.path.join(output_dir, f"country={country}.parquet"),
                        filtered.schema,
                        compression="zstd",
                    )

                writer.write_table(filtered)

        finally:
            if writer is not None:
                writer.close()

        print(f"Written {country}")

repartition_by_country(chunk_files, SORTED_DIR)
