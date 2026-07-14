def smart_scan(directory, country_filter, risk_threshold):
    files = sorted([
        os.path.join(directory, f)
        for f in os.listdir(directory) if f.endswith(".parquet")
    ])

    files_checked = 0
    files_skipped = 0
    row_groups_skipped = 0
    results = []

    for path in files:
        files_checked += 1
        pf = pq.ParquetFile(path)
        meta = pf.metadata

        file_should_skip = country_filter not in path

        if file_should_skip:
            files_skipped += 1
            continue

        for rg_index in range(meta.num_row_groups):
            rg = meta.row_group(rg_index)

            risk_col = None
            for i in range(rg.num_columns):
                col = rg.column(i)
                if col.path_in_schema == "risk":
                    risk_col = col
                    break

            if risk_col is None:
                raise ValueError(f"Column 'risk' not found in {path}")

            if risk_col.statistics.max < risk_threshold:
                row_groups_skipped += 1
                continue

            table = pf.read_row_group(rg_index)
            mask = pc.and_(
                pc.equal(table["country"], country_filter),
                pc.greater(table["risk"], risk_threshold)
            )
            results.append(table.filter(mask))

    print(f"Files checked      : {files_checked}")
    print(f"Files skipped      : {files_skipped}")
    print(f"Row groups skipped : {row_groups_skipped}")
    print(f"Rows returned      : {sum(len(r) for r in results):,}")

    return pa.concat_tables(results) if results else pa.table({})
