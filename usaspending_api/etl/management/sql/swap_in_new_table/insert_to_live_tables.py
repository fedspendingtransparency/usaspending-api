insert_to_live_tables_sql = """
INSERT INTO {dest_table}
SELECT {insert_cols}
FROM {upsert_temp_table} AS s
LEFT JOIN {dest_table} d
ON {join_condition}
WHERE {null_column} IS NULL;
"""
