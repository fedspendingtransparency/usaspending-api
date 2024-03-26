update_live_tables_sql = """
UPDATE {dest_table} AS d
SET {set_cols}
FROM {upsert_temp_table} AS s
WHERE {join_condition};
"""
