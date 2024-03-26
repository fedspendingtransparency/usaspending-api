update_live_tables_sql = """
UPDATE {dest_table} AS d
{set_cols}
FROM {upsert_temp_table} AS s
WHERE ON {join_condition};
"""
