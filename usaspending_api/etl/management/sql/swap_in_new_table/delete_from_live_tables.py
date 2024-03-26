delete_from_live_tables_sql = """
DELETE FROM {dest_table}
WHERE {delete_col} in (
    SELECT {delete_col}
        FROM {delete_temp_table}
)
"""
