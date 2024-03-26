delete_from_live_tables_sql = """
MERGE INTO {dest_table} AS d
USING {delete_temp_table} AS s
ON {join_condition}
WHEN MATCHED
    THEN DELETE;
"""
