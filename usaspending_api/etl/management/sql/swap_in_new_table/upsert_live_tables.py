upsert_live_tables_sql = """
    MERGE INTO rpt.award_search d
    USING {upsert_temp_table} AS s
    ON {join_condition}
    WHEN MATCHED
        THEN UPDATE SET
        {set_cols}
    WHEN NOT MATCHED
        THEN INSERT
            ({insert_col_names})
            VALUES ({insert_values});
"""
