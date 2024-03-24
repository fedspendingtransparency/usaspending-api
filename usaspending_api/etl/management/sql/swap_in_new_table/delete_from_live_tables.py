delete_from_live_tables_sql = """
    MERGE INTO rpt.award_search d
    USING {delete_temp_table} AS s
    ON {join_condition}
    WHEN MATCHED
        THEN DELETE;
"""
