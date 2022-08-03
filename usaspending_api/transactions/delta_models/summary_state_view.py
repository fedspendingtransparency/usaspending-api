SUMMARY_STATE_VIEW_COLUMNS = {
    "duh": "STRING",
    "action_date": "DATE",
    "fiscal_year": "INTEGER",
    "type": "STRING",
    "distinct_awards": "ARRAY<STRING>",
    "pop_country_code": "STRING",
    "pop_state_code": "STRING",
    "generated_pragmatic_obligation": "NUMERIC(23,2)",
    "federal_action_obligation": "NUMERIC(23,2)",
    "original_loan_subsidy_cost": "NUMERIC(23,2)",
    "face_value_loan_guarantee": "NUMERIC(23,2)",
    "counts": "INTEGER",
}

summary_state_view_create_sql_string = fr"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in SUMMARY_STATE_VIEW_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""
