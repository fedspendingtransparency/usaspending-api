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

summary_state_view_load_sql_string = rf"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join([col for col in SUMMARY_STATE_VIEW_COLUMNS])}
    )(
 "SELECT",
    "  -- Deterministic Unique Hash (DUH) created for view concurrent refresh",
    "  MD5(array_to_string(sort(array_agg(raw.transaction_normalized.id::int)), ' '))::uuid AS duh,",
    "  raw.transaction_normalized.action_date,",
    "  raw.transaction_normalized.fiscal_year,",
    "  raw.transaction_normalized.type,",
    "  array_to_string(array_agg(DISTINCT traw.ransaction_normalized.award_id), ',') AS distinct_awards,",
    "",
    "  COALESCE(raw.transaction_fpds.place_of_perform_country_c, raw.transaction_fabs.place_of_perform_country_c, 'USA') AS pop_country_code,",
    "  COALESCE(raw.transaction_fpds.place_of_performance_state, raw.transaction_fabs.place_of_perfor_state_code) AS pop_state_code,",
    "",
    "  COALESCE(SUM(CASE",
    "    WHEN raw.transaction_normalized.type IN('07','08') THEN raw.transaction_normalized.original_loan_subsidy_cost",
    "    ELSE raw.transaction_normalized.federal_action_obligation",
    "  END), 0)::NUMERIC(23, 2) AS generated_pragmatic_obligation,",
    "  COALESCE(SUM(raw.transaction_normalized.federal_action_obligation), 0)::NUMERIC(23, 2) AS federal_action_obligation,",
    "  COALESCE(SUM(raw.transaction_normalized.original_loan_subsidy_cost), 0)::NUMERIC(23, 2) AS original_loan_subsidy_cost,",
    "  COALESCE(SUM(raw.transaction_normalized.face_value_loan_guarantee), 0)::NUMERIC(23, 2) AS face_value_loan_guarantee,",
    "  count(*) AS counts",
    "FROM",
    "  raw.transaction_normalized",
    "LEFT OUTER JOIN",
    "  raw.transaction_fpds ON (raw.transaction_normalized.id = raw.transaction_fpds.transaction_id)",
    "LEFT OUTER JOIN",
    "  raw.transaction_fabs ON (raw.transaction_normalized.id = raw.transaction_fabs.transaction_id)",
    "WHERE",
    "  raw.transaction_normalized.action_date >= '2007-10-01' AND",
    "  COALESCE(raw.transaction_fpds.place_of_perform_country_c, raw.transaction_fabs.place_of_perform_country_c, 'USA') = 'USA' AND",
    "  COALESCE(raw.transaction_fpds.place_of_performance_state, raw.transaction_fabs.place_of_perfor_state_code) IS NOT NULL",
    "GROUP BY",
    "  raw.transaction_normalized.action_date,",
    "  raw.transaction_normalized.fiscal_year,",
    "  raw.transaction_normalized.type,",
    "  pop_country_code,",
    "  pop_state_code"
    )
   """
