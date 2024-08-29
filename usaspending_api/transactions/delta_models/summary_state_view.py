SUMMARY_STATE_VIEW_COLUMNS = {
    "duh": {"delta": "STRING", "postgres": "UUID"},
    "action_date": {"delta": "DATE", "postgres": "DATE"},
    "fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER"},
    "type": {"delta": "STRING", "postgres": "TEXT"},
    "distinct_awards": {"delta": "STRING", "postgres": "TEXT"},
    "pop_country_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "generated_pragmatic_obligation": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "federal_action_obligation": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "original_loan_subsidy_cost": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "face_value_loan_guarantee": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "counts": {"delta": "LONG", "postgres": "BIGINT"},
    "total_outlays": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
}

SUMMARY_STATE_VIEW_DELTA_COLUMNS = {k: v["delta"] for k, v in SUMMARY_STATE_VIEW_COLUMNS.items()}
SUMMARY_STATE_VIEW_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in SUMMARY_STATE_VIEW_COLUMNS.items()}

summary_state_view_create_sql_string = fr"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in SUMMARY_STATE_VIEW_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""

summary_state_view_load_sql_string = [
    fr"""
    -- Step 1: Populate the summary_state_view table with initial values and set total_outlays to 0
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join([col for col in SUMMARY_STATE_VIEW_COLUMNS])}
    )
    SELECT
        -- TODO: Update the "duh" field to determine uniqueness by leveraging the GROUP BY fields
        REGEXP_REPLACE(
            MD5(
                CONCAT_WS(
                    ' ',
                    SORT_ARRAY(COLLECT_LIST(transaction_normalized.id))
                )
            ),
            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
            '\$1-\$2-\$3-\$4-\$5'
        ) AS duh,
        transaction_normalized.action_date,
        transaction_normalized.fiscal_year,
        transaction_normalized.type,
        CONCAT_WS(
            ',',
            SORT_ARRAY(COLLECT_SET(transaction_normalized.award_id))
        ) AS distinct_awards,
        COALESCE(
            transaction_fpds.place_of_perform_country_c,
            transaction_fabs.place_of_perform_country_c,
            'USA'
        ) AS pop_country_code,
        COALESCE(
            transaction_fpds.place_of_performance_state,
            transaction_fabs.place_of_perfor_state_code
        ) AS pop_state_code,
        CAST(
            COALESCE(
                SUM(
                    CASE
                        WHEN COALESCE(transaction_normalized.type, '') IN ('07', '08')
                            THEN transaction_normalized.original_loan_subsidy_cost
                        ELSE transaction_normalized.federal_action_obligation
                    END
                ),
                0
            ) AS NUMERIC(23,2)
        ) AS generated_pragmatic_obligation,
        CAST(
            COALESCE(
                SUM(transaction_normalized.federal_action_obligation),
                0
            ) AS NUMERIC(23, 2)
        ) AS federal_action_obligation,
        CAST(
            COALESCE(
                SUM(transaction_normalized.original_loan_subsidy_cost),
                0
            ) AS NUMERIC(23, 2)
        ) AS original_loan_subsidy_cost,
        CAST(
            COALESCE(
                SUM(transaction_normalized.face_value_loan_guarantee),
                0
            ) AS NUMERIC(23, 2)
        ) AS face_value_loan_guarantee,
        COUNT(*) AS counts,
        0 AS total_outlays  -- Default value for new column
    FROM
        int.transaction_normalized
    LEFT OUTER JOIN
        int.transaction_fpds ON (transaction_normalized.id = transaction_fpds.transaction_id)
    LEFT OUTER JOIN
        int.transaction_fabs ON (transaction_normalized.id = transaction_fabs.transaction_id)
    WHERE
        transaction_normalized.action_date >= '2007-10-01'
        AND COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA') = 'USA'
        AND COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) IS NOT NULL
    GROUP BY
        transaction_normalized.action_date,
        transaction_normalized.fiscal_year,
        transaction_normalized.type,
        COALESCE(
            transaction_fpds.place_of_perform_country_c,
            transaction_fabs.place_of_perform_country_c,
            'USA'
        ),
        COALESCE(
            transaction_fpds.place_of_performance_state,
            transaction_fabs.place_of_perfor_state_code
        )
    """,
    # -----
    # Unnest the distinct awards from summary_state_view table
    # -----
    fr"""
    CREATE OR REPLACE TEMPORARY VIEW split_awards AS (
        SELECT
            ssv.duh,
            explode(split(ssv.distinct_awards, ',')) AS award_id,
            ssv.pop_country_code,
            ssv.pop_state_code
        FROM
           {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} ssv
    );
    """,
    # -----
    # Using the award_id's from split_award's map them to award_search table
    # and get matching awards
    # -----
    fr"""
    CREATE OR REPLACE TEMPORARY VIEW matching_awards AS (
        SELECT
            sa.duh,
            COALESCE(SUM(as2.total_outlays), 0) AS calculated_total_outlays
        FROM
            split_awards sa
        JOIN
            rpt.award_search as2 ON CAST(sa.award_id AS BIGINT) = as2.award_id
        WHERE
            as2.action_date >= '2007-10-01'
            AND as2.pop_country_code = sa.pop_country_code
            AND as2.pop_state_code = sa.pop_state_code
        GROUP BY
            sa.duh
    );
    """,
    # -----
    # Update the summary_state_view.total_outlays with calculated values
    # -----
    fr"""
    MERGE INTO {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} ssv
    USING matching_awards ma
    ON ssv.duh = ma.duh
    WHEN MATCHED THEN
    UPDATE SET ssv.total_outlays = ma.calculated_total_outlays;
    """,
]
