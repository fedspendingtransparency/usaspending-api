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

summary_state_view_load_sql_string = fr"""
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
        CAST(
            COALESCE(
                SUM(matching_awards.total_outlays),
                0
            ) AS NUMERIC(23, 2)
        ) AS total_outlays
    FROM
        int.transaction_normalized
    JOIN
        rpt.award_search as2 ON (transaction_normalized.award_id = as2.award_id)
    LEFT OUTER JOIN
        int.transaction_fpds ON (transaction_normalized.id = transaction_fpds.transaction_id)
    LEFT OUTER JOIN
        int.transaction_fabs ON (transaction_normalized.id = transaction_fabs.transaction_id)
    LEFT JOIN
        (
            SELECT
                as2.award_id,
                as2.total_outlays
            FROM
                rpt.award_search as2
            WHERE
                as2.action_date >= '2007-10-01'
        ) AS matching_awards ON (
            matching_awards.award_id = transaction_normalized.award_id
            AND COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA') = 'USA'
            AND COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) IS NOT NULL
        )
    WHERE
        transaction_normalized.action_date >= '2007-10-01'
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
"""
