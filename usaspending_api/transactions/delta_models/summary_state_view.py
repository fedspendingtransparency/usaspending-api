SUMMARY_STATE_VIEW_COLUMNS = {
    "duh": {"delta": "STRING", "postgres": "UUID"},
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

summary_state_view_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in SUMMARY_STATE_VIEW_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""

summary_state_view_load_sql_string = [
    rf"""
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
    # Build a list of Award Outlays by Fiscal Year using the File C table
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW outlays_by_year AS (
        SELECT
            award_id,
            sa.reporting_fiscal_year,
            SUM(COALESCE(gross_outlay_amount_by_award_cpe, 0)
                    + COALESCE(ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                    + COALESCE(ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)) AS total_outlays
        FROM
            int.financial_accounts_by_awards faba
        INNER JOIN
            global_temp.submission_attributes sa ON sa.submission_id = faba.submission_id
        WHERE
            sa.is_final_balances_for_fy = true
        GROUP BY award_id, sa.reporting_fiscal_year
    )
    """,
    # -----
    # Determine a list of Award IDs per State, along with their Award Types
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW awards_by_state AS (
        SELECT
            distinct
            award_id,
            tn.type,
            COALESCE(fpds.place_of_performance_state, fabs.place_of_perfor_state_code) AS pop_state_code
        FROM
            int.transaction_normalized tn
        LEFT OUTER JOIN
            int.transaction_fpds fpds ON (tn.id = fpds.transaction_id)
        LEFT OUTER JOIN
            int.transaction_fabs fabs ON (tn.id = fabs.transaction_id)
        WHERE
            tn.action_date >= '2007-10-01'
            AND COALESCE(fpds.place_of_perform_country_c, fabs.place_of_perform_country_c, 'USA') = 'USA'
            AND COALESCE(fpds.place_of_performance_state, fabs.place_of_perfor_state_code) IS NOT NULL
    )
    """,
    # -----
    # Combine the above two views into a single view breaking down outlays by State, Award Type, and Fiscal Year
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW outlays_breakdown AS (
        SELECT
            abs.pop_state_code,
            abs.type,
            oby.reporting_fiscal_year,
            SUM(oby.total_outlays) AS total_outlays
        FROM
            outlays_by_year oby
        INNER JOIN
            awards_by_state abs ON oby.award_id = abs.award_id
        GROUP BY abs.pop_state_code, oby.reporting_fiscal_year, abs.type
    )
    """,
    # -----
    # Update the summary_state_view.total_outlays with calculated values
    # -----
    f"""
    MERGE INTO
        {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} ssv
    USING
        outlays_breakdown ob
    ON
        ob.pop_state_code = ssv.pop_state_code
        AND ob.type = ssv.type
        AND ob.reporting_fiscal_year = ssv.fiscal_year
    WHEN MATCHED THEN
        UPDATE SET ssv.total_outlays = ob.total_outlays;
    """,
]
