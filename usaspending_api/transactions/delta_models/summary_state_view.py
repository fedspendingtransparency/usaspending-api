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
        NULL AS total_outlays  -- Default value for new column
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
    # Using the file_C records, create a temporary view to sum the award total outlays by year
    # -----
    r"""
    CREATE OR REPLACE TEMPORARY VIEW award_outlay_sums AS (
        SELECT
            faba.award_id,
            sa.reporting_fiscal_year AS award_fiscal_year,
            SUM(faba.gross_outlay_amount_by_award_cpe) AS total_gross_outlay_amount_by_award_cpe,
            SUM(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe) AS total_ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
            SUM(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe) AS total_ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe
        FROM int.financial_accounts_by_awards faba
        INNER JOIN global_temp.submission_attributes sa
            ON faba.submission_id = sa.submission_id
        WHERE sa.is_final_balances_for_fy = TRUE
        GROUP BY faba.award_id, sa.reporting_fiscal_year
    );
    """,
    # -----
    # Create the Second Temporary View for Coalescing the Summed Values
    # -----
    r"""
    CREATE OR REPLACE TEMPORARY VIEW award_total_outlays AS (
        SELECT
            award_id,
            award_fiscal_year,
            COALESCE(total_gross_outlay_amount_by_award_cpe, 0)
            + COALESCE(total_ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
            + COALESCE(total_ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0) AS total_outlays
        FROM award_outlay_sums
        WHERE total_gross_outlay_amount_by_award_cpe IS NOT NULL
            OR total_ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe IS NOT NULL
            OR total_ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe IS NOT NULL
    );
    """,
    # -----
    # Unnest the distinct awards from summary_state_view table to get distinct award_id's
    # -----
    fr"""
    CREATE OR REPLACE TEMPORARY VIEW split_awards AS (
        SELECT
            ssv.duh,
            explode(split(ssv.distinct_awards, ',')) AS award_id
        FROM
           {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} ssv
    );
    """,
    # -----
    # Join split_awards with award_total_outlays to get total_outlays for each award_id
    # -----
    r"""
    CREATE OR REPLACE TEMPORARY VIEW awards_outlays_aggregated AS (
        SELECT
            split_awards.duh,
            SUM(ato.total_outlays) AS aggregated_total_outlays
        FROM
            split_awards
        LEFT JOIN
            award_total_outlays ato
        ON
            split_awards.award_id = ato.award_id
        GROUP BY
            split_awards.duh
    );
    """,
    # -----
    # Update the summary_state_view.total_outlays with calculated values
    # -----
    fr"""
    MERGE INTO {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} ssv
    USING awards_outlays_aggregated aoa
    ON ssv.duh = aoa.duh
    WHEN MATCHED THEN
    UPDATE SET ssv.total_outlays = aoa.aggregated_total_outlays;
    """,
]
