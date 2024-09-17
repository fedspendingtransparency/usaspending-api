COVID_FABA_SPENDING_COLUMNS = {
    "id": {"delta": "INTEGER", "postgres": "INTEGER NOT NULL"},
    "spending_level": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_id": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_subtier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_subtier_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_federal_account_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_federal_account_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_federal_account_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_treasury_account_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_treasury_account_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_treasury_account_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_major_object_class_id": {"delta": "STRING", "postgres": "TEXT"},
    "funding_major_object_class_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_major_object_class_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_object_class_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_object_class_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_object_class_name": {"delta": "STRING", "postgres": "TEXT"},
    "defc": {"delta": "STRING", "postgres": "TEXT"},
    "award_type": {"delta": "STRING", "postgres": "TEXT"},
    "award_count": {"delta": "LONG", "postgres": "LONG"},
    "obligation_sum": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "outlay_sum": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "face_value_of_loan": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
}

COVID_FABA_SPENDING_DELTA_COLUMNS = {k: v["delta"] for k, v in COVID_FABA_SPENDING_COLUMNS.items()}
COVID_FABA_SPENDING_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in COVID_FABA_SPENDING_COLUMNS.items()}


covid_faba_spending_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in COVID_FABA_SPENDING_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """

covid_faba_spending_load_sql_strings = [
    #   --------------------------------------------------------------------------------
    #     -- Create a temporary view of Covid FABA spending by subtier agencies
    #   --------------------------------------------------------------------------------
    """
    CREATE TEMPORARY VIEW covid_faba_spending_agency_view AS (
        SELECT
            "subtier_agency" AS spending_level,
            funding_toptier_agency_id,
            funding_toptier_agency_code,
            funding_toptier_agency_name,
            funding_subtier_agency_id,
            funding_subtier_agency_code,
            funding_subtier_agency_name,
            NULL AS funding_federal_account_id,
            NULL AS funding_federal_account_code,
            NULL AS funding_federal_account_name,
            NULL AS funding_treasury_account_id,
            NULL AS funding_treasury_account_code,
            NULL AS funding_treasury_account_name,
            NULL AS funding_major_object_class_id,
            NULL AS funding_major_object_class_code,
            NULL AS funding_major_object_class_name,
            NULL AS funding_object_class_id,
            NULL AS funding_object_class_code,
            NULL AS funding_object_class_name,
            defc,
            award_type,
            COUNT(generated_unique_award_id) AS award_count,
            SUM(obligation_sum) AS obligation_sum,
            SUM(outlay_sum) AS outlay_sum,
            SUM(face_value_of_loan) AS face_value_of_loan
        FROM
            (
            SELECT
                agency.id AS funding_toptier_agency_id,
                top_a.toptier_code AS funding_toptier_agency_code,
                top_a.name AS funding_toptier_agency_name,
                sub_a.subtier_agency_id AS funding_subtier_agency_id,
                sub_a.subtier_code AS funding_subtier_agency_code,
                sub_a.name AS funding_subtier_agency_name,
                faba.disaster_emergency_fund_code AS defc,
                awd.type AS award_type,
                awd.generated_unique_award_id,
                SUM(COALESCE(faba.transaction_obligated_amount, 0)) AS obligation_sum,
                SUM(CASE
                    WHEN sa.is_final_balances_for_fy = TRUE
                        THEN
                            COALESCE(gross_outlay_amount_by_award_cpe, 0)
                            +
                            COALESCE(ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                            +
                            COALESCE(ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                    ELSE 0
                END) AS outlay_sum,
                FIRST(awd.total_loan_value) AS face_value_of_loan
            FROM
                int.financial_accounts_by_awards faba
            JOIN
                global_temp.submission_attributes sa ON
                sa.reporting_period_start >= to_date('2020-04-01')
                AND sa.submission_id = faba.submission_id
            JOIN
                global_temp.disaster_emergency_fund_code defc ON
                defc.group_name = 'covid_19'
                AND defc.code = faba.disaster_emergency_fund_code
            JOIN
                global_temp.dabs_submission_window_schedule dabs ON
                dabs.submission_reveal_date <= now()
                AND dabs.id = sa.submission_window_id
            LEFT JOIN
                global_temp.treasury_appropriation_account taa ON
                taa.treasury_account_identifier = faba.treasury_account_id
            LEFT JOIN
                global_temp.toptier_agency top_a ON
                top_a.toptier_agency_id = taa.funding_toptier_agency_id
            LEFT JOIN
                global_temp.agency agency ON
                agency.toptier_agency_id = top_a.toptier_agency_id
                AND agency.toptier_flag = TRUE
            LEFT JOIN
                global_temp.subtier_agency sub_a ON
                sub_a.subtier_agency_id = agency.subtier_agency_id
            LEFT JOIN
                int.awards awd ON
                awd.id = faba.award_id
            WHERE
                defc.group_name = 'covid_19'
                AND
                    (
                        faba.transaction_obligated_amount != 0
                    OR
                        faba.gross_outlay_amount_by_award_cpe != 0
                    OR
                        faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe != 0
                    OR
                        faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe != 0
                    )
            GROUP BY
                agency.id,
                top_a.toptier_code,
                top_a.name,
                sub_a.subtier_agency_id,
                sub_a.subtier_code,
                sub_a.name,
                faba.disaster_emergency_fund_code,
                awd.type,
                awd.generated_unique_award_id
        )
        GROUP BY
            funding_toptier_agency_id,
            funding_toptier_agency_code,
            funding_toptier_agency_name,
            funding_subtier_agency_id,
            funding_subtier_agency_code,
            funding_subtier_agency_name,
            defc,
            award_type
    );
    """,
    #   --------------------------------------------------------------------------------
    #     -- Create a temporary view of Covid FABA spending by treasury accounts
    #   --------------------------------------------------------------------------------
    """
    CREATE TEMPORARY VIEW covid_faba_spending_account_view AS (
        SELECT
            "treasury_account" AS spending_level,
            NULL AS funding_toptier_agency_id,
            NULL AS funding_toptier_agency_code,
            NULL AS funding_toptier_agency_name,
            NULL AS funding_subtier_agency_id,
            NULL AS funding_subtier_agency_code,
            NULL AS funding_subtier_agency_name,
            funding_federal_account_id,
            funding_federal_account_code,
            funding_federal_account_name,
            funding_treasury_account_id,
            funding_treasury_account_code,
            funding_treasury_account_name,
            NULL AS funding_major_object_class_id,
            NULL AS funding_major_object_class_code,
            NULL AS funding_major_object_class_name,
            NULL AS funding_object_class_id,
            NULL AS funding_object_class_code,
            NULL AS funding_object_class_name,
            defc,
            award_type,
            COUNT(generated_unique_award_id) AS award_count,
            SUM(obligation_sum) AS obligation_sum,
            SUM(outlay_sum) AS outlay_sum,
            SUM(face_value_of_loan) AS face_value_of_loan
        FROM
            (
            SELECT
                fa.id AS funding_federal_account_id,
                fa.federal_account_code AS funding_federal_account_code,
                fa.account_title AS funding_federal_account_name,
                taa.treasury_account_identifier AS funding_treasury_account_id,
                taa.tas_rendering_label AS funding_treasury_account_code,
                taa.account_title AS funding_treasury_account_name,
                faba.disaster_emergency_fund_code AS defc,
                awd.type AS award_type,
                awd.generated_unique_award_id,
                SUM(COALESCE(faba.transaction_obligated_amount, 0)) AS obligation_sum,
                SUM(CASE
                    WHEN sa.is_final_balances_for_fy = TRUE
                        THEN
                            COALESCE(gross_outlay_amount_by_award_cpe, 0)
                            +
                            COALESCE(ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                            +
                            COALESCE(ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                    ELSE 0
                END) AS outlay_sum,
                FIRST(awd.total_loan_value) AS face_value_of_loan
            FROM
                int.financial_accounts_by_awards faba
            JOIN
                global_temp.submission_attributes sa ON
                sa.reporting_period_start >= to_date('2020-04-01')
                AND sa.submission_id = faba.submission_id
            JOIN
                global_temp.disaster_emergency_fund_code defc ON
                defc.group_name = 'covid_19'
                AND defc.code = faba.disaster_emergency_fund_code
            JOIN
                global_temp.dabs_submission_window_schedule dabs ON
                dabs.submission_reveal_date <= now()
                AND dabs.id = sa.submission_window_id
            LEFT JOIN
                global_temp.treasury_appropriation_account taa ON
                taa.treasury_account_identifier = faba.treasury_account_id
            LEFT JOIN
                global_temp.federal_account fa ON
                fa.id = taa.federal_account_id
            LEFT JOIN
                int.awards awd ON
                awd.id = faba.award_id
            WHERE
                defc.group_name = 'covid_19'
                AND
                    (
                        faba.transaction_obligated_amount != 0
                    OR
                        faba.gross_outlay_amount_by_award_cpe != 0
                    OR
                        faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe != 0
                    OR
                        faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe != 0
                    )
            GROUP BY
                fa.id,
                fa.federal_account_code,
                fa.account_title,
                taa.treasury_account_identifier,
                taa.tas_rendering_label,
                taa.account_title,
                faba.disaster_emergency_fund_code,
                awd.type,
                awd.generated_unique_award_id
        )
        GROUP BY
            funding_federal_account_id,
            funding_federal_account_code,
            funding_federal_account_name,
            funding_treasury_account_id,
            funding_treasury_account_code,
            funding_treasury_account_name,
            defc,
            award_type
    );
    """,
    #   --------------------------------------------------------------------------------
    #     -- Create a temporary view of Covid FABA spending by object classes
    #   --------------------------------------------------------------------------------
    """
    CREATE TEMPORARY VIEW covid_faba_spending_object_class_view AS (
        SELECT
            "object_class" AS spending_level,
            NULL AS funding_toptier_agency_id,
            NULL AS funding_toptier_agency_code,
            NULL AS funding_toptier_agency_name,
            NULL AS funding_subtier_agency_id,
            NULL AS funding_subtier_agency_code,
            NULL AS funding_subtier_agency_name,
            NULL AS funding_federal_account_id,
            NULL AS funding_federal_account_code,
            NULL AS funding_federal_account_name,
            NULL AS funding_treasury_account_id,
            NULL AS funding_treasury_account_code,
            NULL AS funding_treasury_account_name,
            funding_major_object_class_id,
            funding_major_object_class_code,
            funding_major_object_class_name,
            funding_object_class_id,
            funding_object_class_code,
            funding_object_class_name,
            defc,
            award_type,
            COUNT(generated_unique_award_id) as award_count,
            SUM(obligation_sum) as obligation_sum,
            SUM(outlay_sum) as outlay_sum,
            SUM(face_value_of_loan) as face_value_of_loan
        FROM (
            SELECT
                oc.major_object_class AS funding_major_object_class_id,
                oc.major_object_class AS funding_major_object_class_code,
                oc.major_object_class_name AS funding_major_object_class_name,
                oc.id AS funding_object_class_id,
                oc.object_class AS funding_object_class_code,
                oc.object_class_name AS funding_object_class_name,
                faba.disaster_emergency_fund_code AS defc,
                awd.type AS award_type,
                awd.generated_unique_award_id,
                SUM(COALESCE(faba.transaction_obligated_amount, 0)) AS obligation_sum,
                SUM(CASE
                    WHEN sa.is_final_balances_for_fy = TRUE
                        THEN
                            COALESCE(gross_outlay_amount_by_award_cpe, 0)
                            +
                            COALESCE(ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                            +
                            COALESCE(ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                    ELSE 0
                END) AS outlay_sum,
                FIRST(awd.total_loan_value) as face_value_of_loan
            FROM
                int.financial_accounts_by_awards faba
            JOIN
                global_temp.submission_attributes sa ON	sa.reporting_period_start >= to_date('2020-04-01')	AND sa.submission_id = faba.submission_id
            JOIN
                global_temp.disaster_emergency_fund_code defc ON defc.group_name = 'covid_19' AND defc.code = faba.disaster_emergency_fund_code
            JOIN
                global_temp.dabs_submission_window_schedule dabs ON dabs.submission_reveal_date <= now()	AND dabs.id = sa.submission_window_id
            LEFT JOIN
                global_temp.object_class oc ON oc.id = faba.object_class_id
            LEFT JOIN
                int.awards awd ON	awd.id = faba.award_id
            WHERE
                defc.group_name = 'covid_19'
                AND
                    (
                        faba.transaction_obligated_amount != 0
                        OR
                        faba.gross_outlay_amount_by_award_cpe != 0
                        OR
                        faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe != 0
                        OR
                        faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe != 0
                    )
            GROUP BY
                oc.id,
                oc.major_object_class,
                oc.major_object_class_name,
                oc.object_class,
                oc.object_class_name,
                faba.disaster_emergency_fund_code,
                awd.type,
                awd.generated_unique_award_id
        )
        GROUP BY
            funding_major_object_class_id,
            funding_major_object_class_code,
            funding_major_object_class_name,
            funding_object_class_id,
            funding_object_class_code,
            funding_object_class_name,
            defc,
            award_type
    );
    """,
    #   --------------------------------------------------------------------------------
    #     -- Create a temporary view of Covid FABA spending by award type and DEFC
    #   --------------------------------------------------------------------------------
    """
    CREATE TEMPORARY VIEW covid_faba_spending_awards_view AS (
        SELECT
            "awards" AS spending_level,
            NULL AS funding_toptier_agency_id,
            NULL AS funding_toptier_agency_code,
            NULL AS funding_toptier_agency_name,
            NULL AS funding_subtier_agency_id,
            NULL AS funding_subtier_agency_code,
            NULL AS funding_subtier_agency_name,
            NULL AS funding_federal_account_id,
            NULL AS funding_federal_account_code,
            NULL AS funding_federal_account_name,
            NULL AS funding_treasury_account_id,
            NULL AS funding_treasury_account_code,
            NULL AS funding_treasury_account_name,
            NULL AS funding_major_object_class_id,
            NULL AS funding_major_object_class_code,
            NULL AS funding_major_object_class_name,
            NULL AS funding_object_class_id,
            NULL AS funding_object_class_code,
            NULL AS funding_object_class_name,
            defc,
            award_type,
            COUNT(generated_unique_award_id) AS award_count,
            SUM(obligation_sum) AS obligation_sum,
            SUM(outlay_sum) AS outlay_sum,
            SUM(face_value_of_loan) AS face_value_of_loan
        FROM (
            SELECT
                faba.disaster_emergency_fund_code AS defc,
                awd.type AS award_type,
                awd.generated_unique_award_id,
                SUM(COALESCE(faba.transaction_obligated_amount, 0)) AS obligation_sum,
                SUM(CASE
                        WHEN sa.is_final_balances_for_fy = TRUE
                            THEN
                                COALESCE(gross_outlay_amount_by_award_cpe, 0)
                                +
                                COALESCE(ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                                +
                                COALESCE(ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                        ELSE 0
                    END) AS outlay_sum,
                FIRST(awd.total_loan_value) AS face_value_of_loan
            FROM
                int.financial_accounts_by_awards faba
            JOIN global_temp.submission_attributes sa
                ON sa.reporting_period_start >= to_date('2020-04-01')	AND sa.submission_id = faba.submission_id
            JOIN global_temp.disaster_emergency_fund_code defc
                ON	defc.group_name = 'covid_19' AND defc.code = faba.disaster_emergency_fund_code
            JOIN global_temp.dabs_submission_window_schedule dabs
                ON dabs.submission_reveal_date <= now() AND dabs.id = sa.submission_window_id
            LEFT JOIN int.awards awd
                ON awd.id = faba.award_id
            WHERE
                defc.group_name = 'covid_19'
                AND
                (
                    faba.transaction_obligated_amount != 0
                    OR
                    faba.gross_outlay_amount_by_award_cpe != 0
                    OR
                    faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe != 0
                    OR
                    faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe != 0
                )
            GROUP BY
                faba.disaster_emergency_fund_code,
                awd.type,
                awd.generated_unique_award_id
        )
        GROUP BY
            defc,
            award_type
    );
    """,
    rf"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join(list(COVID_FABA_SPENDING_POSTGRES_COLUMNS))}
    )
    WITH covid_faba_spending_views AS (
        SELECT * FROM covid_faba_spending_agency_view
        UNION ALL
        SELECT * FROM covid_faba_spending_account_view
        UNION ALL
        SELECT * FROM covid_faba_spending_object_class_view
        UNION ALL
        SELECT * FROM covid_faba_spending_awards_view
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id,
        *
    FROM
        covid_faba_spending_views;
    """,
    """DROP VIEW covid_faba_spending_agency_view;""",
    """DROP VIEW covid_faba_spending_account_view;""",
    """DROP VIEW covid_faba_spending_object_class_view;""",
    """DROP VIEW covid_faba_spending_awards_view;""",
]
