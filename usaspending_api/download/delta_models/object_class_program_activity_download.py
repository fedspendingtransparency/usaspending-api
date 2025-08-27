OBJECT_CLASS_PROGRAM_ACTIVITY_DOWNLOAD_COLUMNS = {
    "financial_accounts_by_program_activity_object_class_id": {
        "delta": "INTEGER NOT NULL",
        "postgres": "INTEGER NOT NULL",
    },
    "submission_id": {"delta": "INTEGER NOT NULL", "postgres": "INTEGER NOT NULL"},
    "owning_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "federal_account_symbol": {"delta": "STRING", "postgres": "TEXT"},
    "federal_account_name": {"delta": "STRING", "postgres": "TEXT"},
    "agency_identifier_name": {"delta": "STRING", "postgres": "TEXT"},
    "allocation_transfer_agency_identifier_name": {"delta": "STRING", "postgres": "TEXT"},
    "program_activity_code": {"delta": "STRING", "postgres": "TEXT"},
    "program_activity_name": {"delta": "STRING", "postgres": "TEXT"},
    "object_class_code": {"delta": "STRING", "postgres": "TEXT"},
    "object_class_name": {"delta": "STRING", "postgres": "TEXT"},
    "direct_or_reimbursable_funding_source": {"delta": "STRING", "postgres": "TEXT"},
    "disaster_emergency_fund_code": {"delta": "STRING", "postgres": "TEXT"},
    "disaster_emergency_fund_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "federal_account_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "budget_function_title": {"delta": "STRING", "postgres": "TEXT"},
    "budget_function_code": {"delta": "STRING", "postgres": "TEXT"},
    "budget_subfunction_title": {"delta": "STRING", "postgres": "TEXT"},
    "budget_subfunction_code": {"delta": "STRING", "postgres": "TEXT"},
    "reporting_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "reporting_fiscal_period": {"delta": "INTEGER", "postgres": "INTEGER"},
    "reporting_fiscal_quarter": {"delta": "INTEGER", "postgres": "INTEGER"},
    "reporting_fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER"},
    "quarter_format_flag": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "submission_period": {"delta": "STRING", "postgres": "TEXT"},
    "USSGL480100_undelivered_orders_obligations_unpaid_FYB": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL480100_undelivered_orders_obligations_unpaid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL483100_undelivered_orders_obligations_transferred_unpaid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL490100_delivered_orders_obligations_unpaid_FYB": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL490100_delivered_orders_obligations_unpaid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL493100_delivered_orders_obligations_transferred_unpaid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL480200_undelivered_orders_obligations_prepaid_advanced": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL490200_delivered_orders_obligations_paid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL490800_authority_outlayed_not_yet_disbursed_FYB": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL490800_authority_outlayed_not_yet_disbursed": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "obligations_undelivered_orders_unpaid_total_FYB": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "obligations_undelivered_orders_unpaid_total": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "obligations_delivered_orders_unpaid_total": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "obligations_delivered_orders_unpaid_total_FYB": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "gross_outlays_undelivered_orders_prepaid_total": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "gross_outlays_undelivered_orders_prepaid_total_fyb": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "gross_outlays_delivered_orders_paid_total_FYB": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "gross_outlays_delivered_orders_paid_total": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "gross_outlay_amount_FYB": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "gross_outlay_amount_FYB_to_period_end": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "obligations_incurred": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "deobligations_or_recoveries_or_refunds_from_prior_year": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "last_modified_date": {"delta": "DATE", "postgres": "DATE"},
    "data_source": {"delta": "STRING", "postgres": "TEXT"},
    "program_activity_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "object_class_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "prior_year_adjustment": {"delta": "STRING", "postgres": "TEXT"},
    "ussgl480110_rein_undel_ord_cpe": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "ussgl490110_rein_deliv_ord_cpe": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "drv_obligations_incurred_by_program_object_class": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "drv_obligations_undelivered_orders_unpaid": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
    },
    "reporting_period_start": {"delta": "DATE", "postgres": "DATE"},
    "reporting_period_end": {"delta": "DATE", "postgres": "DATE"},
    "certified_date": {"delta": "DATE", "postgres": "DATE"},
    "create_date": {"delta": "DATE", "postgres": "DATE"},
    "update_date": {"delta": "DATE", "postgres": "DATE"},
    "treasury_account_id": {"delta": "INTEGER", "postgres": "INTEGER"},
}

OBJECT_CLASS_PROGRAM_ACTIVITY_DOWNLOAD_DELTA_COLUMNS = {
    k: v["delta"] for k, v in OBJECT_CLASS_PROGRAM_ACTIVITY_DOWNLOAD_COLUMNS.items()
}
OBJECT_CLASS_PROGRAM_ACTIVITY_DOWNLOAD_POSTGRES_COLUMNS = {
    k: v["postgres"] for k, v in OBJECT_CLASS_PROGRAM_ACTIVITY_DOWNLOAD_COLUMNS.items()
}

object_class_program_activity_download_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f"{key} {val}" for key, val in OBJECT_CLASS_PROGRAM_ACTIVITY_DOWNLOAD_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """

object_class_program_activity_download_load_sql_string = rf"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} (
        {",".join(list(OBJECT_CLASS_PROGRAM_ACTIVITY_DOWNLOAD_COLUMNS))}
    )
    SELECT
        fabpaoc.financial_accounts_by_program_activity_object_class_id,
        fabpaoc.submission_id,
        ta.name AS owning_agency_name,
        fa.federal_account_code AS federal_account_symbol,
        fa.account_title AS federal_account_name,
        cgac_aid.agency_name AS agency_identifier_name,
        cgac_ata.agency_name AS allocation_transfer_agency_identifier_name,
        rpa.program_activity_code,
        rpa.program_activity_name,
        oc.object_class AS object_class_code,
        oc.object_class_name,
        oc.direct_reimbursable AS direct_or_reimbursable_funding_source,
        fabpaoc.disaster_emergency_fund_code,
        defc.title AS disaster_emergency_fund_name,
        taa.funding_toptier_agency_id,
        taa.federal_account_id,
        taa.budget_function_title,
        taa.budget_function_code,
        taa.budget_subfunction_title,
        taa.budget_subfunction_code,
        sa.reporting_agency_name,
        sa.reporting_fiscal_period,
        sa.reporting_fiscal_quarter,
        sa.reporting_fiscal_year,
        sa.quarter_format_flag,
        CASE
            WHEN sa.quarter_format_flag = TRUE
                THEN
                    CONCAT(
                        CAST('FY' AS STRING),
                        CAST(sa.reporting_fiscal_year AS STRING),
                        CAST('Q' AS STRING),
                        CAST(sa.reporting_fiscal_quarter AS STRING)
                    )
            ELSE
                CONCAT(
                    CAST('FY' AS STRING),
                    CAST(sa.reporting_fiscal_year AS STRING),
                    CAST('P' AS STRING),
                    LPAD(CAST(sa.reporting_fiscal_period AS STRING), 2, '0')
                )
        END AS submission_period,
        fabpaoc.ussgl480100_undelivered_orders_obligations_unpaid_fyb AS USSGL480100_undelivered_orders_obligations_unpaid_FYB,
        fabpaoc.ussgl480100_undelivered_orders_obligations_unpaid_cpe AS USSGL480100_undelivered_orders_obligations_unpaid,
        fabpaoc.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe AS USSGL483100_undelivered_orders_obligations_transferred_unpaid,
        fabpaoc.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe AS USSGL488100_upward_adj_prior_year_undeliv_orders_oblig_unpaid,
        fabpaoc.ussgl490100_delivered_orders_obligations_unpaid_fyb AS USSGL490100_delivered_orders_obligations_unpaid_FYB,
        fabpaoc.ussgl490100_delivered_orders_obligations_unpaid_cpe AS USSGL490100_delivered_orders_obligations_unpaid,
        fabpaoc.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe AS USSGL493100_delivered_orders_obligations_transferred_unpaid,
        fabpaoc.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe AS USSGL498100_upward_adj_of_prior_year_deliv_orders_oblig_unpaid,
        fabpaoc.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb AS USSGL480200_undelivered_orders_obligations_prepaid_advanced_FYB,
        fabpaoc.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe AS USSGL480200_undelivered_orders_obligations_prepaid_advanced,
        fabpaoc.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe AS USSGL483200_undeliv_orders_oblig_transferred_prepaid_advanced,
        fabpaoc.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe AS USSGL488200_upward_adj_prior_year_undeliv_orders_oblig_prepaid,
        fabpaoc.ussgl490200_delivered_orders_obligations_paid_cpe AS USSGL490200_delivered_orders_obligations_paid,
        fabpaoc.ussgl490800_authority_outlayed_not_yet_disbursed_fyb AS USSGL490800_authority_outlayed_not_yet_disbursed_FYB,
        fabpaoc.ussgl490800_authority_outlayed_not_yet_disbursed_cpe AS USSGL490800_authority_outlayed_not_yet_disbursed,
        fabpaoc.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe AS USSGL498200_upward_adj_of_prior_year_deliv_orders_oblig_paid,
        fabpaoc.obligations_undelivered_orders_unpaid_total_fyb AS obligations_undelivered_orders_unpaid_total_FYB,
        fabpaoc.obligations_undelivered_orders_unpaid_total_cpe AS obligations_undelivered_orders_unpaid_total,
        fabpaoc.obligations_delivered_orders_unpaid_total_cpe AS obligations_delivered_orders_unpaid_total,
        fabpaoc.obligations_delivered_orders_unpaid_total_cpe AS obligations_delivered_orders_unpaid_total_FYB,
        fabpaoc.gross_outlays_undelivered_orders_prepaid_total_cpe AS gross_outlays_undelivered_orders_prepaid_total,
        fabpaoc.gross_outlays_undelivered_orders_prepaid_total_cpe AS gross_outlays_undelivered_orders_prepaid_total_FYB,
        fabpaoc.gross_outlays_delivered_orders_paid_total_fyb AS gross_outlays_delivered_orders_paid_total_FYB,
        fabpaoc.gross_outlays_delivered_orders_paid_total_cpe AS gross_outlays_delivered_orders_paid_total,
        fabpaoc.gross_outlay_amount_by_program_object_class_fyb AS gross_outlay_amount_FYB,
        fabpaoc.gross_outlay_amount_by_program_object_class_cpe AS gross_outlay_amount_FYB_to_period_end,
        fabpaoc.obligations_incurred_by_program_object_class_cpe AS obligations_incurred,
        fabpaoc.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe AS USSGL487100_downward_adj_prior_year_unpaid_undeliv_orders_oblig,
        fabpaoc.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe AS USSGL497100_downward_adj_prior_year_unpaid_deliv_orders_oblig,
        fabpaoc.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe AS USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,
        fabpaoc.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe AS USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,
        fabpaoc.deobligations_recoveries_refund_pri_program_object_class_cpe AS deobligations_or_recoveries_or_refunds_from_prior_year,
        CAST(sa.published_date AS DATE) AS last_modified_date,
        fabpaoc.data_source,
        fabpaoc.program_activity_id,
        fabpaoc.object_class_id,
        fabpaoc.prior_year_adjustment,
        fabpaoc.ussgl480110_rein_undel_ord_cpe,
        fabpaoc.ussgl490110_rein_deliv_ord_cpe,
        fabpaoc.drv_obligations_incurred_by_program_object_class,
        fabpaoc.drv_obligations_undelivered_orders_unpaid,
        fabpaoc.reporting_period_start,
        fabpaoc.reporting_period_end,
        fabpaoc.certified_date,
        fabpaoc.create_date,
        fabpaoc.update_date,
        fabpaoc.treasury_account_id
    FROM
        global_temp.financial_accounts_by_program_activity_object_class AS fabpaoc
    INNER JOIN global_temp.submission_attributes AS sa ON
        fabpaoc.submission_id = sa.submission_id
    LEFT JOIN global_temp.treasury_appropriation_account AS taa ON
        fabpaoc.treasury_account_id = taa.treasury_account_identifier
    LEFT JOIN global_temp.federal_account AS fa ON
        taa.federal_account_id = fa.id
    LEFT JOIN global_temp.toptier_agency AS ta ON
        fa.parent_toptier_agency_id = ta.toptier_agency_id
    LEFT JOIN global_temp.ref_program_activity AS rpa ON
        fabpaoc.program_activity_id = rpa.id
    LEFT JOIN global_temp.object_class AS oc ON
        fabpaoc.object_class_id = oc.id
    LEFT JOIN global_temp.disaster_emergency_fund_code AS defc ON
        fabpaoc.disaster_emergency_fund_code = defc.code
    LEFT JOIN global_temp.cgac AS cgac_aid ON
        taa.agency_id = cgac_aid.cgac_code
    LEFT JOIN global_temp.cgac AS cgac_ata ON
        taa.allocation_transfer_agency_id = cgac_ata.cgac_code
    """
