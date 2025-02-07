DOWNLOAD_QUERY = """
    SELECT
        owning_agency_name,
        CONCAT_WS('; ', COLLECT_SET(reporting_agency_name)) AS reporting_agency_name,
        submission_period,
        federal_account_symbol,
        federal_account_name,
        agency_identifier_name,
        CONCAT_WS('; ', COLLECT_SET(budget_function)) AS budget_function,
        CONCAT_WS('; ', COLLECT_SET(budget_subfunction)) AS budget_subfunction,
        program_activity_code,
        program_activity_name,
        object_class_code,
        object_class_name,
        direct_or_reimbursable_funding_source,
        disaster_emergency_fund_code,
        disaster_emergency_fund_name,
        SUM(transaction_obligated_amount) AS transaction_obligated_amount,
        SUM(gross_outlay_amount_FYB_to_period_end) AS gross_outlay_amount_FYB_to_period_end,
        SUM(USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig) AS USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,
        SUM(USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig) AS USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,
        award_unique_key,
        award_id_piid,
        parent_award_id_piid,
        award_id_fain,
        award_id_uri,
        award_base_action_date,
        award_base_action_date_fiscal_year,
        award_latest_action_date,
        award_latest_action_date_fiscal_year,
        period_of_performance_start_date,
        period_of_performance_current_end_date,
        ordering_period_end_date,
        award_type_code,
        award_type,
        idv_type_code,
        idv_type,
        prime_award_base_transaction_description,
        awarding_agency_code,
        awarding_agency_name,
        awarding_subagency_code,
        awarding_subagency_name,
        awarding_office_code,
        awarding_office_name,
        funding_agency_code,
        funding_agency_name,
        funding_sub_agency_code,
        funding_sub_agency_name,
        funding_office_code,
        funding_office_name,
        recipient_uei,
        recipient_duns,
        recipient_name,
        recipient_name_raw,
        recipient_parent_uei,
        recipient_parent_duns,
        recipient_parent_name,
        recipient_parent_name_raw,
        recipient_country,
        recipient_state,
        recipient_county,
        recipient_city,
        prime_award_summary_recipient_cd_original,
        prime_award_summary_recipient_cd_current,
        recipient_zip_code,
        primary_place_of_performance_country,
        primary_place_of_performance_state,
        primary_place_of_performance_county,
        prime_award_summary_place_of_performance_cd_original,
        prime_award_summary_place_of_performance_cd_current,
        primary_place_of_performance_zip_code,
        cfda_number,
        cfda_title,
        product_or_service_code,
        product_or_service_code_description,
        naics_code,
        naics_description,
        national_interest_action_code,
        national_interest_action,
        usaspending_permalink,
        MAX(last_modified_date)        
    FROM rpt.account_download
    WHERE
        (
            submission_id IN {}
            OR (
                (
                    (
                        reporting_fiscal_period <= 12
                        AND NOT quarter_format_flag)
                    OR (
                        reporting_fiscal_quarter <= 4
                        AND quarter_format_flag
                    )
                )
                AND reporting_fiscal_year = 2021
            )
        )        
    GROUP BY
        owning_agency_name,        
        federal_account_symbol,
        federal_account_name,
        agency_identifier_name,
        program_activity_code,
        program_activity_name,
        object_class_code,
        object_class_name,
        direct_or_reimbursable_funding_source,
        disaster_emergency_fund_code,
        disaster_emergency_fund_name,
        award_unique_key,
        award_id_piid,
        parent_award_id_piid,
        award_id_fain,
        award_id_uri,
        award_base_action_date,
        award_latest_action_date,
        period_of_performance_start_date,
        period_of_performance_current_end_date,
        ordering_period_end_date,
        idv_type_code,
        idv_type,
        prime_award_base_transaction_description,
        awarding_agency_code,
        awarding_agency_name,
        awarding_subagency_code,
        awarding_subagency_name,
        awarding_office_code,
        awarding_office_name,
        funding_agency_code,
        funding_agency_name,
        funding_sub_agency_code,
        funding_sub_agency_name,
        funding_office_code,
        funding_office_name,
        recipient_uei,
        recipient_duns,
        recipient_name,
        recipient_name_raw,
        recipient_parent_uei,
        recipient_parent_duns,
        recipient_parent_name,
        recipient_parent_name_raw,
        recipient_country,
        recipient_state,
        recipient_county,
        recipient_city,
        primary_place_of_performance_country,
        primary_place_of_performance_state,
        primary_place_of_performance_county,
        primary_place_of_performance_zip_code,
        cfda_number,
        cfda_title,
        product_or_service_code,
        product_or_service_code_description,
        naics_code,
        naics_description,
        national_interest_action_code,
        national_interest_action,
        submission_period,
        award_type_code,
        award_type,
        recipient_zip_code,
        award_base_action_date_fiscal_year,
        award_latest_action_date_fiscal_year,
        usaspending_permalink,
        prime_award_summary_recipient_cd_original,
        prime_award_summary_recipient_cd_current,
        prime_award_summary_place_of_performance_cd_original,
        prime_award_summary_place_of_performance_cd_current
    HAVING
        -- All of the HAVING statements below ensure we return only non-zero sum records
        SUM(gross_outlay_amount_fyb_to_period_end) > 0
        OR SUM(gross_outlay_amount_fyb_to_period_end) < 0
        OR SUM(ussgl487200_downward_adj_prior_year_prepaid_undeliv_order_oblig) < 0
        OR SUM(ussgl487200_downward_adj_prior_year_prepaid_undeliv_order_oblig) > 0
        OR SUM(ussgl497200_downward_adj_of_prior_year_paid_deliv_orders_oblig) < 0
        OR SUM(ussgl497200_downward_adj_of_prior_year_paid_deliv_orders_oblig) > 0
        OR SUM(transaction_obligated_amount) > 0
        OR SUM(transaction_obligated_amount) < 0
"""



SUBMISSION_ID_QUERY = """
    SELECT  submission_id
    FROM    global_temp.submission_attributes
    WHERE   (toptier_code, reporting_fiscal_year, reporting_fiscal_period) IN (
                SELECT  toptier_code, reporting_fiscal_year, reporting_fiscal_period
                FROM    global_temp.submission_attributes
                WHERE   reporting_fiscal_year = 2021 AND
                        (
                            (reporting_fiscal_quarter <= 4 AND quarter_format_flag is true) OR
                            (reporting_fiscal_period <= 12 AND quarter_format_flag is false)
                        )
                ORDER   BY toptier_code, reporting_fiscal_period desc
            ) AND
            (
                (reporting_fiscal_quarter = 4 AND quarter_format_flag IS TRUE) OR
                (reporting_fiscal_period = 12 AND quarter_format_flag IS FALSE)
            )
"""
