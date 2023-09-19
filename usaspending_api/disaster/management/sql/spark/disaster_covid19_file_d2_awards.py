d2_awards_sql_string = """
SELECT
    award_search.generated_unique_award_id AS assistance_award_unique_key,
    award_search.fain AS award_id_fain,
    award_search.uri AS award_id_uri,
    latest_transaction.sai_number AS sai_number,
    COVID_DEFC.disaster_emergency_funds AS disaster_emergency_fund_codes,
    COVID_DEFC.gross_outlay_amount_by_award_cpe + COVID_DEFC.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe + COVID_DEFC.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe AS `outlayed_amount_from_COVID-19_supplementals`,
    COVID_DEFC.transaction_obligated_amount AS `obligated_amount_from_COVID-19_supplementals`,
    IIJA_DEFC.gross_outlay_amount_by_award_cpe + IIJA_DEFC.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe + IIJA_DEFC.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe AS outlayed_amount_from_IIJA_supplemental,
    IIJA_DEFC.transaction_obligated_amount AS obligated_amount_from_IIJA_supplemental,
    award_search.total_obligation AS total_obligated_amount,
    award_search.total_outlays AS total_outlayed_amount,
    award_search.non_federal_funding_amount AS total_non_federal_funding_amount,
    award_search.total_funding_amount AS total_funding_amount,
    award_search.total_loan_value AS total_face_value_of_loan,
    award_search.total_subsidy_cost AS total_loan_subsidy_cost,
    award_search.date_signed AS award_base_action_date,
    EXTRACT (YEAR FROM (award_search.date_signed) + INTERVAL 3 months) AS award_base_action_date_fiscal_year,
    latest_transaction.action_date AS award_latest_action_date,
    EXTRACT (YEAR FROM (TO_DATE(latest_transaction.action_date)) + INTERVAL 3 months) AS award_latest_action_date_fiscal_year,
    award_search.period_of_performance_start_date AS period_of_performance_start_date,
    award_search.period_of_performance_current_end_date AS period_of_performance_current_end_date,
    latest_transaction.awarding_agency_code AS awarding_agency_code,
    latest_transaction.awarding_toptier_agency_name AS awarding_agency_name,
    latest_transaction.awarding_sub_tier_agency_c AS awarding_sub_agency_code,
    latest_transaction.awarding_subtier_agency_name AS awarding_sub_agency_name,
    latest_transaction.awarding_office_code AS awarding_office_code,
    latest_transaction.awarding_office_name AS awarding_office_name,
    latest_transaction.funding_agency_code AS funding_agency_code,
    latest_transaction.funding_toptier_agency_name AS funding_agency_name,
    latest_transaction.funding_sub_tier_agency_co AS funding_sub_agency_code,
    latest_transaction.funding_subtier_agency_name AS funding_sub_agency_name,
    latest_transaction.funding_office_code AS funding_office_code,
    latest_transaction.funding_office_name AS funding_office_name,
    (SELECT CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(U2.tas_rendering_label))) AS value FROM rpt.award_search U0 LEFT OUTER JOIN int.financial_accounts_by_awards U1 ON (U0.award_id = U1.award_id) LEFT OUTER JOIN global_temp.treasury_appropriation_account U2 ON (U1.treasury_account_id = U2.treasury_account_identifier) WHERE U0.award_id = (award_search.award_id) GROUP BY U0.award_id) AS treasury_accounts_funding_this_award,
    (SELECT CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(U3.federal_account_code))) AS value FROM rpt.award_search U0 LEFT OUTER JOIN int.financial_accounts_by_awards U1 ON (U0.award_id = U1.award_id) LEFT OUTER JOIN global_temp.treasury_appropriation_account U2 ON (U1.treasury_account_id = U2.treasury_account_identifier) LEFT OUTER JOIN global_temp.federal_account U3 ON (U2.federal_account_id = U3.id) WHERE U0.award_id = (award_search.award_id) GROUP BY U0.award_id) AS federal_accounts_funding_this_award,
    (SELECT CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(CONCAT(U2.object_class, ':', U2.object_class_name)))) FROM rpt.award_search U0 LEFT OUTER JOIN int.financial_accounts_by_awards U1 ON (U0.award_id = U1.award_id) INNER JOIN global_temp.object_class U2 ON (U1.object_class_id = U2.id) WHERE U0.award_id = (award_search.award_id) and U1.object_class_id IS NOT NULL GROUP BY U0.award_id) AS object_classes_funding_this_award,
    (SELECT CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(CONCAT(U2.program_activity_code, ':', U2.program_activity_name)))) FROM rpt.award_search U0 LEFT OUTER JOIN int.financial_accounts_by_awards U1 ON (U0.award_id = U1.award_id) INNER JOIN global_temp.ref_program_activity U2 ON (U1.program_activity_id = U2.id) WHERE U0.award_id = (award_search.award_id) and U1.program_activity_id IS NOT NULL GROUP BY U0.award_id) AS program_activities_funding_this_award,
    latest_transaction.recipient_unique_id AS recipient_duns,
    latest_transaction.recipient_uei AS recipient_uei,
    latest_transaction.recipient_name_raw AS recipient_name,
    latest_transaction.parent_recipient_unique_id AS recipient_parent_duns,
    latest_transaction.parent_uei AS recipient_parent_uei,
    latest_transaction.parent_recipient_name AS recipient_parent_name,
    latest_transaction.recipient_location_country_code AS recipient_country_code,
    latest_transaction.recipient_location_country_name AS recipient_country_name,
    latest_transaction.legal_entity_address_line1 AS recipient_address_line_1,
    latest_transaction.legal_entity_address_line2 AS recipient_address_line_2,
    latest_transaction.legal_entity_city_code AS recipient_city_code,
    latest_transaction.recipient_location_city_name AS recipient_city_name,
    latest_transaction.recipient_location_county_code AS recipient_county_code,
    latest_transaction.recipient_location_county_name AS recipient_county_name,
    latest_transaction.recipient_location_state_code AS recipient_state_code,
    latest_transaction.recipient_location_state_name AS recipient_state_name,
    latest_transaction.recipient_location_zip5 AS recipient_zip_code,
    latest_transaction.legal_entity_zip_last4 AS recipient_zip_last_4_code,
    CASE
        WHEN latest_transaction.recipient_location_state_code IS NOT NULL
            AND latest_transaction.recipient_location_congressional_code IS NOT NULL
            AND latest_transaction.recipient_location_state_code != ''
        THEN CONCAT(latest_transaction.recipient_location_state_code, '-', latest_transaction.recipient_location_congressional_code)
        ELSE latest_transaction.recipient_location_congressional_code
    END AS prime_award_summary_recipient_cd_original,
    CASE
        WHEN latest_transaction.recipient_location_state_code IS NOT NULL
            AND latest_transaction.recipient_location_congressional_code_current IS NOT NULL
            AND latest_transaction.recipient_location_state_code != ''
        THEN CONCAT(latest_transaction.recipient_location_state_code, '-', latest_transaction.recipient_location_congressional_code_current)
        ELSE latest_transaction.recipient_location_congressional_code_current
    END AS prime_award_summary_recipient_cd_current,
    latest_transaction.legal_entity_foreign_city AS recipient_foreign_city_name,
    latest_transaction.legal_entity_foreign_provi AS recipient_foreign_province_name,
    latest_transaction.legal_entity_foreign_posta AS recipient_foreign_postal_code,
    latest_transaction.place_of_performance_scope AS primary_place_of_performance_scope,
    latest_transaction.pop_country_code AS primary_place_of_performance_country_code,
    latest_transaction.pop_country_name AS primary_place_of_performance_country_name,
    latest_transaction.place_of_performance_code AS primary_place_of_performance_code,
    latest_transaction.pop_city_name AS primary_place_of_performance_city_name,
    latest_transaction.pop_county_code AS primary_place_of_performance_county_code,
    latest_transaction.pop_county_name AS primary_place_of_performance_county_name,
    latest_transaction.pop_state_name AS primary_place_of_performance_state_name,
    latest_transaction.place_of_performance_zip4a AS primary_place_of_performance_zip_4,
    CASE
        WHEN latest_transaction.pop_state_code IS NOT NULL
            AND latest_transaction.pop_congressional_code IS NOT NULL
            AND latest_transaction.pop_state_code != ''
        THEN CONCAT(latest_transaction.pop_state_code, '-', latest_transaction.pop_congressional_code)
        ELSE latest_transaction.pop_congressional_code
    END AS prime_award_summary_place_of_performance_cd_original,
    CASE
        WHEN latest_transaction.pop_state_code IS NOT NULL
            AND latest_transaction.pop_congressional_code_current IS NOT NULL
            AND latest_transaction.pop_state_code != ''
        THEN CONCAT(latest_transaction.pop_state_code, '-', latest_transaction.pop_congressional_code_current)
        ELSE latest_transaction.pop_congressional_code_current
    END AS prime_award_summary_place_of_performance_cd_current,
    latest_transaction.place_of_performance_forei AS primary_place_of_performance_foreign_location,
    ARRAY_JOIN(TRANSFORM(award_search.cfdas, row -> concat(get_json_object(row, '$.cfda_number'), ': ', get_json_object(row, '$.cfda_program_title'))), '; ') AS cfda_numbers_and_titles,
    latest_transaction.type AS assistance_type_code,
    latest_transaction.type_description AS assistance_type_description,
    award_search.description AS prime_award_base_transaction_description,
    latest_transaction.business_funds_indicator AS business_funds_indicator_code,
    latest_transaction.business_funds_ind_desc AS business_funds_indicator_description,
    latest_transaction.business_types AS business_types_code,
    latest_transaction.business_types_desc AS business_types_description,
    latest_transaction.record_type AS record_type_code,
    latest_transaction.record_type_description AS record_type_description,
    award_search.officer_1_name AS highly_compensated_officer_1_name,
    award_search.officer_1_amount AS highly_compensated_officer_1_amount,
    award_search.officer_2_name AS highly_compensated_officer_2_name,
    award_search.officer_2_amount AS highly_compensated_officer_2_amount,
    award_search.officer_3_name AS highly_compensated_officer_3_name,
    award_search.officer_3_amount AS highly_compensated_officer_3_amount,
    award_search.officer_4_name AS highly_compensated_officer_4_name,
    award_search.officer_4_amount AS highly_compensated_officer_4_amount,
    award_search.officer_5_name AS highly_compensated_officer_5_name,
    award_search.officer_5_amount AS highly_compensated_officer_5_amount,
    /*
        Use REPLACE for the asterisk (*) because it is considered a reserved character in
        the Java URLEncoder and therefore won't be replaced by the encode() function.
        This is to match the behavior of our custom urlencode() function that was written
        in PostgresSQL which did not consider the asterisk (*) to be a reserved character.
    */
    CONCAT('https://www.usaspending.gov/award/', REPLACE(REFLECT('java.net.URLEncoder','encode', award_search.generated_unique_award_id, 'UTF-8'), '*', '%2A'), '/') AS usaspending_permalink,
    latest_transaction.last_modified_date AS last_modified_date
FROM rpt.award_search
INNER JOIN rpt.transaction_search AS latest_transaction ON (latest_transaction.is_fpds = FALSE AND award_search.latest_transaction_id = latest_transaction.transaction_id)
INNER JOIN (
    SELECT
        faba.award_id,
        CONCAT_WS('; ', SORT_ARRAY(COLLECT_SET(CONCAT(disaster_emergency_fund_code, ': ', public_law)))) AS disaster_emergency_funds,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS gross_outlay_amount_by_award_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe END), 0) AS ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe END), 0) AS ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
        COALESCE(SUM(faba.transaction_obligated_amount), 0) AS transaction_obligated_amount
    FROM
        int.financial_accounts_by_awards faba
    INNER JOIN global_temp.disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    INNER JOIN global_temp.submission_attributes sa
        ON faba.submission_id = sa.submission_id
        AND sa.reporting_period_start >= '2020-04-01'
    INNER JOIN global_temp.dabs_submission_window_schedule ON (
        sa.submission_window_id = global_temp.dabs_submission_window_schedule.id
        AND global_temp.dabs_submission_window_schedule.submission_reveal_date <= now()
    )
    WHERE faba.award_id IS NOT NULL
    GROUP BY
        faba.award_id
    HAVING
        COALESCE(
            SUM(
                CASE
                    WHEN sa.is_final_balances_for_fy = TRUE
                    THEN
                        COALESCE(faba.gross_outlay_amount_by_award_cpe, 0)
                        + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                        + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                END
            ),
            0
        ) != 0
        OR COALESCE(SUM(faba.transaction_obligated_amount), 0) != 0
) COVID_DEFC ON (COVID_DEFC.award_id = award_search.award_id)
LEFT OUTER JOIN (
    SELECT
        faba.award_id,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS gross_outlay_amount_by_award_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe END), 0) AS ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe END), 0) AS ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
        COALESCE(SUM(faba.transaction_obligated_amount), 0) AS transaction_obligated_amount
    FROM
        int.financial_accounts_by_awards faba
    INNER JOIN global_temp.disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'infrastructure'
    INNER JOIN global_temp.submission_attributes sa
        ON faba.submission_id = sa.submission_id
        AND sa.reporting_period_start >= '2021-11-15'
    INNER JOIN global_temp.dabs_submission_window_schedule ON (
        sa.submission_window_id = global_temp.dabs_submission_window_schedule.id
        AND global_temp.dabs_submission_window_schedule.submission_reveal_date <= now()
    )
    WHERE faba.award_id IS NOT NULL
    GROUP BY
        faba.award_id
    HAVING
        COALESCE(
            SUM(
                CASE
                    WHEN sa.is_final_balances_for_fy = TRUE
                    THEN
                        COALESCE(faba.gross_outlay_amount_by_award_cpe, 0)
                        + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                        + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                END
            ),
            0
        ) != 0
        OR COALESCE(SUM(faba.transaction_obligated_amount), 0) != 0
) IIJA_DEFC
ON IIJA_DEFC.award_id = rpt.award_search.award_id
"""
