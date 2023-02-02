/* NOTE: Comments must be in this form. This SQL gets flattened to 1-line for psql \copy */
SELECT
    award_search.generated_unique_award_id AS assistance_award_unique_key,
    award_search.fain AS award_id_fain,
    award_search.uri AS award_id_uri,
    latest_transaction.sai_number AS sai_number,
    covid_faba_assistance_awards_agg.disaster_emergency_fund_codes AS disaster_emergency_fund_codes,
    (
        covid_faba_assistance_awards_agg.gross_outlay_amount_by_award_cpe
        + covid_faba_assistance_awards_agg.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe
        + covid_faba_assistance_awards_agg.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe
    ) AS "outlayed_amount_funded_by_COVID-19_supplementals",
    covid_faba_assistance_awards_agg.transaction_obligated_amount AS "obligated_amount_funded_by_COVID-19_supplementals",
    award_search.total_obligation AS total_obligated_amount,
    award_search.non_federal_funding_amount AS total_non_federal_funding_amount,
    award_search.total_funding_amount AS total_funding_amount,
    award_search.total_loan_value AS total_face_value_of_loan,
    award_search.total_subsidy_cost AS total_loan_subsidy_cost,
    award_search.date_signed AS award_base_action_date,
    EXTRACT (YEAR FROM (award_search.date_signed) + INTERVAL '3 months') AS award_base_action_date_fiscal_year,
    latest_transaction.action_date AS award_latest_action_date,
    EXTRACT (YEAR FROM (latest_transaction.action_date::DATE) + INTERVAL '3 months') AS award_latest_action_date_fiscal_year,
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
    faba_assistance_awards_agg.treasury_accounts_funding_this_award AS treasury_accounts_funding_this_award,
    faba_assistance_awards_agg.federal_accounts_funding_this_award AS federal_accounts_funding_this_award,
    faba_assistance_awards_agg.object_classes_funding_this_award AS object_classes_funding_this_award,
    faba_assistance_awards_agg.program_activities_funding_this_award AS program_activities_funding_this_award,
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
    latest_transaction.recipient_location_congressional_code AS recipient_congressional_district,
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
    latest_transaction.pop_congressional_code AS primary_place_of_performance_congressional_district,
    latest_transaction.place_of_performance_forei AS primary_place_of_performance_foreign_location,
    cfda_numbers_and_titles_agg.cfda_numbers_and_titles AS cfda_numbers_and_titles,
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
    CONCAT('https://www.usaspending.gov/award/', urlencode(award_search.generated_unique_award_id), '/') AS usaspending_permalink,
    latest_transaction.last_modified_date::TIMESTAMP WITH TIME ZONE AS last_modified_date
FROM rpt.award_search /* constrained with is_fpds = FALSE in the covid_faba_assistance_awards_agg subquery that it's INNER JOINed to */
INNER JOIN rpt.transaction_search AS latest_transaction ON (award_search.latest_transaction_id = latest_transaction.transaction_id)
INNER JOIN (
    SELECT
        awd.award_id,
        STRING_AGG(DISTINCT CONCAT(defc.code, ': ', defc.public_law), '; ' ORDER BY CONCAT(defc.code, ': ', defc.public_law)) AS disaster_emergency_fund_codes,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS gross_outlay_amount_by_award_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe END), 0) AS ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe END), 0) AS ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
        COALESCE(SUM(faba.transaction_obligated_amount), 0) AS transaction_obligated_amount
    FROM financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc
        ON defc.group_name = 'covid_19'
        AND defc.code = faba.disaster_emergency_fund_code
    INNER JOIN submission_attributes sa
        ON sa.reporting_period_start >= '2020-04-01'
        AND faba.submission_id = sa.submission_id
    INNER JOIN dabs_submission_window_schedule
        ON dabs_submission_window_schedule.submission_reveal_date <= now()
        AND sa.submission_window_id = dabs_submission_window_schedule.id
    INNER JOIN rpt.award_search awd
        ON awd.is_fpds = FALSE
        AND faba.award_id = awd.award_id
    GROUP BY
        awd.award_id
    HAVING
        SUM(
            CASE
                WHEN sa.is_final_balances_for_fy = TRUE
                THEN
                    COALESCE(faba.gross_outlay_amount_by_award_cpe, 0)
                    + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                    + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                ELSE 0
            END
        ) != 0
        OR COALESCE(SUM(faba.transaction_obligated_amount), 0) != 0
) AS covid_faba_assistance_awards_agg
    ON award_search.award_id = covid_faba_assistance_awards_agg.award_id,
LATERAL (
    SELECT
        awd.award_id,
        STRING_AGG (DISTINCT tas.tas_rendering_label, ';') AS treasury_accounts_funding_this_award,
        STRING_AGG (DISTINCT fa.federal_account_code, ';') AS federal_accounts_funding_this_award,
        STRING_AGG(DISTINCT CONCAT(oc.object_class, ':', oc.object_class_name), ';') FILTER (WHERE oc.id IS NOT NULL) AS object_classes_funding_this_award,
        STRING_AGG(DISTINCT CONCAT(pa.program_activity_code, ':', pa.program_activity_name), ';') FILTER (WHERE pa.id IS NOT NULL) AS program_activities_funding_this_award
    FROM rpt.award_search awd
    INNER JOIN financial_accounts_by_awards faba
        ON awd.award_id = faba.award_id
    LEFT JOIN treasury_appropriation_account tas
        ON faba.treasury_account_id = tas.treasury_account_identifier
    LEFT JOIN federal_account fa
        ON tas.federal_account_id = fa.id
    LEFT JOIN object_class oc
        ON faba.object_class_id = oc.id
    LEFT JOIN ref_program_activity pa
        ON faba.program_activity_id = pa.id
    /* JOIN back to the filtered down award_search (filtered by its INNER JOIN to the covid_faba_assistance_awards_agg subquery)
       to narrow the scope of how many award-groups this subquery has to make the aggs do less work */
    WHERE
        awd.award_id = award_search.award_id
    GROUP BY
        awd.award_id
) AS faba_assistance_awards_agg,
LATERAL (
    SELECT
        awd.award_id,
        STRING_AGG(DISTINCT CONCAT(ts.cfda_number, ': ', ts.cfda_title), '; ' ORDER BY CONCAT(ts.cfda_number, ': ', ts.cfda_title)) AS cfda_numbers_and_titles
    FROM rpt.award_search awd
    INNER JOIN rpt.transaction_search ts
        ON awd.award_id = ts.award_id
    /* JOIN back to the filtered down award_search (filtered by its INNER JOIN to the covid_faba_assistance_awards_agg subquery)
       to narrow the scope of how many award-groups this subquery has to make the aggs do less work */
    WHERE
        awd.award_id = award_search.award_id
    GROUP BY
        awd.award_id
) AS cfda_numbers_and_titles_agg
