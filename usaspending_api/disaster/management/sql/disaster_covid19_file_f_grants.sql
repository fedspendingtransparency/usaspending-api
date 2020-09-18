SELECT
    "broker_subaward"."unique_award_key" AS "prime_award_unique_key",
    "broker_subaward"."award_id" AS "prime_award_fain",
    "broker_subaward"."award_amount" AS "prime_award_amount",
    DEFC."disaster_emergency_funds" AS "prime_award_disaster_emergency_fund_codes",
    DEFC."gross_outlay_amount_by_award_cpe" AS "prime_award_outlayed_amount_funded_by_COVID-19_supplementals",
    DEFC."transaction_obligated_amount" AS "prime_award_obligated_amount_funded_by_COVID-19_supplementals",
    "broker_subaward"."action_date" AS "prime_award_base_action_date",
    EXTRACT (YEAR FROM ("awards"."date_signed") + INTERVAL '3 months') AS "prime_award_base_action_date_fiscal_year",
    "awards"."certified_date" AS "prime_award_latest_action_date",
    EXTRACT (YEAR FROM ("awards"."certified_date"::DATE) + INTERVAL '3 months') AS "prime_award_latest_action_date_fiscal_year",
    "awards"."period_of_performance_start_date" AS "prime_award_period_of_performance_start_date",
    "awards"."period_of_performance_current_end_date" AS "prime_award_period_of_performance_current_end_date",
    "broker_subaward"."awarding_agency_code" AS "prime_award_awarding_agency_code",
    "broker_subaward"."awarding_agency_name" AS "prime_award_awarding_agency_name",
    "broker_subaward"."awarding_sub_tier_agency_c" AS "prime_award_awarding_sub_agency_code",
    "broker_subaward"."awarding_sub_tier_agency_n" AS "prime_award_awarding_sub_agency_name",
    "broker_subaward"."awarding_office_code" AS "prime_award_awarding_office_code",
    "broker_subaward"."awarding_office_name" AS "prime_award_awarding_office_name",
    "broker_subaward"."funding_agency_code" AS "prime_award_funding_agency_code",
    "broker_subaward"."funding_agency_name" AS "prime_award_funding_agency_name",
    "broker_subaward"."funding_sub_tier_agency_co" AS "prime_award_funding_sub_agency_code",
    "broker_subaward"."funding_sub_tier_agency_na" AS "prime_award_funding_sub_agency_name",
    "broker_subaward"."funding_office_code" AS "prime_award_funding_office_code",
    "broker_subaward"."funding_office_name" AS "prime_award_funding_office_name",
    (SELECT STRING_AGG (DISTINCT U2."tas_rendering_label", ';') AS "value" FROM "awards" U0 LEFT OUTER JOIN "financial_accounts_by_awards" U1 ON (U0."id" = U1."award_id") LEFT OUTER JOIN "treasury_appropriation_account" U2 ON (U1."treasury_account_id" = U2."treasury_account_identifier") WHERE U0."id" = ("subaward"."award_id") GROUP BY U0."id") AS "prime_award_treasury_accounts_funding_this_award",
    (SELECT STRING_AGG (DISTINCT U3."federal_account_code", ';') AS "value" FROM "awards" U0 LEFT OUTER JOIN "financial_accounts_by_awards" U1 ON (U0."id" = U1."award_id") LEFT OUTER JOIN "treasury_appropriation_account" U2 ON (U1."treasury_account_id" = U2."treasury_account_identifier") LEFT OUTER JOIN "federal_account" U3 ON (U2."federal_account_id" = U3."id") WHERE U0."id" = ("subaward"."award_id") GROUP BY U0."id") AS "prime_award_federal_accounts_funding_this_award",
    (SELECT STRING_AGG(DISTINCT CONCAT(U2."object_class", ':', U2.object_class_name), ';') FROM "awards" U0 LEFT OUTER JOIN "financial_accounts_by_awards" U1 ON (U0. "id" = U1. "award_id") INNER JOIN "object_class" U2 ON (U1. "object_class_id" = U2. "id") WHERE U0. "id" = ("subaward"."award_id") and U1.object_class_id is not null GROUP BY U0. "id") AS "prime_award_object_classes_funding_this_award",
    (SELECT STRING_AGG(DISTINCT CONCAT(U2."program_activity_code", ':', U2.program_activity_name), ';') FROM "awards" U0 LEFT OUTER JOIN "financial_accounts_by_awards" U1 ON (U0. "id" = U1. "award_id") INNER JOIN "ref_program_activity" U2 ON (U1. "program_activity_id" = U2. "id") WHERE U0. "id" = ("subaward"."award_id") and U1.program_activity_id is not null GROUP BY U0. "id") AS "prime_award_program_activities_funding_this_award",
    "broker_subaward"."awardee_or_recipient_uniqu" AS "prime_awardee_duns",
    "broker_subaward"."awardee_or_recipient_legal" AS "prime_awardee_name",
    "broker_subaward"."dba_name" AS "prime_awardee_dba_name",
    "broker_subaward"."ultimate_parent_unique_ide" AS "prime_awardee_parent_duns",
    "broker_subaward"."ultimate_parent_legal_enti" AS "prime_awardee_parent_name",
    "broker_subaward"."legal_entity_country_code" AS "prime_awardee_country_code",
    "broker_subaward"."legal_entity_country_name" AS "prime_awardee_country_name",
    "broker_subaward"."legal_entity_address_line1" AS "prime_awardee_address_line_1",
    "broker_subaward"."legal_entity_city_name" AS "prime_awardee_city_name",
    "transaction_fabs"."legal_entity_county_name" AS "prime_awardee_county_name",
    "broker_subaward"."legal_entity_state_code" AS "prime_awardee_state_code",
    "broker_subaward"."legal_entity_state_name" AS "prime_awardee_state_name",
    "broker_subaward"."legal_entity_zip" AS "prime_awardee_zip_code",
    "broker_subaward"."legal_entity_congressional" AS "prime_awardee_congressional_district",
    "broker_subaward"."legal_entity_foreign_posta" AS "prime_awardee_foreign_postal_code",
    "broker_subaward"."business_types" AS "prime_awardee_business_types",
    "subaward"."place_of_perform_scope" AS "prime_award_primary_place_of_performance_scope",
    "broker_subaward"."place_of_perform_city_name" AS "prime_award_primary_place_of_performance_city_name",
    "broker_subaward"."place_of_perform_state_code" AS "prime_award_primary_place_of_performance_state_code",
    "broker_subaward"."place_of_perform_state_name" AS "prime_award_primary_place_of_performance_state_name",
    "broker_subaward"."place_of_performance_zip" AS "prime_award_primary_place_of_performance_address_zip_code",
    "broker_subaward"."place_of_perform_congressio" AS "prime_award_primary_place_of_performance_congressional_district",
    "broker_subaward"."place_of_perform_country_co" AS "prime_award_primary_place_of_performance_country_code",
    "broker_subaward"."place_of_perform_country_na" AS "prime_award_primary_place_of_performance_country_name",
    "broker_subaward"."award_description" AS "prime_award_description",
    "broker_subaward"."cfda_numbers" AS "prime_award_cfda_number",
    "broker_subaward"."cfda_titles" AS "prime_award_cfda_title",
    "broker_subaward"."subaward_type" AS "subaward_type",
    "broker_subaward"."internal_id" AS "subaward_fsrs_report_id",
    "broker_subaward"."subaward_report_year" AS "subaward_fsrs_report_year",
    "broker_subaward"."subaward_report_month" AS "subaward_fsrs_report_month",
    "broker_subaward"."subaward_number" AS "subaward_number",
    "broker_subaward"."subaward_amount" AS "subaward_amount",
    "broker_subaward"."sub_action_date" AS "subaward_action_date",
    EXTRACT(YEAR FROM ("subaward"."action_date") + INTERVAL '3 months') AS "subaward_action_date_fiscal_year",
    "broker_subaward"."sub_awardee_or_recipient_uniqu" AS "subawardee_duns",
    "broker_subaward"."sub_awardee_or_recipient_legal" AS "subawardee_name",
    "broker_subaward"."sub_dba_name" AS "subawardee_dba_name",
    "broker_subaward"."sub_ultimate_parent_unique_ide" AS "subawardee_parent_duns",
    "broker_subaward"."sub_ultimate_parent_legal_enti" AS "subawardee_parent_name",
    "broker_subaward"."sub_legal_entity_country_code" AS "subawardee_country_code",
    "broker_subaward"."sub_legal_entity_country_name" AS "subawardee_country_name",
    "broker_subaward"."sub_legal_entity_address_line1" AS "subawardee_address_line_1",
    "broker_subaward"."sub_legal_entity_city_name" AS "subawardee_city_name",
    "broker_subaward"."sub_legal_entity_state_code" AS "subawardee_state_code",
    "broker_subaward"."sub_legal_entity_state_name" AS "subawardee_state_name",
    "broker_subaward"."sub_legal_entity_zip" AS "subawardee_zip_code",
    "broker_subaward"."sub_legal_entity_congressional" AS "subawardee_congressional_district",
    "broker_subaward"."sub_legal_entity_foreign_posta" AS "subawardee_foreign_postal_code",
    "broker_subaward"."sub_business_types" AS "subawardee_business_types",
    "broker_subaward"."place_of_perform_street" AS "subaward_primary_place_of_performance_address_line_1",
    "broker_subaward"."sub_place_of_perform_city_name" AS "subaward_primary_place_of_performance_city_name",
    "broker_subaward"."sub_place_of_perform_state_code" AS "subaward_primary_place_of_performance_state_code",
    "broker_subaward"."sub_place_of_perform_state_name" AS "subaward_primary_place_of_performance_state_name",
    "broker_subaward"."sub_place_of_performance_zip" AS "subaward_primary_place_of_performance_address_zip_code",
    "broker_subaward"."sub_place_of_perform_congressio" AS "subaward_primary_place_of_performance_congressional_district",
    "broker_subaward"."sub_place_of_perform_country_co" AS "subaward_primary_place_of_performance_country_code",
    "broker_subaward"."sub_place_of_perform_country_na" AS "subaward_primary_place_of_performance_country_name",
    "broker_subaward"."subaward_description" AS "subaward_description",
    "broker_subaward"."sub_high_comp_officer1_full_na" AS "subawardee_highly_compensated_officer_1_name",
    "broker_subaward"."sub_high_comp_officer1_amount" AS "subawardee_highly_compensated_officer_1_amount",
    "broker_subaward"."sub_high_comp_officer2_full_na" AS "subawardee_highly_compensated_officer_2_name",
    "broker_subaward"."sub_high_comp_officer2_amount" AS "subawardee_highly_compensated_officer_2_amount",
    "broker_subaward"."sub_high_comp_officer3_full_na" AS "subawardee_highly_compensated_officer_3_name",
    "broker_subaward"."sub_high_comp_officer3_amount" AS "subawardee_highly_compensated_officer_3_amount",
    "broker_subaward"."sub_high_comp_officer4_full_na" AS "subawardee_highly_compensated_officer_4_name",
    "broker_subaward"."sub_high_comp_officer4_amount" AS "subawardee_highly_compensated_officer_4_amount",
    "broker_subaward"."sub_high_comp_officer5_full_na" AS "subawardee_highly_compensated_officer_5_name",
    "broker_subaward"."sub_high_comp_officer5_amount" AS "subawardee_highly_compensated_officer_5_amount",
    CONCAT('https://www.usaspending.gov/#/award/', urlencode("awards"."generated_unique_award_id"), '/') AS "usaspending_permalink",
    "broker_subaward"."date_submitted" AS "subaward_fsrs_report_last_modified_date"
FROM "subaward"
INNER JOIN "awards" ON ("subaward"."award_id" = "awards"."id")
INNER JOIN "transaction_fabs" ON ("awards"."latest_transaction_id" = "transaction_fabs"."transaction_id")
INNER JOIN "broker_subaward" ON ("subaward"."id" = "broker_subaward"."id")
INNER JOIN (
    SELECT
        faba.award_id,
        STRING_AGG(DISTINCT CONCAT(disaster_emergency_fund_code, ': ', public_law), '; ' ORDER BY CONCAT(disaster_emergency_fund_code, ': ', public_law)) AS disaster_emergency_funds,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS gross_outlay_amount_by_award_cpe,
        COALESCE(SUM(faba.transaction_obligated_amount), 0) AS transaction_obligated_amount
    FROM
        financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc ON (
        defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    )
    INNER JOIN submission_attributes sa ON (
        faba.submission_id = sa.submission_id
        AND sa.reporting_period_start >= '2020-04-01'
    )
    INNER JOIN dabs_submission_window_schedule ON (
        sa."submission_window_id" = dabs_submission_window_schedule."id"
        AND dabs_submission_window_schedule."submission_reveal_date" <= now()
    )
    WHERE faba.award_id IS NOT NULL
    GROUP BY
        faba.award_id
    HAVING
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) != 0
        OR COALESCE(SUM(faba.transaction_obligated_amount), 0) != 0
) DEFC ON (DEFC.award_id = awards.id)
WHERE (
    "subaward"."award_type" IN ('grant')
    AND "subaward"."action_date" >= '2020-04-01'
)
