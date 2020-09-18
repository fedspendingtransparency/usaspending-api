SELECT
    "awards"."generated_unique_award_id" AS "assistance_award_unique_key",
    "awards"."fain" AS "award_id_fain",
    "awards"."uri" AS "award_id_uri",
    "transaction_fabs"."sai_number" AS "sai_number",
    DEFC."disaster_emergency_funds" AS "disaster_emergency_fund_codes",
    DEFC."gross_outlay_amount_by_award_cpe" AS "outlayed_amount_funded_by_COVID-19_supplementals",
    DEFC."transaction_obligated_amount" AS "obligated_amount_funded_by_COVID-19_supplementals",
    "awards"."total_obligation" AS "total_obligated_amount",
    "awards"."non_federal_funding_amount" AS "total_non_federal_funding_amount",
    "awards"."total_funding_amount" AS "total_funding_amount",
    "awards"."total_loan_value" AS "total_face_value_of_loan",
    "awards"."total_subsidy_cost" AS "total_loan_subsidy_cost",
    "awards"."date_signed" AS "award_base_action_date",
    EXTRACT (YEAR FROM ("awards"."date_signed") + INTERVAL '3 months') AS "award_base_action_date_fiscal_year",
    "transaction_fabs"."action_date" AS "award_latest_action_date",
    EXTRACT (YEAR FROM ("transaction_fabs"."action_date"::DATE) + INTERVAL '3 months') AS "award_latest_action_date_fiscal_year",
    "awards"."period_of_performance_start_date" AS "period_of_performance_start_date",
    "awards"."period_of_performance_current_end_date" AS "period_of_performance_current_end_date",
    "transaction_fabs"."awarding_agency_code" AS "awarding_agency_code",
    "transaction_fabs"."awarding_agency_name" AS "awarding_agency_name",
    "transaction_fabs"."awarding_sub_tier_agency_c" AS "awarding_sub_agency_code",
    "transaction_fabs"."awarding_sub_tier_agency_n" AS "awarding_sub_agency_name",
    "transaction_fabs"."awarding_office_code" AS "awarding_office_code",
    "transaction_fabs"."awarding_office_name" AS "awarding_office_name",
    "transaction_fabs"."funding_agency_code" AS "funding_agency_code",
    "transaction_fabs"."funding_agency_name" AS "funding_agency_name",
    "transaction_fabs"."funding_sub_tier_agency_co" AS "funding_sub_agency_code",
    "transaction_fabs"."funding_sub_tier_agency_na" AS "funding_sub_agency_name",
    "transaction_fabs"."funding_office_code" AS "funding_office_code",
    "transaction_fabs"."funding_office_name" AS "funding_office_name",
    (SELECT STRING_AGG (DISTINCT U2. "tas_rendering_label", ';') AS "value" FROM "awards" U0 LEFT OUTER JOIN "financial_accounts_by_awards" U1 ON (U0. "id" = U1. "award_id") LEFT OUTER JOIN "treasury_appropriation_account" U2 ON (U1. "treasury_account_id" = U2. "treasury_account_identifier") WHERE U0. "id" = ("awards"."id") GROUP BY U0. "id") AS "treasury_accounts_funding_this_award",
    (SELECT STRING_AGG (DISTINCT U3. "federal_account_code", ';') AS "value" FROM "awards" U0 LEFT OUTER JOIN "financial_accounts_by_awards" U1 ON (U0. "id" = U1. "award_id") LEFT OUTER JOIN "treasury_appropriation_account" U2 ON (U1. "treasury_account_id" = U2. "treasury_account_identifier") LEFT OUTER JOIN "federal_account" U3 ON (U2. "federal_account_id" = U3. "id") WHERE U0. "id" = ("awards"."id") GROUP BY U0. "id") AS "federal_accounts_funding_this_award",
    (SELECT STRING_AGG(DISTINCT CONCAT(U2."object_class", ':', U2.object_class_name), ';') FROM "awards" U0 LEFT OUTER JOIN "financial_accounts_by_awards" U1 ON (U0. "id" = U1. "award_id") INNER JOIN "object_class" U2 ON (U1. "object_class_id" = U2. "id") WHERE U0. "id" = ("awards"."id") and U1.object_class_id is not null GROUP BY U0. "id")AS "object_classes_funding_this_award",
    (SELECT STRING_AGG(DISTINCT CONCAT(U2."program_activity_code", ':', U2.program_activity_name), ';') FROM "awards" U0 LEFT OUTER JOIN "financial_accounts_by_awards" U1 ON (U0. "id" = U1. "award_id") INNER JOIN "ref_program_activity" U2 ON (U1. "program_activity_id" = U2. "id") WHERE U0. "id" = ("awards"."id") and U1.program_activity_id is not null GROUP BY U0. "id") AS "program_activities_funding_this_award",
    "transaction_fabs"."awardee_or_recipient_uniqu" AS "recipient_duns",
    "transaction_fabs"."awardee_or_recipient_legal" AS "recipient_name",
    "transaction_fabs"."ultimate_parent_unique_ide" AS "recipient_parent_duns",
    "transaction_fabs"."ultimate_parent_legal_enti" AS "recipient_parent_name",
    "transaction_fabs"."legal_entity_country_code" AS "recipient_country_code",
    "transaction_fabs"."legal_entity_country_name" AS "recipient_country_name",
    "transaction_fabs"."legal_entity_address_line1" AS "recipient_address_line_1",
    "transaction_fabs"."legal_entity_address_line2" AS "recipient_address_line_2",
    "transaction_fabs"."legal_entity_city_code" AS "recipient_city_code",
    "transaction_fabs"."legal_entity_city_name" AS "recipient_city_name",
    "transaction_fabs"."legal_entity_county_code" AS "recipient_county_code",
    "transaction_fabs"."legal_entity_county_name" AS "recipient_county_name",
    "transaction_fabs"."legal_entity_state_code" AS "recipient_state_code",
    "transaction_fabs"."legal_entity_state_name" AS "recipient_state_name",
    "transaction_fabs"."legal_entity_zip5" AS "recipient_zip_code",
    "transaction_fabs"."legal_entity_zip_last4" AS "recipient_zip_last_4_code",
    "transaction_fabs"."legal_entity_congressional" AS "recipient_congressional_district",
    "transaction_fabs"."legal_entity_foreign_city" AS "recipient_foreign_city_name",
    "transaction_fabs"."legal_entity_foreign_provi" AS "recipient_foreign_province_name",
    "transaction_fabs"."legal_entity_foreign_posta" AS "recipient_foreign_postal_code",
    "transaction_fabs"."place_of_performance_scope" AS "primary_place_of_performance_scope",
    "transaction_fabs"."place_of_perform_country_c" AS "primary_place_of_performance_country_code",
    "transaction_fabs"."place_of_perform_country_n" AS "primary_place_of_performance_country_name",
    "transaction_fabs"."place_of_performance_code" AS "primary_place_of_performance_code",
    "transaction_fabs"."place_of_performance_city" AS "primary_place_of_performance_city_name",
    "transaction_fabs"."place_of_perform_county_co" AS "primary_place_of_performance_county_code",
    "transaction_fabs"."place_of_perform_county_na" AS "primary_place_of_performance_county_name",
    "transaction_fabs"."place_of_perform_state_nam" AS "primary_place_of_performance_state_name",
    "transaction_fabs"."place_of_performance_zip4a" AS "primary_place_of_performance_zip_4",
    "transaction_fabs"."place_of_performance_congr" AS "primary_place_of_performance_congressional_district",
    "transaction_fabs"."place_of_performance_forei" AS "primary_place_of_performance_foreign_location",
    "transaction_fabs"."cfda_number" AS "cfda_number",
    "transaction_fabs"."cfda_title" AS "cfda_title",
    "transaction_fabs"."assistance_type" AS "assistance_type_code",
    "transaction_fabs"."assistance_type_desc" AS "assistance_type_description",
    "awards"."description" AS "award_description",
    "transaction_fabs"."business_funds_indicator" AS "business_funds_indicator_code",
    "transaction_fabs"."business_funds_ind_desc" AS "business_funds_indicator_description",
    "transaction_fabs"."business_types" AS "business_types_code",
    "transaction_fabs"."business_types_desc" AS "business_types_description",
    "transaction_fabs"."record_type" AS "record_type_code",
    "transaction_fabs"."record_type_description" AS "record_type_description",
    "awards"."officer_1_name" AS "highly_compensated_officer_1_name",
    "awards"."officer_1_amount" AS "highly_compensated_officer_1_amount",
    "awards"."officer_2_name" AS "highly_compensated_officer_2_name",
    "awards"."officer_2_amount" AS "highly_compensated_officer_2_amount",
    "awards"."officer_3_name" AS "highly_compensated_officer_3_name",
    "awards"."officer_3_amount" AS "highly_compensated_officer_3_amount",
    "awards"."officer_4_name" AS "highly_compensated_officer_4_name",
    "awards"."officer_4_amount" AS "highly_compensated_officer_4_amount",
    "awards"."officer_5_name" AS "highly_compensated_officer_5_name",
    "awards"."officer_5_amount" AS "highly_compensated_officer_5_amount",
    CONCAT('https://www.usaspending.gov/#/award/', urlencode("awards"."generated_unique_award_id"), '/') AS "usaspending_permalink",
    "transaction_fabs"."modified_at" AS "last_modified_date"
FROM "awards"
INNER JOIN "transaction_fabs" ON ("awards"."latest_transaction_id" = "transaction_fabs"."transaction_id")
INNER JOIN (
    SELECT
        faba.award_id,
        STRING_AGG(DISTINCT CONCAT(disaster_emergency_fund_code, ': ', public_law), '; ' ORDER BY CONCAT(disaster_emergency_fund_code, ': ', public_law)) AS disaster_emergency_funds,
        COALESCE(SUM(CASE WHEN sa.is_final_balances_for_fy = TRUE THEN faba.gross_outlay_amount_by_award_cpe END), 0) AS gross_outlay_amount_by_award_cpe,
        COALESCE(SUM(faba.transaction_obligated_amount), 0) AS transaction_obligated_amount
    FROM
        financial_accounts_by_awards faba
    INNER JOIN disaster_emergency_fund_code defc
        ON defc.code = faba.disaster_emergency_fund_code
        AND defc.group_name = 'covid_19'
    INNER JOIN submission_attributes sa
        ON faba.submission_id = sa.submission_id
        AND sa.reporting_period_start >= '2020-04-01'
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
