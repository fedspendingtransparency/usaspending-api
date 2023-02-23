SELECT
    "financial_accounts_by_awards"."reporting_period_end" AS "submission_period",
    "treasury_appropriation_account"."allocation_transfer_agency_id" AS "allocation_transfer_agency_identifier",
    "treasury_appropriation_account"."agency_id" AS "agency_identifier",
    "treasury_appropriation_account"."beginning_period_of_availability" AS "beginning_period_of_availability",
    "treasury_appropriation_account"."ending_period_of_availability" AS "ending_period_of_availability",
    "treasury_appropriation_account"."availability_type_code" AS "availability_type_code",
    "treasury_appropriation_account"."main_account_code" AS "main_account_code",
    "treasury_appropriation_account"."sub_account_code" AS "sub_account_code",
    CONCAT("treasury_appropriation_account"."agency_id",
        CONCAT('-',
            CONCAT(CASE
                WHEN "treasury_appropriation_account"."availability_type_code" = 'X' THEN 'X'
                ELSE CONCAT("treasury_appropriation_account"."beginning_period_of_availability", CONCAT('/', "treasury_appropriation_account"."ending_period_of_availability"))
                END,
                CONCAT('-',
                    CONCAT("treasury_appropriation_account"."main_account_code",
                        CONCAT('-', "treasury_appropriation_account"."sub_account_code")
                    )
                )
            )
        )
    )
    AS "treasury_account_symbol",
    (SELECT U0."name" FROM "toptier_agency" U0 WHERE U0."toptier_code" = ( "treasury_appropriation_account"."agency_id") LIMIT 1)
      AS "agency_name",
    (SELECT U0. "name" FROM "toptier_agency" U0 WHERE U0."toptier_code" = ( "treasury_appropriation_account"."allocation_transfer_agency_id") LIMIT 1)
      AS "allocation_transfer_agency_name",
    "treasury_appropriation_account"."budget_function_title" AS "budget_function",
    "treasury_appropriation_account"."budget_subfunction_title" AS "budget_subfunction",
    CONCAT ("federal_account"."agency_identifier", CONCAT ('-', "federal_account"."main_account_code"))
      AS "federal_account_symbol",
    "federal_account"."account_title" AS "federal_account_name",
    "ref_program_activity"."program_activity_code" AS "program_activity_code",
    "ref_program_activity"."program_activity_name" AS "program_activity_name",
    "object_class"."object_class" AS "object_class_code",
    "object_class"."object_class_name" AS "object_class_name",
    "object_class"."direct_reimbursable" AS "direct_or_reimbursable_funding_source",
    "financial_accounts_by_awards"."piid" AS "piid",
    "financial_accounts_by_awards"."parent_award_id" AS "parent_award_piid",
    "financial_accounts_by_awards"."fain" AS "fain",
    "financial_accounts_by_awards"."uri" AS "uri",
    "financial_accounts_by_awards"."transaction_obligated_amount",

    "awards"."total_obligation" AS "obligated_amount", -- awards.total_obligation AS total_dollars_obligated,
    -- "awards"."potential_total_value_of_award",
    "awards"."total_funding_amount",
    "awards"."total_subsidy_cost",
    "awards"."total_loan_value",
    "awards"."period_of_performance_start_date",
    "awards"."period_of_performance_current_end_date",
    "subtier_agency"."subtier_code" AS "awarding_subagency_code",
    "subtier_agency"."name" AS "awarding_subagency_name",
    "awards"."type" AS "award_type_code",
    "awards"."type_description" AS "award_type",

    "transaction_fabs"."original_loan_subsidy_cost" AS "original_subsidy_cost",
    "transaction_fabs"."sai_number" AS "sai_number",
    "transaction_fabs"."non_federal_funding_amount" AS "non_federal_funding_amount",
    "transaction_fabs"."face_value_loan_guarantee" AS "face_value_of_loan",
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
    "transaction_fabs"."award_description" AS "award_description",
    "transaction_fabs"."business_funds_indicator" AS "business_funds_indicator_code",
    "transaction_fabs"."business_funds_ind_desc" AS "business_funds_indicator_description",
    "transaction_fabs"."business_types" AS "business_types_code",
    "transaction_fabs"."business_types_desc" AS "business_types_description",
    "transaction_fabs"."record_type" AS "record_type_code",
    "transaction_fabs"."record_type_description" AS "record_type_description",
    "transaction_fabs"."modified_at" AS "last_modified_date"
    FROM
    "financial_accounts_by_awards"
    INNER JOIN
    "treasury_appropriation_account"
        ON (
            "financial_accounts_by_awards"."treasury_account_id" = "treasury_appropriation_account"."treasury_account_identifier"
       )
    LEFT OUTER JOIN
    "federal_account"
        ON (
            "treasury_appropriation_account"."federal_account_id" = "federal_account"."id"
       )
    LEFT OUTER JOIN
    "awards"
        ON (
            "financial_accounts_by_awards"."award_id" = "awards"."id"
       )
    INNER JOIN
    "vw_transaction_fabs" AS "transaction_fabs"
        ON (
            "awards"."latest_transaction_id" = "transaction_fabs"."transaction_id"
       )
    LEFT OUTER JOIN
    "ref_program_activity"
        ON (
            "financial_accounts_by_awards"."program_activity_id" = "ref_program_activity"."id"
       )
    LEFT OUTER JOIN
    "object_class"
        ON (
            "financial_accounts_by_awards"."object_class_id" = "object_class"."id"
       )
    LEFT OUTER JOIN
    "agency"
        ON (
            "awards"."awarding_agency_id" = "agency"."id"
       )
    LEFT OUTER JOIN
    "toptier_agency"
        ON (
            "agency"."toptier_agency_id" = "toptier_agency"."toptier_agency_id"
       )
    LEFT OUTER JOIN
    "subtier_agency"
        ON (
            "agency"."subtier_agency_id" = "subtier_agency"."subtier_agency_id"
       )
    WHERE
    (
        (
            treasury_appropriation_account.allocation_transfer_agency_id {ATA} AND
            treasury_appropriation_account.agency_id {AID} AND
            treasury_appropriation_account.availability_type_code {AvailType Code} AND
            treasury_appropriation_account.beginning_period_of_availability {BPOA} AND
            treasury_appropriation_account.ending_period_of_availability {EPOA} AND
            treasury_appropriation_account.main_account_code {Main} AND
            treasury_appropriation_account.sub_account_code {Sub}
        )
    )
;
