TREASURY_ACCOUNT_DOWNLOAD_COLUMNS = {
    "financial_accounts_by_awards_id",
    "submission_id",
    "owning_agency_name",
    "reporting_agency_name",
    "submission_period",
    "allocation_transfer_agency_identifier_code",
    "agency_identifier_code",
    "beginning_period_of_availability",
    "ending_period_of_availability",
    "availability_type_code",
    "main_account_code",
    "sub_account_code",
    "treasury_account_symbol",
    "treasury_account_name",
    "agency_identifier_name",
    "allocation_transfer_agency_identifier_name",
    "budget_function",
    "budget_subfunction",
    "federal_account_symbol",
    "federal_account_name",
    "program_activity_code",
    "program_activity_name",
    "object_class_code",
    "object_class_name",
    "direct_or_reimbursable_funding_source",
    "disaster_emergency_fund_code",
    "disaster_emergency_fund_name",
    "transaction_obligated_amount",
    "gross_outlay_amount_fyb_to_period_end",
    "ussgl487200_downward_adj_prior_year_prepaid_undeliv_order_oblig",
    "ussgl497200_downward_adj_of_prior_year_paid_deliv_orders_oblig",
    "award_unique_key",
    "award_id_piid",
    "parent_award_id_piid",
    "award_id_fain",
    "award_id_uri",
    "award_base_action_date",
    "award_base_action_date_fiscal_year",
    "award_latest_action_date",
    "award_latest_action_date_fiscal_year",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "ordering_period_end_date",
    "award_type_code",
    "award_type",
    "idv_type_code",
    "idv_type",
    "prime_award_base_transaction_description",
    "awarding_agency_code",
    "awarding_agency_name",
    "awarding_subagency_code",
    "awarding_subagency_name",
    "awarding_office_code",
    "awarding_office_name",
    "funding_agency_code",
    "funding_agency_name",
    "funding_sub_agency_code",
    "funding_sub_agency_name",
    "funding_office_code",
    "funding_office_name",
    "recipient_uei",
    "recipient_duns",
    "recipient_name",
    "recipient_name_raw",
    "recipient_parent_uei",
    "recipient_parent_duns",
    "recipient_parent_name",
    "recipient_parent_name_raw",
    "recipient_country",
    "recipient_state",
    "recipient_county",
    "recipient_city",
    "prime_award_summary_recipient_cd_original",
    "prime_award_summary_recipient_cd_current",
    "recipient_zip_code",
    "primary_place_of_performance_country",
    "primary_place_of_performance_state",
    "primary_place_of_performance_county",
    "prime_award_summary_place_of_performance_cd_original",
    "prime_award_summary_place_of_performance_cd_current",
    "primary_place_of_performance_zip_code",
    "cfda_number",
    "cfda_title",
    "product_or_service_code",
    "product_or_service_code_description",
    "naics_code",
    "naics_description",
    "national_interest_action_code",
    "national_interest_action",
    "usaspending_permalink",
    "last_modified_date",
}

TREASURY_ACCOUNT_DOWNLOAD_DELTA_COLUMNS = {}

account_download_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in TREASURY_ACCOUNT_DOWNLOAD_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """


treasury_account_download_load_sql_string = rf"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} (
        {",".join(list(TREASURY_ACCOUNT_DOWNLOAD_COLUMNS))}
    )
    SELECT
        financial_accounts_by_awards.financial_accounts_by_awards_id,
        financial_accounts_by_awards.submission_id,
        toptier_agency.name AS owning_agency_name,
        submission_attributes.reporting_agency_name AS reporting_agency_name,
        CASE
            WHEN submission_attributes.quarter_format_flag = TRUE
                THEN
                    CONCAT(
                        CAST('FY' AS STRING),
                        CAST(submission_attributes.reporting_fiscal_year AS STRING),
                        CAST('Q' AS STRING),
                        CAST(
                            submission_attributes.reporting_fiscal_quarter AS STRING
                        )
                    )
            ELSE
                CONCAT(
                    CAST('FY' AS STRING),
                    CAST(submission_attributes.reporting_fiscal_year AS STRING),
                    CAST('P' AS STRING),
                    LPAD(
                        CAST(
                            submission_attributes.reporting_fiscal_period AS STRING
                        ),
                        2,
                        '0'
                    )
                )
        END AS submission_period,
        treasury_appropriation_account.allocation_transfer_agency_id AS allocation_transfer_agency_identifier_code,
        treasury_appropriation_account.agency_id AS agency_identifier_code,
        treasury_appropriation_account.beginning_period_of_availability AS beginning_period_of_availability,
        treasury_appropriation_account.ending_period_of_availability AS ending_period_of_availability,
        treasury_appropriation_account.availability_type_code AS availability_type_code,
        treasury_appropriation_account.main_account_code AS main_account_code,
        treasury_appropriation_account.sub_account_code AS sub_account_code,
        treasury_appropriation_account.tas_rendering_label AS treasury_account_symbol,
        treasury_appropriation_account.account_title AS treasury_account_name,
        CGAC_AID.agency_name AS agency_identifier_name,
        CGAC_ATA.agency_name AS allocation_transfer_agency_identifier_name,
        treasury_appropriation_account.budget_function_title AS budget_function,
        treasury_appropriation_account.budget_subfunction_title AS budget_subfunction,
        federal_account.federal_account_code AS federal_account_symbol,
        federal_account.account_title AS federal_account_name,
        ref_program_activity.program_activity_code AS program_activity_code,
        ref_program_activity.program_activity_name AS program_activity_name,
        object_class.object_class AS object_class_code,
        object_class.object_class_name AS object_class_name,
        object_class.direct_reimbursable AS direct_or_reimbursable_funding_source,
        financial_accounts_by_awards.disaster_emergency_fund_code AS disaster_emergency_fund_code,
        disaster_emergency_fund_code.title AS disaster_emergency_fund_name,
        financial_accounts_by_awards.transaction_obligated_amount AS transaction_obligated_amount,
        financial_accounts_by_awards.gross_outlay_amount_by_award_cpe AS gross_outlay_amount_fyb_to_period_end,
        financial_accounts_by_awards.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe AS ussgl487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,
        financial_accounts_by_awards.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe AS ussgl497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,
        award_search.generated_unique_award_id AS award_unique_key,
        financial_accounts_by_awards.piid AS award_id_piid,
        financial_accounts_by_awards.parent_award_id AS parent_award_id_piid,
        financial_accounts_by_awards.fain AS award_id_fain,
        financial_accounts_by_awards.uri AS award_id_uri,
        award_search.date_signed AS award_base_action_date,
        EXTRACT(YEAR from (award_search.date_signed) + INTERVAL '3 months') AS award_base_action_date_fiscal_year,
        award_search.certified_date AS award_latest_action_date,
        EXTRACT(YEAR from (award_search.certified_date) + INTERVAL '3 months') AS award_latest_action_date_fiscal_year,
        award_search.period_of_performance_start_date AS period_of_performance_start_date,
        award_search.period_of_performance_current_end_date AS period_of_performance_current_end_date,
        transaction_search.ordering_period_end_date AS ordering_period_end_date,
        COALESCE(transaction_search.contract_award_type, transaction_search.type) AS award_type_code,
        COALESCE(transaction_search.contract_award_type_desc, transaction_search.type_description) AS award_type,
        transaction_search.idv_type AS idv_type_code,
        transaction_search.idv_type_description AS idv_type,
        award_search.description AS prime_award_base_transaction_description,
        transaction_search.awarding_agency_code AS awarding_agency_code,
        transaction_search.awarding_toptier_agency_name_raw AS awarding_agency_name,
        transaction_search.awarding_sub_tier_agency_c AS awarding_subagency_code,
        transaction_search.awarding_subtier_agency_name_raw AS awarding_subagency_name,
        transaction_search.awarding_office_code AS awarding_office_code,
        transaction_search.awarding_office_name AS awarding_office_name,
        transaction_search.funding_agency_code AS funding_agency_code,
        transaction_search.funding_toptier_agency_name_raw AS funding_agency_name,
        transaction_search.funding_sub_tier_agency_co AS funding_sub_agency_code,
        transaction_search.funding_subtier_agency_name_raw AS funding_sub_agency_name,
        transaction_search.funding_office_code AS funding_office_code,
        transaction_search.funding_office_name AS funding_office_name,
        transaction_search.recipient_uei AS recipient_uei,
        transaction_search.recipient_unique_id AS recipient_duns,
        transaction_search.recipient_name AS recipient_name,
        transaction_search.recipient_name_raw AS recipient_name_raw,
        transaction_search.parent_uei AS recipient_parent_uei,
        transaction_search.parent_uei AS recipient_parent_duns,
        transaction_search.parent_recipient_name AS recipient_parent_name,
        transaction_search.parent_recipient_name_raw AS recipient_parent_name_raw,
        transaction_search.recipient_location_country_code AS recipient_country,
        transaction_search.recipient_location_state_code AS recipient_state,
        transaction_search.recipient_location_county_name AS recipient_county,
        transaction_search.recipient_location_city_name AS recipient_city,
        CASE
            WHEN (
                transaction_search.recipient_location_state_code IS NOT NULL
                AND transaction_search.recipient_location_congressional_code IS NOT NULL
                AND NOT (
                    transaction_search.recipient_location_state_code = ''
                    AND transaction_search.recipient_location_state_code IS NOT NULL
                ))
                THEN
                    CONCAT(
                        transaction_search.recipient_location_state_code, '-',
                        transaction_search.recipient_location_congressional_code
                    )
            ELSE transaction_search.recipient_location_congressional_code
        END AS prime_award_summary_recipient_cd_original,
        CASE
            WHEN (
                transaction_search.recipient_location_state_code IS NOT NULL
                AND transaction_search.recipient_location_congressional_code_current IS NOT NULL
                AND NOT (
                    transaction_search.recipient_location_state_code = ''
                    AND transaction_search.recipient_location_state_code IS NOT NULL
                ))
                THEN
                    CONCAT(
                        transaction_search.recipient_location_state_code,
                        '-',
                        transaction_search.recipient_location_congressional_code_current
                    )
            ELSE transaction_search.recipient_location_congressional_code_current
        END AS prime_award_summary_recipient_cd_current,
        COALESCE(
            transaction_search.legal_entity_zip4,
            CONCAT(
                CAST(transaction_search.recipient_location_zip5 AS STRING),
                CAST(transaction_search.legal_entity_zip_last4 AS STRING)
            )
        ) AS recipient_zip_code,
        transaction_search.pop_country_name AS primary_place_of_performance_country,
        transaction_search.pop_state_name AS primary_place_of_performance_state,
        transaction_search.pop_county_name AS primary_place_of_performance_county,
        CASE
            WHEN
                transaction_search.pop_state_code IS NOT NULL
                AND transaction_search.pop_congressional_code IS NOT NULL
                AND NOT (
                    transaction_search.pop_state_code = ''
                    AND transaction_search.pop_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.pop_state_code,
                        '-',
                        transaction_search.pop_congressional_code
                    )
            ELSE transaction_search.pop_congressional_code
        END AS prime_award_summary_place_of_performance_cd_original,
        CASE
            WHEN
                transaction_search.pop_state_code IS NOT NULL
                AND transaction_search.pop_congressional_code_current IS NOT NULL
                AND NOT (
                    transaction_search.pop_state_code = ''
                    AND transaction_search.pop_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.pop_state_code,
                        '-',
                        transaction_search.pop_congressional_code_current
                    )
            ELSE transaction_search.pop_congressional_code_current
        END AS prime_award_summary_place_of_performance_cd_current,
        transaction_search.place_of_performance_zip4a AS primary_place_of_performance_zip_code,
        transaction_search.cfda_number AS cfda_number,
        transaction_search.cfda_title AS cfda_title,
        transaction_search.product_or_service_code AS product_or_service_code,
        transaction_search.product_or_service_description AS product_or_service_code_description,
        transaction_search.naics_code AS naics_code,
        transaction_search.naics_description AS naics_description,
        transaction_search.national_interest_action AS national_interest_action_code,
        transaction_search.national_interest_desc AS national_interest_action,
        CASE
            WHEN award_search.generated_unique_award_id IS NOT NULL
                THEN CONCAT('localhost:3000/award/', URL_ENCODE(award_search.generated_unique_award_id), '/')
            ELSE '/' END AS usaspending_permalink,
        CAST(submission_attributes.published_date AS DATE) AS last_modified_date,
        submission_attributes.reporting_fiscal_period,
        submission_attributes.reporting_fiscal_quarter,
        submission_attributes.reporting_fiscal_year,
        submission_attributes.quarter_format_flag
    FROM
        raw.financial_accounts_by_awards
        INNER JOIN global_temp.submission_attributes
            ON (financial_accounts_by_awards.submission_id = submission_attributes.submission_id)
        LEFT OUTER JOIN global_temp.treasury_appropriation_account
            ON (financial_accounts_by_awards.treasury_account_id = treasury_appropriation_account.treasury_account_identifier)
        LEFT OUTER JOIN global_temp.cgac AS CGAC_AID
            ON (treasury_appropriation_account.agency_id = CGAC_AID.cgac_code)
        LEFT OUTER JOIN global_temp.cgac AS CGAC_ATA
            ON (treasury_appropriation_account.allocation_transfer_agency_id = CGAC_ATA.cgac_code)
        INNER JOIN award_search
            ON (financial_accounts_by_awards.award_id = award_search.award_id)
        INNER JOIN transaction_search
            ON (award_search.latest_transaction_search_id = transaction_search.transaction_id)        
        LEFT OUTER JOIN global_temp.toptier_agency
            ON (treasury_appropriation_account.funding_toptier_agency_id = toptier_agency.toptier_agency_id)
        LEFT OUTER JOIN global_temp.federal_account
            ON (treasury_appropriation_account.federal_account_id = federal_account.id)
        LEFT OUTER JOIN global_temp.ref_program_activity
            ON (financial_accounts_by_awards.program_activity_id = ref_program_activity.id)
        LEFT OUTER JOIN global_temp.object_class
            ON (financial_accounts_by_awards.object_class_id = object_class.id)
        LEFT OUTER JOIN global_temp.disaster_emergency_fund_code
            ON (financial_accounts_by_awards.disaster_emergency_fund_code = disaster_emergency_fund_code.code)
"""
