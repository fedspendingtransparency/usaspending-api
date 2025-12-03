from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

award_financial_schema = StructType(
    [
        StructField("financial_accounts_by_awards_id", IntegerType(), False),
        StructField("submission_id", IntegerType(), False),
        StructField("federal_owning_agency_name", StringType()),
        StructField("treasury_owning_agency_name", StringType()),
        StructField("federal_account_symbol", StringType()),
        StructField("federal_account_name", StringType()),
        StructField("agency_identifier_name", StringType()),
        StructField("allocation_transfer_agency_identifier_name", StringType()),
        StructField("program_activity_code", StringType()),
        StructField("program_activity_name", StringType()),
        StructField("object_class_code", StringType()),
        StructField("object_class_name", StringType()),
        StructField("direct_or_reimbursable_funding_source", StringType()),
        StructField("disaster_emergency_fund_code", StringType()),
        StructField("disaster_emergency_fund_name", StringType()),
        StructField("award_unique_key", StringType()),
        StructField("award_id_piid", StringType()),
        StructField("parent_award_id_piid", StringType()),
        StructField("award_id_fain", StringType()),
        StructField("award_id_uri", StringType()),
        StructField("award_base_action_date", DateType()),
        StructField("award_latest_action_date", DateType()),
        StructField("period_of_performance_start_date", DateType()),
        StructField("period_of_performance_current_end_date", DateType()),
        StructField("ordering_period_end_date", DateType()),
        StructField("idv_type_code", StringType()),
        StructField("idv_type", StringType()),
        StructField("prime_award_base_transaction_description", StringType()),
        StructField("awarding_agency_code", StringType()),
        StructField("awarding_agency_name", StringType()),
        StructField("awarding_subagency_code", StringType()),
        StructField("awarding_subagency_name", StringType()),
        StructField("awarding_office_code", StringType()),
        StructField("awarding_office_name", StringType()),
        StructField("funding_agency_code", StringType()),
        StructField("funding_agency_name", StringType()),
        StructField("funding_sub_agency_code", StringType()),
        StructField("funding_sub_agency_name", StringType()),
        StructField("funding_office_code", StringType()),
        StructField("funding_office_name", StringType()),
        StructField("recipient_uei", StringType()),
        StructField("recipient_duns", StringType()),
        StructField("recipient_name", StringType()),
        StructField("recipient_name_raw", StringType()),
        StructField("recipient_parent_uei", StringType()),
        StructField("recipient_parent_duns", StringType()),
        StructField("recipient_parent_name", StringType()),
        StructField("recipient_parent_name_raw", StringType()),
        StructField("recipient_country", StringType()),
        StructField("recipient_state", StringType()),
        StructField("recipient_county", StringType()),
        StructField("recipient_city", StringType()),
        StructField("primary_place_of_performance_country", StringType()),
        StructField("primary_place_of_performance_state", StringType()),
        StructField("primary_place_of_performance_county", StringType()),
        StructField("primary_place_of_performance_zip_code", StringType()),
        StructField("cfda_number", StringType()),
        StructField("cfda_title", StringType()),
        StructField("product_or_service_code", StringType()),
        StructField("product_or_service_code_description", StringType()),
        StructField("naics_code", StringType()),
        StructField("naics_description", StringType()),
        StructField("national_interest_action_code", StringType()),
        StructField("national_interest_action", StringType()),
        StructField("reporting_agency_name", StringType()),
        StructField("submission_period", StringType()),
        StructField("allocation_transfer_agency_identifier_code", StringType()),
        StructField("agency_identifier_code", StringType()),
        StructField("beginning_period_of_availability", DateType()),
        StructField("ending_period_of_availability", DateType()),
        StructField("availability_type_code", StringType()),
        StructField("main_account_code", StringType()),
        StructField("sub_account_code", StringType()),
        StructField("treasury_account_symbol", StringType()),
        StructField("treasury_account_name", StringType()),
        StructField("funding_toptier_agency_id", IntegerType()),
        StructField("federal_account_id", IntegerType()),
        StructField("budget_function", StringType()),
        StructField("budget_function_code", StringType()),
        StructField("budget_subfunction", StringType()),
        StructField("budget_subfunction_code", StringType()),
        StructField("transaction_obligated_amount", DecimalType(23, 2)),
        StructField("gross_outlay_amount_fyb_to_period_end", DecimalType(23, 2)),
        StructField("ussgl487200_downward_adj_prior_year_prepaid_undeliv_order_oblig", DecimalType(23, 2)),
        StructField("ussgl497200_downward_adj_of_prior_year_paid_deliv_orders_oblig", DecimalType(23, 2)),
        StructField("award_base_action_date_fiscal_year", IntegerType()),
        StructField("award_latest_action_date_fiscal_year", IntegerType()),
        StructField("award_type_code", StringType()),
        StructField("award_type", StringType()),
        StructField("prime_award_summary_recipient_cd_original", StringType()),
        StructField("prime_award_summary_recipient_cd_current", StringType()),
        StructField("recipient_zip_code", StringType()),
        StructField("prime_award_summary_place_of_performance_cd_original", StringType()),
        StructField("prime_award_summary_place_of_performance_cd_current", StringType()),
        StructField("usaspending_permalink", StringType()),
        StructField("last_modified_date", DateType()),
        StructField("reporting_fiscal_period", IntegerType()),
        StructField("reporting_fiscal_quarter", IntegerType()),
        StructField("reporting_fiscal_year", IntegerType()),
        StructField("quarter_format_flag", BooleanType()),
    ]
)


award_financial_download_load_sql_string = rf"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} (
        {",".join(list([field.name for field in award_financial_schema]))}
    )
    SELECT
        financial_accounts_by_awards.financial_accounts_by_awards_id,
        financial_accounts_by_awards.submission_id,
        federal_toptier_agency.name AS federal_owning_agency_name,
        treasury_toptier_agency.name AS treasury_owning_agency_name,
        federal_account.federal_account_code AS federal_account_symbol,
        federal_account.account_title AS federal_account_name,
        cgac_aid.agency_name AS agency_identifier_name,
        cgac_ata.agency_name AS allocation_transfer_agency_identifier_name,
        ref_program_activity.program_activity_code,
        ref_program_activity.program_activity_name,
        object_class.object_class AS object_class_code,
        object_class.object_class_name,
        object_class.direct_reimbursable AS direct_or_reimbursable_funding_source,
        financial_accounts_by_awards.disaster_emergency_fund_code,
        disaster_emergency_fund_code.title AS disaster_emergency_fund_name,
        award_search.generated_unique_award_id AS award_unique_key,
        financial_accounts_by_awards.piid AS award_id_piid,
        financial_accounts_by_awards.parent_award_id AS parent_award_id_piid,
        financial_accounts_by_awards.fain AS award_id_fain,
        financial_accounts_by_awards.uri AS award_id_uri,
        CAST(award_search.date_signed AS DATE) AS award_base_action_date,
        CAST(award_search.certified_date AS DATE) AS award_latest_action_date,
        CAST(award_search.period_of_performance_start_date AS DATE),
        CAST(award_search.period_of_performance_current_end_date AS DATE),
        CAST(transaction_search.ordering_period_end_date AS DATE),
        transaction_search.idv_type AS idv_type_code,
        transaction_search.idv_type_description AS idv_type,
        award_search.description AS prime_award_base_transaction_description,
        transaction_search.awarding_agency_code,
        transaction_search.awarding_toptier_agency_name_raw AS awarding_agency_name,
        transaction_search.awarding_sub_tier_agency_c AS awarding_subagency_code,
        transaction_search.awarding_subtier_agency_name_raw AS awarding_subagency_name,
        transaction_search.awarding_office_code,
        transaction_search.awarding_office_name,
        transaction_search.funding_agency_code,
        transaction_search.funding_toptier_agency_name_raw AS funding_agency_name,
        transaction_search.funding_sub_tier_agency_co AS funding_sub_agency_code,
        transaction_search.funding_subtier_agency_name_raw AS funding_sub_agency_name,
        transaction_search.funding_office_code,
        transaction_search.funding_office_name,
        transaction_search.recipient_uei,
        transaction_search.recipient_unique_id AS recipient_duns,
        transaction_search.recipient_name,
        transaction_search.recipient_name_raw,
        transaction_search.parent_uei AS recipient_parent_uei,
        transaction_search.parent_uei AS recipient_parent_duns,
        transaction_search.parent_recipient_name AS recipient_parent_name,
        transaction_search.parent_recipient_name_raw AS recipient_parent_name_raw,
        transaction_search.recipient_location_country_code AS recipient_country,
        transaction_search.recipient_location_state_code AS recipient_state,
        transaction_search.recipient_location_county_name AS recipient_county,
        transaction_search.recipient_location_city_name AS recipient_city,
        transaction_search.pop_country_name AS primary_place_of_performance_country,
        transaction_search.pop_state_name AS primary_place_of_performance_state,
        transaction_search.pop_county_name AS primary_place_of_performance_county,
        transaction_search.place_of_performance_zip4a AS primary_place_of_performance_zip_code,
        transaction_search.cfda_number,
        transaction_search.cfda_title,
        transaction_search.product_or_service_code,
        transaction_search.product_or_service_description AS product_or_service_code_description,
        transaction_search.naics_code,
        transaction_search.naics_description,
        transaction_search.national_interest_action AS national_interest_action_code,
        transaction_search.national_interest_desc AS national_interest_action,
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
        treasury_appropriation_account.funding_toptier_agency_id AS funding_toptier_agency_id,
        treasury_appropriation_account.federal_account_id AS federal_account_id,
        treasury_appropriation_account.budget_function_title AS budget_function,
        treasury_appropriation_account.budget_function_code AS budget_function_code,
        treasury_appropriation_account.budget_subfunction_title AS budget_subfunction,
        treasury_appropriation_account.budget_subfunction_code AS budget_subfunction_code,
        financial_accounts_by_awards.transaction_obligated_amount AS transaction_obligated_amount,
        financial_accounts_by_awards.gross_outlay_amount_by_award_cpe as gross_outlay_amount_fyb_to_period_end,
        financial_accounts_by_awards.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe as ussgl487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,
        financial_accounts_by_awards.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe as ussgl497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,
        EXTRACT(
            YEAR FROM (award_search.date_signed) + INTERVAL '3 months'
        ) AS award_base_action_date_fiscal_year,
        EXTRACT(
            YEAR FROM (award_search.certified_date) + INTERVAL '3 months'
        ) AS award_latest_action_date_fiscal_year,
        COALESCE(
            transaction_search.contract_award_type,
            transaction_search.type
        ) AS award_type_code,
        COALESCE(
            transaction_search.contract_award_type_desc,
            transaction_search.type_description
        ) AS award_type,
        CASE
            WHEN
                transaction_search.recipient_location_state_code IS NOT NULL
                AND transaction_search.recipient_location_congressional_code IS NOT NULL
                AND NOT (
                    transaction_search.recipient_location_state_code = ''
                    AND transaction_search.recipient_location_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.recipient_location_state_code, '-',
                        transaction_search.recipient_location_congressional_code
                    )
            ELSE transaction_search.recipient_location_congressional_code
        END AS prime_award_summary_recipient_cd_original,
        CASE
            WHEN
                transaction_search.recipient_location_state_code IS NOT NULL
                AND transaction_search.recipient_location_congressional_code_current IS NOT NULL
                AND NOT (
                    transaction_search.recipient_location_state_code = ''
                    AND transaction_search.recipient_location_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.recipient_location_state_code, '-',
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
        CASE
            WHEN award_search.generated_unique_award_id IS NOT NULL
                THEN
                    CONCAT(
                        '{{AWARD_URL}}',
                        URL_ENCODE(award_search.generated_unique_award_id),
                        '/'
                    )
            ELSE ''
        END AS usaspending_permalink,
        CAST(submission_attributes.published_date AS DATE) AS last_modified_date,
        submission_attributes.reporting_fiscal_period,
        submission_attributes.reporting_fiscal_quarter,
        submission_attributes.reporting_fiscal_year,
        submission_attributes.quarter_format_flag
    FROM
        int.financial_accounts_by_awards
        INNER JOIN global_temp.submission_attributes
            ON (financial_accounts_by_awards.submission_id = submission_attributes.submission_id)
        LEFT OUTER JOIN global_temp.treasury_appropriation_account
            ON (financial_accounts_by_awards.treasury_account_id = treasury_appropriation_account.treasury_account_identifier)
        LEFT OUTER JOIN award_search
            ON (financial_accounts_by_awards.award_id = award_search.award_id)
        LEFT OUTER JOIN transaction_search
            ON (award_search.latest_transaction_search_id = transaction_search.transaction_id)
        LEFT OUTER JOIN global_temp.ref_program_activity
            ON (financial_accounts_by_awards.program_activity_id = ref_program_activity.id)
        LEFT OUTER JOIN global_temp.object_class
            ON (financial_accounts_by_awards.object_class_id = object_class.id)
        LEFT OUTER JOIN global_temp.disaster_emergency_fund_code
            ON (financial_accounts_by_awards.disaster_emergency_fund_code = disaster_emergency_fund_code.code)
        LEFT OUTER JOIN global_temp.federal_account
            ON (treasury_appropriation_account.federal_account_id = federal_account.id)
        LEFT OUTER JOIN global_temp.toptier_agency as federal_toptier_agency
            ON (federal_account.parent_toptier_agency_id = federal_toptier_agency.toptier_agency_id)
        LEFT OUTER JOIN global_temp.toptier_agency as treasury_toptier_agency
            ON (treasury_appropriation_account.funding_toptier_agency_id = treasury_toptier_agency.toptier_agency_id)
        LEFT OUTER JOIN global_temp.cgac AS cgac_aid
            ON (treasury_appropriation_account.agency_id = cgac_aid.cgac_code)
        LEFT OUTER JOIN global_temp.cgac AS cgac_ata
            ON (treasury_appropriation_account.allocation_transfer_agency_id = cgac_ata.cgac_code);
    """
