from usaspending_api.awards.v2.lookups.lookups import award_type_mapping

AWARD_SEARCH_COLUMNS = {
    "treasury_account_identifiers": {"delta": "ARRAY<INTEGER>", "postgres": "INTEGER[]", "gold": False},
    "award_id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL", "gold": False},
    "data_source": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "transaction_unique_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "latest_transaction_id": {"delta": "LONG", "postgres": "BIGINT", "gold": True},
    "earliest_transaction_id": {"delta": "LONG", "postgres": "BIGINT", "gold": True},
    "latest_transaction_search_id": {"delta": "LONG", "postgres": "BIGINT", "gold": True},
    "earliest_transaction_search_id": {"delta": "LONG", "postgres": "BIGINT", "gold": True},
    "category": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_description_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "is_fpds": {"delta": "boolean", "postgres": "boolean", "gold": True},
    "generated_unique_award_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "generated_unique_award_id_legacy": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "display_award_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP", "gold": False},
    "certified_date": {"delta": "DATE", "postgres": "DATE", "gold": True},
    "create_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP", "gold": True},
    "piid": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "fain": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "uri": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_award_piid": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "award_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "total_obligation": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "total_obl_bin": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "total_subsidy_cost": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "total_loan_value": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "total_funding_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23,2)", "gold": True},
    "total_indirect_federal_sharing": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "base_and_all_options_value": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "base_exercised_options_val": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "non_federal_funding_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "recipient_hash": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_levels": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "recipient_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "raw_recipient_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_unique_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_recipient_unique_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_uei": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_uei": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_recipient_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "business_categories": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "total_subaward_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "subaward_count": {"delta": "INTEGER", "postgres": "INTEGER", "gold": True},
    "action_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "last_modified_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "period_of_performance_start_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "period_of_performance_current_end_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "date_signed": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "ordering_period_end_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "original_loan_subsidy_cost": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "face_value_loan_guarantee": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "awarding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "funding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "awarding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_subtier_agency_name": {"delta": " STRING", "postgres": " STRING", "gold": False},
    "funding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_toptier_agency_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_toptier_agency_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_subtier_agency_name_raw": {"delta": " STRING", "postgres": " STRING", "gold": False},
    "funding_subtier_agency_name_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_toptier_agency_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_toptier_agency_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_subtier_agency_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_subtier_agency_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_toptier_agency_code_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_toptier_agency_code_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "awarding_subtier_agency_code_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_subtier_agency_code_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "funding_toptier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "funding_subtier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "fpds_agency_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "fpds_parent_agency_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "recipient_location_country_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_country_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_state_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_county_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_county_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_congressional_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_congressional_code_current": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "recipient_location_zip5": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_city_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_state_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_state_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_state_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "recipient_location_county_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "recipient_location_congressional_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "recipient_location_county_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_address_line1": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_address_line2": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_address_line3": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_zip4": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_foreign_postal_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_foreign_province": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_country_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_country_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_county_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_county_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_city_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_zip5": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_congressional_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_congressional_code_current": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "pop_city_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "pop_county_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "pop_congressional_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "pop_county_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_zip4": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "cfda_program_title": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "cfda_number": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "cfdas": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "sai_number": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_of_contract_pricing": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "extent_competed": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_set_aside": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "product_or_service_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "product_or_service_description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "naics_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "naics_description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "tas_paths": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "tas_components": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "federal_accounts": {"delta": "STRING", "postgres": "JSONB", "gold": False},
    "disaster_emergency_fund_codes": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "spending_by_defc": {"delta": "STRING", "postgres": "JSONB", "gold": False},
    "total_covid_outlay": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "total_covid_obligation": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "officer_1_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "officer_1_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_2_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "officer_2_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_3_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "officer_3_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_4_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "officer_4_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_5_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "officer_5_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "total_iija_outlay": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "total_iija_obligation": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": True},
    "total_outlays": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)", "gold": False},
    "generated_pragmatic_obligation": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)", "gold": False},
    "program_activities": {"delta": "STRING", "postgres": "JSONB", "gold": False},
}
AWARD_SEARCH_DELTA_COLUMNS = {k: v["delta"] for k, v in AWARD_SEARCH_COLUMNS.items()}
AWARD_SEARCH_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in AWARD_SEARCH_COLUMNS.items() if not v["gold"]}
AWARD_SEARCH_POSTGRES_GOLD_COLUMNS = {k: v["gold"] for k, v in AWARD_SEARCH_COLUMNS.items()}

ALL_AWARD_TYPES = list(award_type_mapping.keys())

award_search_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in AWARD_SEARCH_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
"""

_base_load_sql_string = rf"""
  SELECT
  TREASURY_ACCT.treasury_account_identifiers,
  awards.id AS award_id,
  awards.data_source AS data_source,
  awards.transaction_unique_id,
  awards.latest_transaction_id,
  awards.earliest_transaction_id,
  awards.latest_transaction_id AS latest_transaction_search_id,
  awards.earliest_transaction_id AS earliest_transaction_search_id,
  awards.category,
  awards.type AS type_raw,
  awards.type_description AS type_description_raw,
  CASE
    WHEN (
        awards.type NOT IN ({", ".join([f"'{award_type}'" for award_type in ALL_AWARD_TYPES])})
        OR
        awards.type IS NULL
    ) THEN '-1'
    ELSE awards.type
  END AS type,
  CASE
    WHEN (
        awards.type NOT IN ({", ".join([f"'{award_type}'" for award_type in ALL_AWARD_TYPES])})
        OR
        awards.type IS NULL
    ) THEN 'NOT SPECIFIED'
    ELSE awards.type_description
  END AS type_description,
  awards.is_fpds,
  awards.generated_unique_award_id,
  CASE
    WHEN awards.is_fpds = FALSE AND transaction_fabs.record_type = 1
        THEN UPPER(CONCAT(
            'ASST_AGG',
            '_',
            COALESCE(transaction_fabs.uri, '-none-'),
            '_',
            COALESCE(SAA.subtier_code, '-none-')
        ))
    WHEN awards.is_fpds = FALSE
        THEN UPPER(CONCAT(
            'ASST_NON',
            '_',
            COALESCE(transaction_fabs.fain, '-none-'),
            '_',
            COALESCE(SAA.subtier_code, '-none-')
        ))
    ELSE NULL
  END AS generated_unique_award_id_legacy,
  CASE
    WHEN awards.type IN ('02', '03', '04', '05', '06', '10', '07', '08', '09', '11') AND awards.fain IS NOT NULL THEN awards.fain
    WHEN awards.piid IS NOT NULL THEN awards.piid  -- contracts. Did it this way to easily handle IDV contracts
    ELSE awards.uri
  END AS display_award_id,
  awards.update_date,
  awards.certified_date,
  awards.create_date,
  awards.piid,
  awards.fain AS fain,
  awards.uri AS uri,
  awards.parent_award_piid,
  CAST(
    COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) AS NUMERIC(23, 2) ) AS award_amount,
  CAST(
    COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN 0
            ELSE awards.total_obligation END, 0) AS NUMERIC(23, 2) ) AS total_obligation,
  awards.description,
  CASE WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) = 500000000.0 THEN '500M'
    WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) = 100000000.0 THEN '100M'
    WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) = 1000000.0 THEN '1M'
    WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) = 25000000.0 THEN '25M'
    WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) > 500000000.0 THEN '>500M'
    WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) < 1000000.0 THEN '<1M'
    WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) < 25000000.0 THEN '1M..25M'
    WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) < 100000000.0 THEN '25M..100M'
    WHEN COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation END, 0) < 500000000.0 THEN '100M..500M'
    ELSE NULL END AS total_obl_bin,
  CAST(
    COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE 0 END, 0 ) AS NUMERIC(23, 2) ) AS total_subsidy_cost,
  CAST(
    COALESCE(
        CASE WHEN awards.type IN('07', '08') THEN awards.total_loan_value
            ELSE 0 END, 0 ) AS NUMERIC(23, 2) ) AS total_loan_value,
  awards.total_funding_amount,
  awards.total_indirect_federal_sharing,
  awards.base_and_all_options_value,
  awards.base_exercised_options_val,
  awards.non_federal_funding_amount,
  CAST(REGEXP_REPLACE(MD5(UPPER(
     CASE
       WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
       WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
       ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
    END)), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5') AS STRING) AS recipient_hash,
  RECIPIENT_HASH_AND_LEVELS.recipient_levels,
  UPPER(COALESCE(recipient_lookup.legal_business_name, transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) AS recipient_name,
  UPPER(COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) as raw_recipient_name,
  COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AS recipient_unique_id,
  COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id,
  COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) AS recipient_uei,
  COALESCE(transaction_fpds.ultimate_parent_uei, transaction_fabs.ultimate_parent_uei) AS parent_uei,
  COALESCE(transaction_fpds.ultimate_parent_legal_enti, transaction_fabs.ultimate_parent_legal_enti) AS parent_recipient_name,
  latest_transaction.business_categories,

  awards.total_subaward_amount,
  awards.subaward_count,

  latest_transaction.action_date,
  latest_transaction.fiscal_year,
  latest_transaction.last_modified_date,
  awards.period_of_performance_start_date,
  awards.period_of_performance_current_end_date,
  awards.date_signed,
  transaction_fpds.ordering_period_end_date,

  COALESCE(transaction_fabs.original_loan_subsidy_cost, 0) AS original_loan_subsidy_cost,
  COALESCE(transaction_fabs.face_value_loan_guarantee, 0) AS face_value_loan_guarantee,

  latest_transaction.awarding_agency_id,
  latest_transaction.funding_agency_id,
  TAA.name AS awarding_toptier_agency_name,
  TFA.name AS funding_toptier_agency_name,
  SAA.name AS awarding_subtier_agency_name,
  SFA.name AS funding_subtier_agency_name,
  COALESCE(transaction_fabs.awarding_agency_name, transaction_fpds.awarding_agency_name) AS awarding_toptier_agency_name_raw,
  COALESCE(transaction_fabs.funding_agency_name, transaction_fpds.funding_agency_name) funding_toptier_agency_name_raw,
  COALESCE(transaction_fabs.awarding_sub_tier_agency_n, transaction_fpds.awarding_sub_tier_agency_n) awarding_subtier_agency_name_raw,
  COALESCE(transaction_fabs.funding_sub_tier_agency_na, transaction_fpds.funding_sub_tier_agency_na) funding_subtier_agency_name_raw,
  TAA.toptier_code AS awarding_toptier_agency_code,
  TFA.toptier_code AS funding_toptier_agency_code,
  SAA.subtier_code AS awarding_subtier_agency_code,
  SFA.subtier_code AS funding_subtier_agency_code,
  COALESCE(transaction_fabs.awarding_agency_code, transaction_fpds.awarding_agency_code) AS awarding_toptier_agency_code_raw,
  COALESCE(transaction_fabs.funding_agency_code, transaction_fpds.funding_agency_code) AS funding_toptier_agency_code_raw,
  COALESCE(transaction_fabs.awarding_sub_tier_agency_c, transaction_fpds.awarding_sub_tier_agency_c) AS awarding_subtier_agency_code_raw,
  COALESCE(transaction_fabs.funding_sub_tier_agency_co, transaction_fpds.funding_sub_tier_agency_co) AS funding_subtier_agency_code_raw,
  FA_ID.id AS funding_toptier_agency_id,
  latest_transaction.funding_agency_id AS funding_subtier_agency_id,
  awards.fpds_agency_id,
  awards.fpds_parent_agency_id,

  COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code, 'USA') AS recipient_location_country_code,
  COALESCE(transaction_fpds.legal_entity_country_name, transaction_fabs.legal_entity_country_name) AS recipient_location_country_name,
  COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) AS recipient_location_state_code,
  LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AS recipient_location_county_code,
  COALESCE(transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name) AS recipient_location_county_name,
  LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
            AS recipient_location_congressional_code,
  LATEST_CURRENT_CD.recipient_location_congressional_code_current AS recipient_location_congressional_code_current,
  COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5) AS recipient_location_zip5,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.legal_entity_city_name, transaction_fabs.legal_entity_city_name)) AS recipient_location_city_name,
  COALESCE(transaction_fpds.legal_entity_state_descrip, transaction_fabs.legal_entity_state_name) AS recipient_location_state_name,
  RL_STATE_LOOKUP.fips AS recipient_location_state_fips,
  RL_STATE_POPULATION.latest_population AS recipient_location_state_population,
  RL_COUNTY_POPULATION.latest_population AS recipient_location_county_population,
  RL_DISTRICT_POPULATION.latest_population AS recipient_location_congressional_population,
  CONCAT(
    RL_STATE_LOOKUP.fips,
    COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code)
  ) AS recipient_location_county_fips,
  COALESCE(transaction_fpds.legal_entity_address_line1, transaction_fabs.legal_entity_address_line1) AS recipient_location_address_line1,
  COALESCE(transaction_fpds.legal_entity_address_line2, transaction_fabs.legal_entity_address_line2) AS recipient_location_address_line2,
  COALESCE(transaction_fpds.legal_entity_address_line3, transaction_fabs.legal_entity_address_line3) AS recipient_location_address_line3,
  COALESCE(transaction_fpds.legal_entity_zip_last4, transaction_fabs.legal_entity_zip_last4) AS recipient_location_zip4,
  transaction_fabs.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,
  transaction_fabs.legal_entity_foreign_provi AS recipient_location_foreign_province,

  COALESCE(transaction_fpds.place_of_perf_country_desc, transaction_fabs.place_of_perform_country_n) AS pop_country_name,
  COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA') AS pop_country_code,
  COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) AS pop_state_code,
  LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AS pop_county_code,
  COALESCE(transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na) AS pop_county_name,
  transaction_fabs.place_of_performance_code AS pop_city_code,
  COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5) AS pop_zip5,
  LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
            AS pop_congressional_code,
  LATEST_CURRENT_CD.pop_congressional_code_current AS pop_congressional_code_current,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city)) AS pop_city_name,
  COALESCE(transaction_fpds.place_of_perfor_state_desc, transaction_fabs.place_of_perform_state_nam) AS pop_state_name,
  POP_STATE_LOOKUP.fips AS pop_state_fips,
  POP_STATE_POPULATION.latest_population AS pop_state_population,
  POP_COUNTY_POPULATION.latest_population AS pop_county_population,
  POP_DISTRICT_POPULATION.latest_population AS pop_congressional_population,
  CONCAT(
    POP_STATE_LOOKUP.fips,
    COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co)
  ) AS pop_county_fips,
  COALESCE(transaction_fpds.place_of_performance_zip4a, transaction_fabs.place_of_performance_zip4a) AS pop_zip4,

  transaction_fabs.cfda_title AS cfda_program_title,
  transaction_fabs.cfda_number AS cfda_number,
  CASE WHEN awards.is_fpds = FALSE THEN transaction_cfdas.cfdas ELSE NULL END AS cfdas,


  transaction_fabs.sai_number AS sai_number,
  transaction_fpds.type_of_contract_pricing,
  transaction_fpds.extent_competed,
  transaction_fpds.type_set_aside,

  transaction_fpds.product_or_service_code,
  psc.description AS product_or_service_description,
  transaction_fpds.naics AS naics_code,
  transaction_fpds.naics_description,

  TREASURY_ACCT.tas_paths,
  TREASURY_ACCT.tas_components,
  TREASURY_ACCT.federal_accounts,
  TREASURY_ACCT.disaster_emergency_fund_codes,
  OUTLAYS_AND_OBLIGATIONS.spending_by_defc,
  OUTLAYS_AND_OBLIGATIONS.total_covid_outlay,
  OUTLAYS_AND_OBLIGATIONS.total_covid_obligation,
  awards.officer_1_amount,
  awards.officer_1_name,
  awards.officer_2_amount,
  awards.officer_2_name,
  awards.officer_3_amount,
  awards.officer_3_name,
  awards.officer_4_amount,
  awards.officer_4_name,
  awards.officer_5_amount,
  awards.officer_5_name,

  OUTLAYS_AND_OBLIGATIONS.total_iija_outlay,
  OUTLAYS_AND_OBLIGATIONS.total_iija_obligation,
  CAST(OUTLAYS_AND_OBLIGATIONS.total_outlays AS NUMERIC(23, 2)) AS total_outlays,
  CAST(COALESCE(
        CASE
            WHEN awards.type IN('07', '08') THEN awards.total_subsidy_cost
            ELSE awards.total_obligation
        END,
        0
  ) AS NUMERIC(23, 2)) AS generated_pragmatic_obligation,
  TREASURY_ACCT.program_activities
FROM
  int.awards
INNER JOIN
  int.transaction_normalized AS latest_transaction
    ON (awards.latest_transaction_id = latest_transaction.id)
LEFT OUTER JOIN
  int.transaction_fpds
    ON (awards.latest_transaction_id = transaction_fpds.transaction_id AND latest_transaction.is_fpds = true)
LEFT OUTER JOIN
  int.transaction_fabs
    ON (awards.latest_transaction_id = transaction_fabs.transaction_id AND latest_transaction.is_fpds = false)
LEFT OUTER JOIN
  rpt.recipient_lookup ON recipient_lookup.recipient_hash = REGEXP_REPLACE(MD5(UPPER(
     CASE
       WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
       WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
       ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
    END)), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
LEFT OUTER JOIN
  global_temp.psc ON (transaction_fpds.product_or_service_code = psc.code)
  LEFT OUTER JOIN
    (SELECT
      award_id, SORT_ARRAY(COLLECT_SET(DISTINCT TO_JSON(NAMED_STRUCT('cfda_number', cfda_number, 'cfda_program_title', cfda_title)))) as cfdas
      FROM
         int.transaction_fabs tf
       INNER JOIN int.transaction_normalized tn ON
         tf.transaction_id = tn.id
       GROUP BY
         award_id
    ) AS transaction_cfdas ON awards.id = transaction_cfdas.award_id
LEFT OUTER JOIN
  global_temp.agency AS AA
    ON (awards.awarding_agency_id = AA.id)
LEFT OUTER JOIN
  global_temp.toptier_agency AS TAA
    ON (AA.toptier_agency_id = TAA.toptier_agency_id)
LEFT OUTER JOIN
  global_temp.subtier_agency AS SAA
    ON (AA.subtier_agency_id = SAA.subtier_agency_id)
LEFT OUTER JOIN
  global_temp.agency AS FA ON (awards.funding_agency_id = FA.id)
LEFT OUTER JOIN
  global_temp.toptier_agency AS TFA
    ON (FA.toptier_agency_id = TFA.toptier_agency_id)
LEFT OUTER JOIN
  global_temp.subtier_agency AS SFA
    ON (FA.subtier_agency_id = SFA.subtier_agency_id)
LEFT OUTER JOIN
  (SELECT id, toptier_agency_id, ROW_NUMBER() OVER (PARTITION BY toptier_agency_id ORDER BY toptier_flag DESC, id ASC) AS row_num FROM global_temp.agency) AS FA_ID
    ON (FA_ID.toptier_agency_id = TFA.toptier_agency_id AND row_num = 1)
LEFT OUTER JOIN
 (SELECT code, name, fips, MAX(id) FROM global_temp.state_data GROUP BY code, name, fips) AS POP_STATE_LOOKUP
 ON (POP_STATE_LOOKUP.code = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code))
LEFT OUTER JOIN
 (SELECT code, name, fips, MAX(id) FROM global_temp.state_data GROUP BY code, name, fips) AS RL_STATE_LOOKUP
 ON (RL_STATE_LOOKUP.code = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code))
LEFT OUTER JOIN global_temp.ref_population_county AS POP_STATE_POPULATION ON (
    POP_STATE_POPULATION.state_code = POP_STATE_LOOKUP.fips
    AND POP_STATE_POPULATION.county_number = '000'
)
LEFT OUTER JOIN global_temp.ref_population_county AS POP_COUNTY_POPULATION ON (
    POP_COUNTY_POPULATION.state_code = POP_STATE_LOOKUP.fips AND
    POP_COUNTY_POPULATION.county_number = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
)
LEFT OUTER JOIN global_temp.ref_population_county AS RL_STATE_POPULATION ON (
    RL_STATE_POPULATION.state_code = RL_STATE_LOOKUP.fips
    AND RL_STATE_POPULATION.county_number = '000'
)
LEFT OUTER JOIN
    global_temp.ref_population_county RL_COUNTY_POPULATION ON (
        RL_COUNTY_POPULATION.state_code = RL_STATE_LOOKUP.fips
        AND RL_COUNTY_POPULATION.county_number = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
)
LEFT OUTER JOIN
    global_temp.ref_population_cong_district POP_DISTRICT_POPULATION ON (
        POP_DISTRICT_POPULATION.state_code = POP_STATE_LOOKUP.fips
        AND POP_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
)
LEFT OUTER JOIN
    global_temp.ref_population_cong_district RL_DISTRICT_POPULATION ON (
         RL_DISTRICT_POPULATION.state_code = RL_STATE_LOOKUP.fips
        AND RL_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
)
LEFT OUTER JOIN
    int.transaction_current_cd_lookup AS LATEST_CURRENT_CD ON (
        awards.latest_transaction_id = LATEST_CURRENT_CD.transaction_id
    )
LEFT OUTER JOIN (
        SELECT recipient_hash, uei, SORT_ARRAY(COLLECT_SET(recipient_level)) AS recipient_levels
        FROM rpt.recipient_profile
        WHERE recipient_level != 'P'
        GROUP BY recipient_hash, uei
    ) RECIPIENT_HASH_AND_LEVELS ON (
        recipient_lookup.recipient_hash = RECIPIENT_HASH_AND_LEVELS.recipient_hash
        AND recipient_lookup.legal_business_name NOT IN (
            'MULTIPLE RECIPIENTS',
            'REDACTED DUE TO PII',
            'MULTIPLE FOREIGN RECIPIENTS',
            'PRIVATE INDIVIDUAL',
            'INDIVIDUAL RECIPIENT',
            'MISCELLANEOUS FOREIGN AWARDEES'
        )
        AND recipient_lookup.legal_business_name IS NOT NULL
    )
LEFT OUTER JOIN (
  SELECT
        GROUPED_BY_DEFC.award_id,
        CAST(SORT_ARRAY(COLLECT_SET(
            TO_JSON(NAMED_STRUCT('defc', GROUPED_BY_DEFC.def_code, 'outlay', GROUPED_BY_DEFC.outlay, 'obligation', GROUPED_BY_DEFC.obligation))
        ) FILTER (WHERE GROUPED_BY_DEFC.def_code IS NOT NULL)) AS STRING) AS spending_by_defc,
        sum(GROUPED_BY_DEFC.outlay) AS total_outlays,
        sum(GROUPED_BY_DEFC.outlay) FILTER (WHERE GROUPED_BY_DEFC.group_name = 'covid_19') AS total_covid_outlay,
        sum(GROUPED_BY_DEFC.obligation) FILTER (WHERE GROUPED_BY_DEFC.group_name = 'covid_19') AS total_covid_obligation,
        sum(GROUPED_BY_DEFC.outlay) FILTER (WHERE GROUPED_BY_DEFC.group_name = 'infrastructure') AS total_iija_outlay,
        sum(GROUPED_BY_DEFC.obligation) FILTER (WHERE GROUPED_BY_DEFC.group_name = 'infrastructure') AS total_iija_obligation
    FROM (
        SELECT
            faba.award_id,
            faba.disaster_emergency_fund_code AS def_code,
            defc.group_name,
            COALESCE(sum(
                CASE
                    WHEN sa.is_final_balances_for_fy = true
                    THEN
                        COALESCE(faba.gross_outlay_amount_by_award_cpe, 0)
                        + COALESCE(faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0)
                        + COALESCE(faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0)
                    ELSE NULL
                END), 0) AS outlay,
            COALESCE(sum(faba.transaction_obligated_amount), 0) AS obligation
        FROM
            int.financial_accounts_by_awards AS faba
        INNER JOIN
            global_temp.submission_attributes AS sa ON (faba.submission_id = sa.submission_id)
        INNER JOIN
            global_temp.dabs_submission_window_schedule AS dsws ON (sa.submission_window_id = dsws.id AND dsws.submission_reveal_date <= now())
        LEFT OUTER JOIN
            global_temp.disaster_emergency_fund_code AS defc ON (faba.disaster_emergency_fund_code = defc.code)
        WHERE
            faba.gross_outlay_amount_by_award_cpe IS NOT NULL
            OR faba.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe IS NOT NULL
            OR faba.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe IS NOT NULL
            OR faba.transaction_obligated_amount IS NOT NULL
        GROUP BY
            faba.award_id, faba.disaster_emergency_fund_code, defc.group_name
    ) AS GROUPED_BY_DEFC
    WHERE
        GROUPED_BY_DEFC.award_id IS NOT NULL
    GROUP BY
        GROUPED_BY_DEFC.award_id
) OUTLAYS_AND_OBLIGATIONS on (OUTLAYS_AND_OBLIGATIONS.award_id = awards.id)
LEFT OUTER JOIN (
  SELECT
    faba.award_id,
    TO_JSON(
        SORT_ARRAY(
            COLLECT_SET(
                NAMED_STRUCT(
                    'id', fa.id,
                    'account_title', fa.account_title,
                    'federal_account_code', fa.federal_account_code
                )
            )
        )
    ) AS federal_accounts,
    SORT_ARRAY(COLLECT_SET(
      DISTINCT CONCAT(
        'agency=', COALESCE(agency.toptier_code, ''),
        'faaid=', COALESCE(fa.agency_identifier, ''),
        'famain=', COALESCE(fa.main_account_code, ''),
        'aid=', COALESCE(taa.agency_id, ''),
        'main=', COALESCE(taa.main_account_code, ''),
        'ata=', COALESCE(taa.allocation_transfer_agency_id, ''),
        'sub=', COALESCE(taa.sub_account_code, ''),
        'bpoa=', COALESCE(taa.beginning_period_of_availability, ''),
        'epoa=', COALESCE(taa.ending_period_of_availability, ''),
        'a=', COALESCE(taa.availability_type_code, '')
      )
    )) AS tas_paths,
    SORT_ARRAY(COLLECT_SET(
      CONCAT(
        'aid=', COALESCE(taa.agency_id, ''),
        'main=', COALESCE(taa.main_account_code, ''),
        'ata=', COALESCE(taa.allocation_transfer_agency_id, ''),
        'sub=', COALESCE(taa.sub_account_code, ''),
        'bpoa=', COALESCE(taa.beginning_period_of_availability, ''),
        'epoa=', COALESCE(taa.ending_period_of_availability, ''),
        'a=', COALESCE(taa.availability_type_code, '')
      )
    )) AS tas_components,
    -- "CASE" put in place so that Spark value matches Postgres; can most likely be refactored out in the future
    CASE
        WHEN SIZE(COLLECT_SET(faba.disaster_emergency_fund_code)) > 0
            THEN SORT_ARRAY(COLLECT_SET(faba.disaster_emergency_fund_code))
        ELSE NULL
    END AS disaster_emergency_fund_codes,
    SORT_ARRAY(COLLECT_SET(taa.treasury_account_identifier)) AS treasury_account_identifiers,
    CAST(SORT_ARRAY(COLLECT_SET(
        TO_JSON(
            NAMED_STRUCT(
                'name', UPPER(rpa.program_activity_name),
                'code', LPAD(rpa.program_activity_code, 4, "0")
            )
        )
    )) AS STRING) AS program_activities
  FROM
    global_temp.treasury_appropriation_account taa
  INNER JOIN int.financial_accounts_by_awards faba ON (taa.treasury_account_identifier = faba.treasury_account_id)
  INNER JOIN global_temp.federal_account fa ON (taa.federal_account_id = fa.id)
  INNER JOIN global_temp.toptier_agency agency ON (fa.parent_toptier_agency_id = agency.toptier_agency_id)
  LEFT JOIN global_temp.ref_program_activity rpa ON (faba.program_activity_id = rpa.id)
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
) TREASURY_ACCT ON (TREASURY_ACCT.award_id = awards.id)
"""

award_search_incremental_load_sql_string = [
    f"""
    CREATE OR REPLACE TEMPORARY VIEW temp_award_search_view AS (
        {_base_load_sql_string}
    )
    """,
    f"""
    MERGE INTO {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} AS t
    USING (SELECT * FROM temp_award_search_view) AS s
    ON t.award_id = s.award_id
    WHEN MATCHED AND
      ({" OR ".join([f"NOT (s.{col} <=> t.{col})" for col in AWARD_SEARCH_DELTA_COLUMNS])})
      THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """,
]

award_search_overwrite_load_sql_string = f"""
INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join([col for col in AWARD_SEARCH_DELTA_COLUMNS])}
    )
    {_base_load_sql_string}
"""
