from usaspending_api.awards.v2.lookups.lookups import award_type_mapping

AWARD_SEARCH_COLUMNS = {
    "treasury_account_identifiers": {
        "delta": "ARRAY<INTEGER>",
        "postgres": "INTEGER[]",
        "gold": False,
    },
    "award_id": {
        "delta": "LONG NOT NULL",
        "postgres": "BIGINT NOT NULL",
        "gold": False,
    },
    "data_source": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "transaction_unique_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "latest_transaction_id": {"delta": "LONG", "postgres": "BIGINT", "gold": True},
    "earliest_transaction_id": {"delta": "LONG", "postgres": "BIGINT", "gold": True},
    "latest_transaction_search_id": {
        "delta": "LONG",
        "postgres": "BIGINT",
        "gold": True,
    },
    "earliest_transaction_search_id": {
        "delta": "LONG",
        "postgres": "BIGINT",
        "gold": True,
    },
    "category": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_description_raw": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "type_description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "is_fpds": {"delta": "boolean", "postgres": "boolean", "gold": True},
    "generated_unique_award_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "generated_unique_award_id_legacy": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "display_award_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP", "gold": False},
    "certified_date": {"delta": "DATE", "postgres": "DATE", "gold": True},
    "create_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP", "gold": True},
    "piid": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "fain": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "uri": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_award_piid": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "award_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "total_obligation": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "total_obl_bin": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "total_subsidy_cost": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "total_loan_value": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "total_funding_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23,2)",
        "gold": True,
    },
    "total_indirect_federal_sharing": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "base_and_all_options_value": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "base_exercised_options_val": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "non_federal_funding_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "recipient_hash": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_levels": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "recipient_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "raw_recipient_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_unique_id": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_recipient_unique_id": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_uei": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_uei": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "parent_recipient_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "business_categories": {
        "delta": "ARRAY<STRING>",
        "postgres": "TEXT[]",
        "gold": False,
    },
    "total_subaward_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "subaward_count": {"delta": "INTEGER", "postgres": "INTEGER", "gold": True},
    "action_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "last_modified_date": {
        "delta": "TIMESTAMP",
        "postgres": "TIMESTAMP",
        "gold": False,
    },
    "period_of_performance_start_date": {
        "delta": "DATE",
        "postgres": "DATE",
        "gold": False,
    },
    "period_of_performance_current_end_date": {
        "delta": "DATE",
        "postgres": "DATE",
        "gold": False,
    },
    "date_signed": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "ordering_period_end_date": {"delta": "DATE", "postgres": "DATE", "gold": False},
    "original_loan_subsidy_cost": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "face_value_loan_guarantee": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "awarding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "funding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "awarding_toptier_agency_name": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "funding_toptier_agency_name": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "awarding_subtier_agency_name": {
        "delta": " STRING",
        "postgres": " STRING",
        "gold": False,
    },
    "funding_subtier_agency_name": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "awarding_toptier_agency_name_raw": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "funding_toptier_agency_name_raw": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "awarding_subtier_agency_name_raw": {
        "delta": " STRING",
        "postgres": " STRING",
        "gold": False,
    },
    "funding_subtier_agency_name_raw": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "awarding_toptier_agency_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "funding_toptier_agency_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "awarding_subtier_agency_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "funding_subtier_agency_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "awarding_toptier_agency_code_raw": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "funding_toptier_agency_code_raw": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "awarding_subtier_agency_code_raw": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "funding_subtier_agency_code_raw": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "funding_toptier_agency_id": {
        "delta": "INTEGER",
        "postgres": "INTEGER",
        "gold": False,
    },
    "funding_subtier_agency_id": {
        "delta": "INTEGER",
        "postgres": "INTEGER",
        "gold": False,
    },
    "fpds_agency_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "fpds_parent_agency_id": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "recipient_location_country_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_country_name": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_state_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_county_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_county_name": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_congressional_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_congressional_code_current": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": True,
    },
    "recipient_location_zip5": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_city_name": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_state_name": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_state_fips": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_state_population": {
        "delta": "INTEGER",
        "postgres": "INTEGER",
        "gold": False,
    },
    "recipient_location_county_population": {
        "delta": "INTEGER",
        "postgres": "INTEGER",
        "gold": False,
    },
    "recipient_location_congressional_population": {
        "delta": "INTEGER",
        "postgres": "INTEGER",
        "gold": False,
    },
    "recipient_location_county_fips": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_address_line1": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_address_line2": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_address_line3": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_zip4": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "recipient_location_foreign_postal_code": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "recipient_location_foreign_province": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "pop_country_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_country_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_county_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_county_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_city_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_zip5": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_congressional_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_congressional_code_current": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": True,
    },
    "pop_city_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_name": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_fips": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "pop_state_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "pop_county_population": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
    "pop_congressional_population": {
        "delta": "INTEGER",
        "postgres": "INTEGER",
        "gold": False,
    },
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
    "product_or_service_description": {
        "delta": "STRING",
        "postgres": "TEXT",
        "gold": False,
    },
    "naics_code": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "naics_description": {"delta": "STRING", "postgres": "TEXT", "gold": False},
    "tas_paths": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "tas_components": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]", "gold": False},
    "federal_accounts": {"delta": "STRING", "postgres": "JSONB", "gold": False},
    "disaster_emergency_fund_codes": {
        "delta": "ARRAY<STRING>",
        "postgres": "TEXT[]",
        "gold": False,
    },
    "spending_by_defc": {"delta": "STRING", "postgres": "JSONB", "gold": False},
    "total_covid_outlay": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "total_covid_obligation": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "officer_1_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "officer_1_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_2_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "officer_2_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_3_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "officer_3_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_4_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "officer_4_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "officer_5_amount": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "officer_5_name": {"delta": "STRING", "postgres": "TEXT", "gold": True},
    "total_iija_outlay": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "total_iija_obligation": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": True,
    },
    "total_outlays": {
        "delta": "NUMERIC(23, 2)",
        "postgres": "NUMERIC(23, 2)",
        "gold": False,
    },
    "generated_pragmatic_obligation": {
        "delta": "NUMERIC(23,2)",
        "postgres": "NUMERIC(23,2)",
        "gold": False,
    },
    "program_activities": {"delta": "STRING", "postgres": "JSONB", "gold": False},
    "transaction_count": {"delta": "INTEGER", "postgres": "INTEGER", "gold": False},
}
DELTA_ONLY_COLUMNS = {
    "merge_hash_key": "LONG",
}
AWARD_SEARCH_DELTA_COLUMNS = {
    **{k: v["delta"] for k, v in AWARD_SEARCH_COLUMNS.items()},
    **DELTA_ONLY_COLUMNS,
}
AWARD_SEARCH_POSTGRES_COLUMNS = {
    k: v["postgres"] for k, v in AWARD_SEARCH_COLUMNS.items() if not v["gold"]
}
AWARD_SEARCH_POSTGRES_GOLD_COLUMNS = {
    k: v["gold"] for k, v in AWARD_SEARCH_COLUMNS.items()
}

ALL_AWARD_TYPES = list(award_type_mapping.keys())

award_search_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f"{key} {val}" for key, val in AWARD_SEARCH_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
"""
