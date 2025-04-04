from usaspending_api.recipient.v2.lookups import SPECIAL_CASES

SUBAWARD_SEARCH_COLUMNS = {
    # Broker Subaward Table Meta
    "broker_created_at": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "broker_updated_at": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "broker_subaward_id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL"},
    # Prime Award Fields (from Broker)
    "unique_award_key": {"delta": "STRING", "postgres": "TEXT"},
    "award_piid_fain": {"delta": "STRING", "postgres": "TEXT"},
    "parent_award_id": {"delta": "STRING", "postgres": "TEXT"},
    "award_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "action_date": {"delta": "DATE", "postgres": "DATE"},
    "fy": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_sub_tier_agency_c": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_sub_tier_agency_n": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_office_code": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_office_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_sub_tier_agency_co": {"delta": "STRING", "postgres": "TEXT"},
    "funding_sub_tier_agency_na": {"delta": "STRING", "postgres": "TEXT"},
    "funding_office_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_office_name": {"delta": "STRING", "postgres": "TEXT"},
    "awardee_or_recipient_uniqu": {"delta": "STRING", "postgres": "TEXT"},
    "awardee_or_recipient_uei": {"delta": "STRING", "postgres": "TEXT"},
    "awardee_or_recipient_legal": {"delta": "STRING", "postgres": "TEXT"},
    "dba_name": {"delta": "STRING", "postgres": "TEXT"},
    "ultimate_parent_unique_ide": {"delta": "STRING", "postgres": "TEXT"},
    "ultimate_parent_uei": {"delta": "STRING", "postgres": "TEXT"},
    "ultimate_parent_legal_enti": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_country_code": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_country_name": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_state_name": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_zip": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_county_code": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_congressional": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_foreign_posta": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_city_name": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_address_line1": {"delta": "STRING", "postgres": "TEXT"},
    "business_types": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_country_co": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_country_na": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_state_name": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_performance_zip": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_county_code": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_congressio": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_city_name": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_street": {"delta": "STRING", "postgres": "TEXT"},
    "award_description": {"delta": "STRING", "postgres": "TEXT"},
    "naics": {"delta": "STRING", "postgres": "TEXT"},
    "naics_description": {"delta": "STRING", "postgres": "TEXT"},
    "cfda_numbers": {"delta": "STRING", "postgres": "TEXT"},
    "cfda_titles": {"delta": "STRING", "postgres": "TEXT"},
    # Subaward Fields (from Broker)
    "subaward_type": {"delta": "STRING", "postgres": "TEXT"},
    "subaward_report_year": {"delta": "SHORT", "postgres": "SMALLINT"},
    "subaward_report_month": {"delta": "SHORT", "postgres": "SMALLINT"},
    "subaward_number": {"delta": "STRING", "postgres": "TEXT"},
    "subaward_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "sub_action_date": {"delta": "DATE", "postgres": "DATE"},
    "sub_awardee_or_recipient_uniqu": {"delta": "STRING", "postgres": "TEXT"},
    "sub_awardee_or_recipient_uei": {"delta": "STRING", "postgres": "TEXT"},
    "sub_awardee_or_recipient_legal_raw": {"delta": "STRING", "postgres": "TEXT"},
    "sub_dba_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_ultimate_parent_unique_ide": {"delta": "STRING", "postgres": "TEXT"},
    "sub_ultimate_parent_uei": {"delta": "STRING", "postgres": "TEXT"},
    "sub_ultimate_parent_legal_enti_raw": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_country_code_raw": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_country_name_raw": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_state_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_zip": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_county_code": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_congressional_raw": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_foreign_posta": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_city_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_address_line1": {"delta": "STRING", "postgres": "TEXT"},
    "sub_business_types": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_country_co_raw": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_country_na": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_state_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_performance_zip": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_county_code": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_congressio_raw": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_city_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_street": {"delta": "STRING", "postgres": "TEXT"},
    "subaward_description": {"delta": "STRING", "postgres": "TEXT"},
    "sub_high_comp_officer1_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "sub_high_comp_officer1_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "sub_high_comp_officer2_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "sub_high_comp_officer2_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "sub_high_comp_officer3_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "sub_high_comp_officer3_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "sub_high_comp_officer4_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "sub_high_comp_officer4_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "sub_high_comp_officer5_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "sub_high_comp_officer5_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    # Additional Prime Award Fields (from Broker)
    "prime_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "internal_id": {"delta": "STRING", "postgres": "TEXT"},
    "date_submitted": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "report_type": {"delta": "STRING", "postgres": "TEXT"},
    "transaction_type": {"delta": "STRING", "postgres": "TEXT"},
    "program_title": {"delta": "STRING", "postgres": "TEXT"},
    "contract_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "contract_idv_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "grant_funding_agency_id": {"delta": "STRING", "postgres": "TEXT"},
    "grant_funding_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "federal_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "treasury_symbol": {"delta": "STRING", "postgres": "TEXT"},
    "dunsplus4": {"delta": "STRING", "postgres": "TEXT"},
    "recovery_model_q1": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "recovery_model_q2": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "compensation_q1": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "compensation_q2": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "high_comp_officer1_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "high_comp_officer1_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "high_comp_officer2_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "high_comp_officer2_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "high_comp_officer3_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "high_comp_officer3_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "high_comp_officer4_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "high_comp_officer4_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "high_comp_officer5_full_na": {"delta": "STRING", "postgres": "TEXT"},
    "high_comp_officer5_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    # Additional Subaward Fields (from Broker)
    "sub_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "sub_parent_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "sub_federal_agency_id": {"delta": "STRING", "postgres": "TEXT"},
    "sub_federal_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_funding_agency_id": {"delta": "STRING", "postgres": "TEXT"},
    "sub_funding_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_funding_office_id": {"delta": "STRING", "postgres": "TEXT"},
    "sub_funding_office_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_naics": {"delta": "STRING", "postgres": "TEXT"},
    "sub_cfda_numbers": {"delta": "STRING", "postgres": "TEXT"},
    "sub_dunsplus4": {"delta": "STRING", "postgres": "TEXT"},
    "sub_recovery_subcontract_amt": {"delta": "STRING", "postgres": "TEXT"},
    "sub_recovery_model_q1": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "sub_recovery_model_q2": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "sub_compensation_q1": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "sub_compensation_q2": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    # USAS Links (and associated derivations)
    "award_id": {"delta": "LONG", "postgres": "BIGINT"},
    "prime_award_group": {"delta": "STRING", "postgres": "TEXT"},
    "prime_award_type": {"delta": "STRING", "postgres": "TEXT"},
    "piid": {"delta": "STRING", "postgres": "TEXT"},
    "fain": {"delta": "STRING", "postgres": "TEXT"},
    "latest_transaction_id": {"delta": "LONG", "postgres": "BIGINT"},
    "last_modified_date": {"delta": "DATE", "postgres": "DATE"},
    "awarding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "awarding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_toptier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_subtier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "funding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_subtier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "funding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "cfda_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "cfda_number": {"delta": "STRING", "postgres": "TEXT"},
    "cfda_title": {"delta": "STRING", "postgres": "TEXT"},
    # USAS Derived Fields
    "sub_fiscal_year": {"delta": "INTEGER NOT NULL", "postgres": "INTEGER NOT NULL"},
    "sub_total_obl_bin": {"delta": "STRING NOT NULL", "postgres": "TEXT NOT NULL"},
    "sub_awardee_or_recipient_legal": {"delta": "STRING", "postgres": "TEXT"},
    "sub_ultimate_parent_legal_enti": {"delta": "STRING", "postgres": "TEXT"},
    "business_type_code": {"delta": "STRING", "postgres": "TEXT"},
    "business_categories": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "treasury_account_identifiers": {"delta": "ARRAY<INTEGER>", "postgres": "INTEGER[]"},
    "pulled_from": {"delta": "STRING", "postgres": "TEXT"},
    "type_of_contract_pricing": {"delta": "STRING", "postgres": "TEXT"},
    "type_set_aside": {"delta": "STRING", "postgres": "TEXT"},
    "extent_competed": {"delta": "STRING", "postgres": "TEXT"},
    "product_or_service_code": {"delta": "STRING", "postgres": "TEXT"},
    "product_or_service_description": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_congressional_current": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_country_code": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_country_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_zip5": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_city_code": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_congressional": {"delta": "STRING", "postgres": "TEXT"},
    "sub_legal_entity_congressional_current": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_performance_congressional_current": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_scope": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_country_co": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_country_name": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_zip5": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_city_code": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_perform_congressio": {"delta": "STRING", "postgres": "TEXT"},
    "sub_place_of_performance_congressional_current": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_state_fips": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_state_fips": {"delta": "STRING", "postgres": "TEXT"},
    "legal_entity_county_fips": {"delta": "STRING", "postgres": "TEXT"},
    "place_of_perform_county_fips": {"delta": "STRING", "postgres": "TEXT"},
    "pop_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "program_activities": {"delta": "STRING", "postgres": "JSONB", "gold": False},
    "prime_award_recipient_id": {"delta": "STRING", "postgres": "TEXT"},
    "subaward_recipient_hash": {"delta": "STRING", "postgres": "TEXT"},
    "subaward_recipient_level": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_toptier_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_code": {"delta": "STRING", "postgres": "TEXT"},
}
SUBAWARD_SEARCH_POSTGRES_VECTORS = {
    "keyword_ts_vector": ["sub_awardee_or_recipient_legal", "product_or_service_description", "subaward_description"],
    "award_ts_vector": ["award_piid_fain", "subaward_number"],
    "recipient_name_ts_vector": ["sub_awardee_or_recipient_legal"],
}
SUBAWARD_SEARCH_DELTA_COLUMNS = {k: v["delta"] for k, v in SUBAWARD_SEARCH_COLUMNS.items()}
SUBAWARD_SEARCH_POSTGRES_COLUMNS = {
    **{k: v["postgres"] for k, v in SUBAWARD_SEARCH_COLUMNS.items()},
    **dict.fromkeys(SUBAWARD_SEARCH_POSTGRES_VECTORS, "TSVECTOR"),
}

special_cases = tuple(sc for sc in SPECIAL_CASES)

subaward_search_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in SUBAWARD_SEARCH_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""

subaward_search_load_sql_string = rf"""
    WITH location_summary AS (
        SELECT
            UPPER(feature_name) as feature_name,
            state_alpha,
            county_numeric,
            UPPER(county_name) as county_name,
            census_code,
            ROW_NUMBER() OVER (PARTITION BY UPPER(feature_name), state_alpha ORDER BY UPPER(feature_name), state_alpha, county_sequence, coalesce(date_edited, date_created) DESC, id DESC) as row_num
        FROM
            global_temp.ref_city_county_state_code
        WHERE
            feature_class = 'Populated Place'
            AND COALESCE(feature_name, '') <>  ''
            AND COALESCE(state_alpha, '') <> ''
    ),
    recipient_summary AS (
        SELECT
          legal_business_name AS recipient_name,
          uei,
          duns,
          ROW_NUMBER() OVER(PARTITION BY uei ORDER BY uei, duns NULLS LAST, legal_business_name NULLS LAST) AS row
        FROM
            rpt.recipient_lookup AS rlv
    ),
    tas_summary AS (
        SELECT
            faba.award_id,
            SORT_ARRAY(COLLECT_SET(CAST(taa.treasury_account_identifier AS INTEGER))) AS treasury_account_identifiers,
            COLLECT_SET(
                TO_JSON(
                    NAMED_STRUCT(
                        'name', UPPER(rpa.program_activity_name),
                        'code', LPAD(rpa.program_activity_code, 4, "0")
                    )
                )
            ) AS program_activities
        FROM
            global_temp.treasury_appropriation_account AS taa
        INNER JOIN
            int.financial_accounts_by_awards AS faba
                ON taa.treasury_account_identifier = faba.treasury_account_id
        LEFT JOIN
            global_temp.ref_program_activity AS rpa
                ON faba.program_activity_id = rpa.id
        WHERE
            faba.award_id IS NOT NULL
        GROUP BY
            faba.award_id
    ),
    state_fips AS (
        SELECT
            fips,
            code as state_code
        FROM
            global_temp.state_data
        GROUP BY
            fips,
            state_code
    ),
    county_fips AS (
        SELECT
            state_numeric,
            county_numeric,
            county_name,
            state_alpha
        FROM
            global_temp.ref_city_county_state_code
        GROUP BY
            state_numeric,
            county_numeric,
            county_name,
            state_alpha
    ),
    cd_city_grouped_rownum AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY(city_name, state_abbreviation) ORDER BY city_name, state_abbreviation ASC) AS row_num
      FROM global_temp.cd_city_grouped
    ),
    cd_city_grouped_distinct AS (
        SELECT
            city_name,
            state_abbreviation,
            congressional_district_no
        FROM cd_city_grouped_rownum
        WHERE row_num = 1
    )
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {", ".join([key for key in SUBAWARD_SEARCH_DELTA_COLUMNS])}
    )
    SELECT
        -- Broker Subaward Table Meta
        bs.created_at AS broker_created_at,
        bs.updated_at AS broker_updated_at,
        CAST(bs.id AS LONG) AS broker_subaward_id,

        -- Prime Award Fields (from Broker)
        bs.unique_award_key,
        bs.award_id AS award_piid_fain,
        bs.parent_award_id,
        CAST(bs.award_amount AS NUMERIC(23,2)),
        CAST(bs.action_date AS DATE),
        bs.fy,
        bs.awarding_agency_code,
        bs.awarding_agency_name,
        bs.awarding_sub_tier_agency_c,
        bs.awarding_sub_tier_agency_n,
        bs.awarding_office_code,
        bs.awarding_office_name,
        bs.funding_agency_code,
        bs.funding_agency_name,
        bs.funding_sub_tier_agency_co,
        bs.funding_sub_tier_agency_na,
        bs.funding_office_code,
        bs.funding_office_name,
        bs.awardee_or_recipient_uniqu,
        bs.awardee_or_recipient_uei,
        UPPER(bs.awardee_or_recipient_legal) AS awardee_or_recipient_legal,
        bs.dba_name,
        bs.ultimate_parent_unique_ide,
        bs.ultimate_parent_uei,
        bs.ultimate_parent_legal_enti,
        bs.legal_entity_country_code,
        bs.legal_entity_country_name,
        bs.legal_entity_state_code,
        bs.legal_entity_state_name,
        bs.legal_entity_zip,
        bs.legal_entity_county_code,
        bs.legal_entity_county_name,
        bs.legal_entity_congressional,
        bs.legal_entity_foreign_posta,
        bs.legal_entity_city_name,
        bs.legal_entity_address_line1,
        bs.business_types,
        bs.place_of_perform_country_co,
        bs.place_of_perform_country_na,
        bs.place_of_perform_state_code,
        bs.place_of_perform_state_name,
        bs.place_of_performance_zip,
        bs.place_of_performance_county_code AS place_of_perform_county_code,
        bs.place_of_performance_county_name AS place_of_perform_county_name,
        bs.place_of_perform_congressio,
        bs.place_of_perform_city_name,
        bs.place_of_perform_street,
        bs.award_description,
        bs.naics,
        bs.naics_description,
        bs.assistance_listing_numbers AS cfda_numbers,
        bs.assistance_listing_titles AS cfda_titles,

        -- Subaward Fields (from Broker)
        bs.subaward_type,
        CAST(bs.subaward_report_year AS SHORT),
        CAST(bs.subaward_report_month AS SHORT),
        UPPER(bs.subaward_number) AS subaward_number,
        COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00),
        CAST(bs.sub_action_date AS DATE),
        UPPER(bs.sub_awardee_or_recipient_uniqu) AS sub_awardee_or_recipient_uniqu,
        UPPER(bs.sub_awardee_or_recipient_uei) AS sub_awardee_or_recipient_uei,
        UPPER(bs.sub_awardee_or_recipient_legal) AS sub_awardee_or_recipient_legal_raw,
        UPPER(bs.sub_dba_name) AS sub_dba_name,
        UPPER(bs.sub_ultimate_parent_unique_ide) AS sub_ultimate_parent_unique_ide,
        UPPER(bs.sub_ultimate_parent_uei) AS sub_ultimate_parent_uei,
        UPPER(bs.sub_ultimate_parent_legal_enti) AS sub_ultimate_parent_legal_enti_raw,
        UPPER(bs.sub_legal_entity_country_code) AS sub_legal_entity_country_code_raw,
        bs.sub_legal_entity_country_name AS sub_legal_entity_country_name_raw,
        UPPER(bs.sub_legal_entity_state_code) AS sub_legal_entity_state_code,
        UPPER(bs.sub_legal_entity_state_name) AS sub_legal_entity_state_name,
        bs.sub_legal_entity_zip,
        bs.sub_legal_entity_county_code,
        UPPER(bs.sub_legal_entity_county_name) AS sub_legal_entity_county_name,
        UPPER(bs.sub_legal_entity_congressional) AS sub_legal_entity_congressional_raw,
        bs.sub_legal_entity_foreign_posta,
        UPPER(bs.sub_legal_entity_city_name) AS sub_legal_entity_city_name,
        UPPER(bs.sub_legal_entity_address_line1) AS sub_legal_entity_address_line1,
        UPPER(bs.sub_business_types) AS sub_business_types,
        UPPER(bs.sub_place_of_perform_country_co) AS sub_place_of_perform_country_co_raw,
        bs.sub_place_of_perform_country_na,
        UPPER(bs.sub_place_of_perform_state_code) AS sub_place_of_perform_state_code,
        UPPER(bs.sub_place_of_perform_state_name) AS sub_place_of_perform_state_name,
        bs.sub_place_of_performance_zip,
        bs.sub_place_of_performance_county_code AS sub_place_of_perform_county_code,
        UPPER(bs.sub_place_of_performance_county_name) AS sub_place_of_perform_county_name,
        UPPER(bs.sub_place_of_perform_congressio) AS sub_place_of_perform_congressio_raw,
        UPPER(bs.sub_place_of_perform_city_name) AS sub_place_of_perform_city_name,
        UPPER(bs.sub_place_of_perform_street) AS sub_place_of_perform_street,
        UPPER(bs.subaward_description) AS subaward_description,
        bs.sub_high_comp_officer1_full_na,
        CAST(bs.sub_high_comp_officer1_amount AS NUMERIC(23,2)),
        bs.sub_high_comp_officer2_full_na,
        CAST(bs.sub_high_comp_officer2_amount AS NUMERIC(23,2)),
        bs.sub_high_comp_officer3_full_na,
        CAST(bs.sub_high_comp_officer3_amount AS NUMERIC(23,2)),
        bs.sub_high_comp_officer4_full_na,
        CAST(bs.sub_high_comp_officer4_amount AS NUMERIC(23,2)),
        bs.sub_high_comp_officer5_full_na,
        CAST(bs.sub_high_comp_officer5_amount AS NUMERIC(23,2)),

        -- Additional Prime Award Fields (from Broker)
        bs.prime_id,
        UPPER(bs.internal_id) AS internal_id,
        CAST(bs.date_submitted AS TIMESTAMP),
        bs.report_type,
        bs.transaction_type,
        bs.program_title,
        bs.contract_agency_code,
        bs.contract_idv_agency_code,
        bs.grant_funding_agency_id,
        bs.grant_funding_agency_name,
        bs.federal_agency_name,
        bs.treasury_symbol,
        bs.dunsplus4,
        CAST(bs.recovery_model_q1 AS BOOLEAN),
        CAST(bs.recovery_model_q2 AS BOOLEAN),
        CAST(bs.compensation_q1 AS BOOLEAN),
        CAST(bs.compensation_q2 AS BOOLEAN),
        bs.high_comp_officer1_full_na,
        CAST(bs.high_comp_officer1_amount AS NUMERIC(23,2)),
        bs.high_comp_officer2_full_na,
        CAST(bs.high_comp_officer2_amount AS NUMERIC(23,2)),
        bs.high_comp_officer3_full_na,
        CAST(bs.high_comp_officer3_amount AS NUMERIC(23,2)),
        bs.high_comp_officer4_full_na,
        CAST(bs.high_comp_officer4_amount AS NUMERIC(23,2)),
        bs.high_comp_officer5_full_na,
        CAST(bs.high_comp_officer5_amount AS NUMERIC(23,2)),

        -- Additional Subaward Fields (from Broker)
        bs.sub_id,
        bs.sub_parent_id,
        bs.sub_federal_agency_id,
        bs.sub_federal_agency_name,
        bs.sub_funding_agency_id,
        bs.sub_funding_agency_name,
        bs.sub_funding_office_id,
        bs.sub_funding_office_name,
        bs.sub_naics,
        bs.sub_assistance_listing_numbers AS sub_cfda_numbers,
        bs.sub_dunsplus4,
        bs.sub_recovery_subcontract_amt,
        CAST(bs.sub_recovery_model_q1 AS BOOLEAN),
        CAST(bs.sub_recovery_model_q2 AS BOOLEAN),
        CAST(bs.sub_compensation_q1 AS BOOLEAN),
        CAST(bs.sub_compensation_q2 AS BOOLEAN),

        -- USAS Links (and associated derivations)
        a.id AS award_id,
        CASE
          WHEN bs.subaward_type = 'sub-grant' THEN 'grant'
          WHEN bs.subaward_type = 'sub-contract' THEN 'procurement'
          ELSE NULL
        END AS prime_award_group,
        a.type AS prime_award_type,
        CASE
            WHEN bs.subaward_type = 'sub-contract' THEN bs.award_id
            ELSE NULL
        END AS piid,
        CASE
          WHEN bs.subaward_type = 'sub-grant' THEN bs.award_id
          ELSE NULL
        END AS fain,
        a.latest_transaction_id,
        a.last_modified_date,

        a.awarding_agency_id,
        taa.name AS awarding_toptier_agency_name,
        taa.abbreviation AS awarding_toptier_agency_abbreviation,
        saa.name AS awarding_subtier_agency_name,
        saa.abbreviation AS awarding_subtier_agency_abbreviation,
        a.funding_agency_id,
        sfa.abbreviation AS funding_subtier_agency_abbreviation,
        sfa.name AS funding_subtier_agency_name,
        tfa.abbreviation AS funding_toptier_agency_abbreviation,
        tfa.name AS funding_toptier_agency_name,

        cfda.id AS cfda_id,
        cfda.program_number AS cfda_number,
        cfda.program_title AS cfda_title,

        -- USAS Derived Fields
        YEAR(CAST(bs.sub_action_date AS DATE) + interval '3 months') AS sub_fiscal_year,
        CASE
              WHEN CAST(bs.subaward_amount AS NUMERIC(23,2)) IS NULL THEN NULL
              WHEN COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00) < 1000000.0 THEN '<1M'         -- under $1 million
              WHEN COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00) = 1000000.0 THEN '1M'          -- $1 million
              WHEN COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00) < 25000000.0 THEN '1M..25M'     -- under $25 million
              WHEN COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00) = 25000000.0 THEN '25M'         -- $25 million
              WHEN COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00) < 100000000.0 THEN '25M..100M'   -- under $100 million
              WHEN COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00) = 100000000.0 THEN '100M'        -- $100 million
              WHEN COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00) < 500000000.0 THEN '100M..500M'  -- under $500 million
              WHEN COALESCE(CAST(bs.subaward_amount AS NUMERIC(23,2)), 0.00) = 500000000.0 THEN '500M'        -- $500 million
              ELSE '>500M'                               --  over $500 million
        END AS sub_total_obl_bin,
        UPPER(COALESCE(recipient_lookup.recipient_name, bs.sub_awardee_or_recipient_legal)) AS sub_awardee_or_recipient_legal,
        UPPER(COALESCE(parent_recipient_lookup.recipient_name, bs.sub_ultimate_parent_legal_enti)) AS sub_ultimate_parent_legal_enti,
        NULL AS business_type_code,
        COALESCE(tn.business_categories, array()) AS business_categories,
        tas.treasury_account_identifiers,
        fpds.pulled_from,
        fpds.type_of_contract_pricing,
        fpds.type_set_aside,
        fpds.extent_competed,
        fpds.product_or_service_code,
        psc.description AS product_or_service_description,

        LATEST_CURRENT_CD.recipient_location_congressional_code_current AS legal_entity_congressional_current,
        COALESCE(UPPER(bs.sub_legal_entity_country_code), 'USA') AS sub_legal_entity_country_code,
        rcc.country_name AS sub_legal_entity_country_name,
        LEFT(COALESCE(bs.sub_legal_entity_zip, ''), 5) AS sub_legal_entity_zip5,
        rec.census_code AS sub_legal_entity_city_code,
        LPAD(CAST(CAST(REGEXP_EXTRACT(UPPER(bs.sub_legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0') AS sub_legal_entity_congressional,
        (CASE
            WHEN (
                UPPER(bs.sub_legal_entity_country_code) <> 'USA'
            ) THEN NULL
            ELSE COALESCE(rl_cd_state_grouped.congressional_district_no, rl_zips.congressional_district_no, rl_cd_zips_grouped.congressional_district_no, rl_cd_city_grouped.congressional_district_no, rl_cd_county_grouped.congressional_district_no)
        END) AS sub_legal_entity_congressional_current,

        LATEST_CURRENT_CD.pop_congressional_code_current AS place_of_performance_congressional_current,
        fabs.place_of_performance_scope AS place_of_perform_scope,
        COALESCE(UPPER(bs.sub_place_of_perform_country_co), 'USA') AS sub_place_of_perform_country_co,
        pcc.country_name AS sub_place_of_perform_country_name,
        LEFT(COALESCE(sub_place_of_performance_zip, ''), 5) AS sub_place_of_perform_zip5,
        pop.census_code AS sub_place_of_perform_city_code,
        LPAD(CAST(CAST(REGEXP_EXTRACT(UPPER(bs.sub_place_of_perform_congressio), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0') AS sub_place_of_perform_congressio,
        (CASE
            WHEN (
                UPPER(bs.sub_place_of_perform_country_co) <> 'USA'
            ) THEN NULL
            ELSE COALESCE(pop_cd_state_grouped.congressional_district_no, pop_zips.congressional_district_no, pop_cd_zips_grouped.congressional_district_no, pop_cd_city_grouped.congressional_district_no, pop_cd_county_grouped.congressional_district_no)
        END) AS sub_place_of_performance_congressional_current,
        rl_state_fips.fips AS legal_entity_state_fips,
        pop_state_fips.fips AS place_of_perform_state_fips,
        CONCAT(rl_state_fips.fips, rl_county_fips.county_numeric) AS legal_entity_county_fips,
        CONCAT(pop_state_fips.fips, pop_county_fips.county_numeric) AS place_of_perform_county_fips,
        UPPER(COALESCE(fpds.place_of_perform_county_na, fabs.place_of_perform_county_na)) AS pop_county_name,
        tas.program_activities,
        RECIPIENT_HASH_AND_LEVEL.prime_award_recipient_id,
        SUB_RECIPIENT_HASH_AND_LEVEL.subaward_recipient_hash,
        SUB_RECIPIENT_HASH_AND_LEVEL.subaward_recipient_level,
        taa.toptier_code AS awarding_toptier_agency_code,
        tfa.toptier_code AS funding_toptier_agency_code
    FROM
        raw.subaward AS bs
    LEFT OUTER JOIN
        int.awards AS a
            ON a.generated_unique_award_id = bs.unique_award_key
    LEFT OUTER JOIN
        int.transaction_normalized AS tn
            ON tn.id = a.latest_transaction_id
    LEFT OUTER JOIN
        int.transaction_fpds AS fpds
            ON fpds.transaction_id = a.latest_transaction_id
    LEFT OUTER JOIN
        int.transaction_fabs AS fabs
            ON fabs.transaction_id = a.latest_transaction_id
    LEFT OUTER JOIN
        global_temp.agency AS aa
            ON aa.id = a.awarding_agency_id
    LEFT OUTER JOIN
        global_temp.toptier_agency AS taa
            ON taa.toptier_agency_id = aa.toptier_agency_id
    LEFT OUTER JOIN
        global_temp.subtier_agency AS saa
            ON saa.subtier_agency_id = aa.subtier_agency_id
    LEFT OUTER JOIN
        global_temp.agency AS fa
            ON fa.id = a.funding_agency_id
    LEFT OUTER JOIN
        global_temp.toptier_agency AS tfa
            ON tfa.toptier_agency_id = fa.toptier_agency_id
    LEFT OUTER JOIN
        global_temp.subtier_agency AS sfa
            ON sfa.subtier_agency_id = fa.subtier_agency_id
    LEFT OUTER JOIN
        tas_summary AS tas
            ON tas.award_id = a.id
    LEFT OUTER JOIN
        recipient_summary AS recipient_lookup
            ON (recipient_lookup.uei = UPPER(bs.sub_awardee_or_recipient_uei)
                AND bs.sub_awardee_or_recipient_uei IS NOT NULL AND recipient_lookup.row = 1)
    LEFT OUTER JOIN
        recipient_summary AS parent_recipient_lookup
            ON (parent_recipient_lookup.uei = UPPER(bs.sub_ultimate_parent_uei)
                AND bs.sub_ultimate_parent_uei IS NOT NULL AND parent_recipient_lookup.row = 1)
    LEFT OUTER JOIN
        int.transaction_current_cd_lookup AS LATEST_CURRENT_CD
            ON a.latest_transaction_id = LATEST_CURRENT_CD.transaction_id
    LEFT OUTER JOIN
        location_summary AS pop
            ON (pop.feature_name = UPPER(bs.sub_place_of_perform_city_name)
                AND pop.state_alpha = UPPER(bs.sub_place_of_perform_state_code)
                AND pop.row_num = 1)
    LEFT OUTER JOIN
        location_summary AS rec
            ON (rec.feature_name = UPPER(bs.sub_legal_entity_city_name)
                AND rec.state_alpha = UPPER(bs.sub_legal_entity_state_code)
                AND rec.row_num = 1)
    LEFT OUTER JOIN
        global_temp.ref_country_code AS pcc
            ON (pcc.country_code = UPPER(bs.sub_place_of_perform_country_co)
                AND bs.sub_place_of_perform_country_co IS NOT NULL)
    LEFT OUTER JOIN
        global_temp.ref_country_code AS rcc
            ON (rcc.country_code = UPPER(bs.sub_legal_entity_country_code)
                AND bs.sub_legal_entity_country_code IS NOT NULL)
    -- Congressional District '90' represents multiple congressional districts
    LEFT OUTER JOIN
        global_temp.cd_state_grouped pop_cd_state_grouped ON (
            pop_cd_state_grouped.state_abbreviation=UPPER(bs.sub_place_of_perform_state_code)
            AND pop_cd_state_grouped.congressional_district_no <> '90'
        )
    LEFT OUTER JOIN
        global_temp.cd_state_grouped rl_cd_state_grouped ON (
            rl_cd_state_grouped.state_abbreviation=UPPER(bs.sub_legal_entity_state_code)
            AND rl_cd_state_grouped.congressional_district_no <> '90'
        )
    LEFT OUTER JOIN
        raw.zips pop_zips ON (
            pop_zips.zip5=LEFT(COALESCE(sub_place_of_performance_zip, ''), 5)
            AND pop_zips.zip_last4=RIGHT(COALESCE(sub_place_of_performance_zip, ''), 4)
        )
    LEFT OUTER JOIN
        raw.zips rl_zips ON (
            rl_zips.zip5=LEFT(COALESCE(bs.sub_legal_entity_zip, ''), 5)
            AND rl_zips.zip_last4=RIGHT(COALESCE(bs.sub_legal_entity_zip, ''), 4)
        )
    LEFT OUTER JOIN
        global_temp.cd_zips_grouped pop_cd_zips_grouped ON (
            pop_cd_zips_grouped.zip5=LEFT(COALESCE(sub_place_of_performance_zip, ''), 5)
            AND pop_cd_zips_grouped.state_abbreviation=UPPER(bs.sub_place_of_perform_state_code)
        )
    LEFT OUTER JOIN
        global_temp.cd_zips_grouped rl_cd_zips_grouped ON (
            rl_cd_zips_grouped.zip5=LEFT(COALESCE(bs.sub_legal_entity_zip, ''), 5)
            AND rl_cd_zips_grouped.state_abbreviation=UPPER(bs.sub_legal_entity_state_code)
        )
    LEFT OUTER JOIN
        cd_city_grouped_distinct pop_cd_city_grouped ON (
            pop_cd_city_grouped.city_name=UPPER(bs.sub_place_of_perform_city_name)
            AND pop_cd_city_grouped.state_abbreviation=UPPER(bs.sub_place_of_perform_state_code)
        )
    LEFT OUTER JOIN
        cd_city_grouped_distinct rl_cd_city_grouped ON (
            rl_cd_city_grouped.city_name=UPPER(bs.sub_legal_entity_city_name)
            AND rl_cd_city_grouped.state_abbreviation=UPPER(bs.sub_legal_entity_state_code)
        )
    LEFT OUTER JOIN
        global_temp.cd_county_grouped pop_cd_county_grouped ON (
            pop_cd_county_grouped.county_number=LPAD(CAST(CAST(REGEXP_EXTRACT(pop.county_numeric, '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AND pop_cd_county_grouped.state_abbreviation=UPPER(bs.sub_place_of_perform_state_code)
        )
    LEFT OUTER JOIN
        global_temp.cd_county_grouped rl_cd_county_grouped ON (
            rl_cd_county_grouped.county_number=LPAD(CAST(CAST(REGEXP_EXTRACT(rec.county_numeric, '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AND rl_cd_county_grouped.state_abbreviation=UPPER(bs.sub_legal_entity_state_code)
        )
    LEFT OUTER JOIN
        global_temp.psc
            ON fpds.product_or_service_code = psc.code
    LEFT OUTER JOIN
        global_temp.references_cfda AS cfda
            ON cfda.program_number = split(bs.assistance_listing_numbers, ',')[0]
    LEFT OUTER JOIN state_fips AS pop_state_fips
        ON pop_state_fips.state_code = bs.place_of_perform_state_code
    LEFT OUTER JOIN state_fips AS rl_state_fips
        ON rl_state_fips.state_code = bs.legal_entity_state_code
    LEFT OUTER JOIN county_fips AS pop_county_fips
        ON UPPER(pop_county_fips.county_name) = UPPER(COALESCE(fpds.place_of_perform_county_na, fabs.place_of_perform_county_na))
            AND pop_county_fips.state_alpha = bs.place_of_perform_state_code
    LEFT OUTER JOIN county_fips AS rl_county_fips
        ON UPPER(rl_county_fips.county_name) = UPPER(COALESCE(fpds.legal_entity_county_name, fabs.legal_entity_county_name))
            AND rl_county_fips.state_alpha = bs.legal_entity_state_code
    LEFT OUTER JOIN (
        SELECT
            recipient_hash,
            recipient_level,
            recipient_hash || '-' || recipient_level AS prime_award_recipient_id
        FROM
            rpt.recipient_profile
        WHERE
            recipient_level != 'P'
            AND
            recipient_name NOT IN {special_cases}
    ) RECIPIENT_HASH_AND_LEVEL ON (
        RECIPIENT_HASH_AND_LEVEL.recipient_hash = REGEXP_REPLACE(
            MD5(
                UPPER(
                    CASE
                        WHEN bs.awardee_or_recipient_uei IS NOT NULL
                            THEN CONCAT("uei-", bs.awardee_or_recipient_uei)
                        WHEN bs.awardee_or_recipient_uniqu IS NOT NULL
                            THEN CONCAT("duns-", bs.awardee_or_recipient_uniqu)
                        ELSE
                            CONCAT("name-", COALESCE(bs.awardee_or_recipient_legal, ""))
                    END
                )
            ),
            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
            '\$1-\$2-\$3-\$4-\$5'
        )
        AND
        RECIPIENT_HASH_AND_LEVEL.recipient_level = CASE
            WHEN bs.ultimate_parent_uei IS NULL OR bs.ultimate_parent_uei = ''
                THEN 'R'
            ELSE 'C'
        END
    )
    LEFT OUTER JOIN (
        SELECT
            recipient_hash AS subaward_recipient_hash,
            recipient_level AS subaward_recipient_level
        FROM
            rpt.recipient_profile
        WHERE
            recipient_level != 'P'
            AND
            recipient_name NOT IN {special_cases}
    ) SUB_RECIPIENT_HASH_AND_LEVEL ON (
        SUB_RECIPIENT_HASH_AND_LEVEL.subaward_recipient_hash = REGEXP_REPLACE(
            MD5(
                UPPER(
                    CASE
                        WHEN bs.sub_awardee_or_recipient_uei IS NOT NULL
                            THEN CONCAT("uei-", bs.sub_awardee_or_recipient_uei)
                        WHEN bs.sub_awardee_or_recipient_uniqu IS NOT NULL
                            THEN CONCAT("duns-", bs.sub_awardee_or_recipient_uniqu)
                        ELSE
                            CONCAT("name-", COALESCE(bs.sub_awardee_or_recipient_legal, ""))
                    END
                )
            ),
            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
            '\$1-\$2-\$3-\$4-\$5'
        )
        AND
        SUB_RECIPIENT_HASH_AND_LEVEL.subaward_recipient_level = CASE
            WHEN bs.sub_ultimate_parent_uei IS NULL OR bs.sub_ultimate_parent_uei = ''
                THEN 'R'
            ELSE 'C'
        END
    )
    -- Subaward numbers are crucial for identifying subawards and so those without subaward numbers won't be surfaced.
    WHERE
        bs.subaward_number IS NOT NULL
        AND bs.sub_action_date IS NOT NULL
        AND bs.subaward_amount IS NOT NULL
"""
