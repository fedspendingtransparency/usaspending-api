# The order of these fields should always match the order of the
# SELECT statement in "transaction_search_load_sql_string"
TRANSACTION_SEARCH_COLUMNS = {
    "transaction_id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL"},
    "award_id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL"},
    "modification_number": {"delta": "STRING", "postgres": "TEXT"},
    "detached_award_proc_unique": {"delta": "STRING", "postgres": "TEXT"},
    "afa_generated_unique": {"delta": "STRING", "postgres": "TEXT"},
    "generated_unique_award_id": {"delta": "STRING", "postgres": "TEXT"},
    "fain": {"delta": "STRING", "postgres": "TEXT"},
    "uri": {"delta": "STRING", "postgres": "TEXT"},
    "piid": {"delta": "STRING", "postgres": "TEXT"},
    "action_date": {"delta": "DATE", "postgres": "DATE"},
    "fiscal_action_date": {"delta": "DATE", "postgres": "DATE"},
    "last_modified_date": {"delta": "DATE", "postgres": "DATE"},
    "fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER"},
    "award_certified_date": {"delta": "DATE", "postgres": "DATE"},
    "award_fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER"},
    "update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "award_update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "award_date_signed": {"delta": "DATE", "postgres": "DATE"},
    "etl_update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "period_of_performance_start_date": {"delta": "DATE", "postgres": "DATE"},
    "period_of_performance_current_end_date": {"delta": "DATE", "postgres": "DATE"},
    "type": {"delta": "STRING", "postgres": "TEXT"},
    "type_description": {"delta": "STRING", "postgres": "TEXT"},
    "award_category": {"delta": "STRING", "postgres": "TEXT"},
    "transaction_description": {"delta": "STRING", "postgres": "TEXT"},
    "award_amount": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "generated_pragmatic_obligation": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "federal_action_obligation": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "original_loan_subsidy_cost": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "face_value_loan_guarantee": {"delta": "NUMERIC(23,2)", "postgres": "NUMERIC(23,2)"},
    "business_categories": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "naics_code": {"delta": "STRING", "postgres": "TEXT"},
    "naics_description": {"delta": "STRING", "postgres": "TEXT"},
    "product_or_service_code": {"delta": "STRING", "postgres": "TEXT"},
    "product_or_service_description": {"delta": "STRING", "postgres": "TEXT"},
    "type_of_contract_pricing": {"delta": "STRING", "postgres": "TEXT"},
    "type_set_aside": {"delta": "STRING", "postgres": "TEXT"},
    "extent_competed": {"delta": "STRING", "postgres": "TEXT"},
    "ordering_period_end_date": {"delta": "STRING", "postgres": "TEXT"},
    "cfda_number": {"delta": "STRING", "postgres": "TEXT"},
    "cfda_title": {"delta": "STRING", "postgres": "TEXT"},
    "cfda_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "pop_country_name": {"delta": "STRING", "postgres": "TEXT"},
    "pop_country_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_state_name": {"delta": "STRING", "postgres": "TEXT"},
    "pop_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_county_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "pop_zip5": {"delta": "STRING", "postgres": "TEXT"},
    "pop_congressional_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_congressional_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "pop_county_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "pop_state_fips": {"delta": "STRING", "postgres": "TEXT"},
    "pop_state_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "pop_city_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_country_code": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_country_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_state_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_state_fips": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_state_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "recipient_location_county_code": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_county_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "recipient_location_congressional_code": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_congressional_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "recipient_location_zip5": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_city_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_hash": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_levels": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "recipient_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_unique_id": {"delta": "STRING", "postgres": "TEXT"},
    "parent_recipient_hash": {"delta": "STRING", "postgres": "TEXT"},
    "parent_recipient_name": {"delta": "STRING", "postgres": "TEXT"},
    "parent_recipient_unique_id": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_uei": {"delta": "STRING", "postgres": "TEXT"},
    "parent_uei": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_toptier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_toptier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "awarding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "awarding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_toptier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_subtier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "funding_subtier_agency_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "treasury_account_identifiers": {"delta": "ARRAY<INTEGER>", "postgres": "TEXT[]"},
    "tas_paths": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "tas_components": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "federal_accounts": {"delta": "STRING", "postgres": "JSONB"},
    "disaster_emergency_fund_codes": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "awarding_office_code": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_office_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_office_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_office_name": {"delta": "STRING", "postgres": "TEXT"},
}
TRANSACTION_SEARCH_DELTA_COLUMNS = {k: v["delta"] for k, v in TRANSACTION_SEARCH_COLUMNS.items()}
TRANSACTION_SEARCH_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in TRANSACTION_SEARCH_COLUMNS.items()}

transaction_search_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in TRANSACTION_SEARCH_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""

transaction_search_load_sql_string = rf"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join([col for col in TRANSACTION_SEARCH_DELTA_COLUMNS])}
    )
    SELECT
        transaction_normalized.id AS transaction_id,
        transaction_normalized.award_id,
        transaction_normalized.modification_number,
        transaction_fpds.detached_award_proc_unique,
        transaction_fabs.afa_generated_unique,
        awards.generated_unique_award_id,
        awards.fain,
        awards.uri,
        awards.piid,

        DATE(transaction_normalized.action_date) AS action_date,
        DATE(transaction_normalized.action_date + interval '3 months') AS fiscal_action_date,
        DATE(transaction_normalized.last_modified_date) AS last_modified_date,
        transaction_normalized.fiscal_year,
        awards.certified_date AS award_certified_date,
        YEAR(awards.certified_date + interval '3 months') AS award_fiscal_year,
        transaction_normalized.update_date,
        awards.update_date AS award_update_date,
        DATE(awards.date_signed) AS award_date_signed,
        GREATEST(transaction_normalized.update_date, awards.update_date) AS etl_update_date,
        awards.period_of_performance_start_date,
        awards.period_of_performance_current_end_date,

        transaction_normalized.type,
        awards.type_description,
        awards.category AS award_category,
        transaction_normalized.description AS transaction_description,

        CAST(COALESCE(
            CASE
                WHEN transaction_normalized.type IN('07','08') THEN awards.total_subsidy_cost
                ELSE awards.total_obligation
            END,
            0
        ) AS NUMERIC(23, 2)) AS award_amount,
        CAST(COALESCE(
            CASE
                WHEN transaction_normalized.type IN('07','08') THEN transaction_normalized.original_loan_subsidy_cost
                ELSE transaction_normalized.federal_action_obligation
            END,
            0
        ) AS NUMERIC(23, 2)) AS generated_pragmatic_obligation,
        CAST(COALESCE(transaction_normalized.federal_action_obligation, 0) AS NUMERIC(23, 2))
            AS federal_action_obligation,
        CAST(COALESCE(transaction_normalized.original_loan_subsidy_cost, 0) AS NUMERIC(23, 2))
            AS original_loan_subsidy_cost,
        CAST(COALESCE(transaction_normalized.face_value_loan_guarantee, 0) AS NUMERIC(23, 2))
            AS face_value_loan_guarantee,

        transaction_normalized.business_categories,
        transaction_fpds.naics AS naics_code,
        naics.description AS naics_description,
        transaction_fpds.product_or_service_code,
        psc.description AS product_or_service_description,
        transaction_fpds.type_of_contract_pricing,
        transaction_fpds.type_set_aside,
        transaction_fpds.extent_competed,
        transaction_fpds.ordering_period_end_date,
        transaction_fabs.cfda_number,
        transaction_fabs.cfda_title,
        references_cfda.id AS cfda_id,

        pop_country_lookup.country_name AS pop_country_name,
        pop_country_lookup.country_code AS pop_country_code,
        POP_STATE_LOOKUP.name AS pop_state_name,
        COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
            AS pop_state_code,
        LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AS pop_county_code,
        COALESCE(pop_county_lookup.county_name, transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na)
            AS pop_county_name,
        COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5)
            AS pop_zip5,
        LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
            AS pop_congressional_code,
        POP_DISTRICT_POPULATION.latest_population AS pop_congressional_population,
        POP_COUNTY_POPULATION.latest_population AS pop_county_population,
        POP_STATE_LOOKUP.fips AS pop_state_fips,
        POP_STATE_POPULATION.latest_population AS pop_state_population,
        TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city))
            AS pop_city_name,

        rl_country_lookup.country_code AS recipient_location_country_code,
        rl_country_lookup.country_name AS recipient_location_country_name,
        RL_STATE_LOOKUP.name AS recipient_location_state_name,
        COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
            AS recipient_location_state_code,
        RL_STATE_LOOKUP.fips AS recipient_location_state_fips,
        RL_STATE_POPULATION.latest_population AS recipient_location_state_population,
        LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AS recipient_location_county_code,
        COALESCE(rl_county_lookup.county_name, transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name)
            AS recipient_location_county_name,
        RL_COUNTY_POPULATION.latest_population AS recipient_location_county_population,
        LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
            AS recipient_location_congressional_code,
        RL_DISTRICT_POPULATION.latest_population AS recipient_location_congressional_population,
        COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5)
            AS recipient_location_zip5,
        TRIM(TRAILING FROM COALESCE(transaction_fpds.legal_entity_city_name, transaction_fabs.legal_entity_city_name))
            AS recipient_location_city_name,

        COALESCE(
            recipient_lookup.recipient_hash,
            REGEXP_REPLACE(MD5(UPPER(
                CASE
                    WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL
                        THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
                    WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
                        THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
                    ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
                END
            )), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
        ) AS recipient_hash,
        RECIPIENT_HASH_AND_LEVELS.recipient_levels,
        UPPER(COALESCE(
            recipient_lookup.legal_business_name,
            transaction_fpds.awardee_or_recipient_legal,
            transaction_fabs.awardee_or_recipient_legal
        )) AS recipient_name,
        COALESCE(
            recipient_lookup.duns,
            transaction_fpds.awardee_or_recipient_uniqu,
            transaction_fabs.awardee_or_recipient_uniqu
        ) AS recipient_unique_id,
        PRL.recipient_hash AS parent_recipient_hash,
        UPPER(PRL.legal_business_name) AS parent_recipient_name,
        COALESCE(
            PRL.duns,
            transaction_fpds.ultimate_parent_unique_ide,
            transaction_fabs.ultimate_parent_unique_ide
        ) AS parent_recipient_unique_id,
        COALESCE(
            recipient_lookup.uei,
            transaction_fpds.awardee_or_recipient_uei,
            transaction_fabs.uei
        ) AS recipient_uei,
        COALESCE(
            PRL.uei,
            transaction_fpds.ultimate_parent_uei,
            transaction_fabs.ultimate_parent_uei
        ) AS parent_uei,

        AA_ID.id AS awarding_toptier_agency_id,
        FA_ID.id AS funding_toptier_agency_id,
        transaction_normalized.awarding_agency_id,
        transaction_normalized.funding_agency_id,
        TAA.name AS awarding_toptier_agency_name,
        TFA.name AS funding_toptier_agency_name,
        SAA.name AS awarding_subtier_agency_name,
        SFA.name AS funding_subtier_agency_name,
        TAA.abbreviation AS awarding_toptier_agency_abbreviation,
        TFA.abbreviation AS funding_toptier_agency_abbreviation,
        SAA.abbreviation AS awarding_subtier_agency_abbreviation,
        SFA.abbreviation AS funding_subtier_agency_abbreviation,

        FED_AND_TRES_ACCT.treasury_account_identifiers,
        FED_AND_TRES_ACCT.tas_paths,
        FED_AND_TRES_ACCT.tas_components,
        FED_AND_TRES_ACCT.federal_accounts,
        FED_AND_TRES_ACCT.disaster_emergency_fund_codes,

        AO.office_code AS awarding_office_code,
        AO.office_name AS awarding_office_name,
        FO.office_code AS funding_office_code,
        FO.office_name AS funding_office_name
    FROM
        raw.transaction_normalized
    LEFT OUTER JOIN
        raw.transaction_fabs ON (transaction_normalized.id = transaction_fabs.transaction_id AND transaction_normalized.is_fpds = false)
    LEFT OUTER JOIN
        raw.transaction_fpds ON (transaction_normalized.id = transaction_fpds.transaction_id AND transaction_normalized.is_fpds = true)
    LEFT OUTER JOIN
        global_temp.references_cfda ON (transaction_fabs.cfda_number = references_cfda.program_number)
    LEFT OUTER JOIN
        raw.recipient_lookup ON (
            recipient_lookup.recipient_hash = REGEXP_REPLACE(MD5(UPPER(
                CASE
                    WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL
                        THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
                    WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
                        THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
                    ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
                END
            )), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
        )
    LEFT OUTER JOIN
        raw.awards ON (transaction_normalized.award_id = awards.id)
    LEFT OUTER JOIN
        global_temp.agency AS AA ON (transaction_normalized.awarding_agency_id = AA.id)
    LEFT OUTER JOIN
        global_temp.toptier_agency AS TAA ON (AA.toptier_agency_id = TAA.toptier_agency_id)
    LEFT OUTER JOIN
        global_temp.subtier_agency AS SAA ON (AA.subtier_agency_id = SAA.subtier_agency_id)
    LEFT OUTER JOIN
        global_temp.agency AS AA_ID ON (AA_ID.toptier_agency_id = TAA.toptier_agency_id AND AA_ID.toptier_flag = TRUE)
    LEFT OUTER JOIN
        global_temp.agency AS FA ON (transaction_normalized.funding_agency_id = FA.id)
    LEFT OUTER JOIN
        global_temp.toptier_agency AS TFA ON (FA.toptier_agency_id = TFA.toptier_agency_id)
    LEFT OUTER JOIN
        global_temp.subtier_agency AS SFA ON (FA.subtier_agency_id = SFA.subtier_agency_id)
   LEFT OUTER JOIN
        (SELECT id, toptier_agency_id, ROW_NUMBER() OVER (PARTITION BY toptier_agency_id ORDER BY toptier_flag DESC, id ASC) AS row_num FROM global_temp.agency) AS FA_ID
        ON (FA_ID.toptier_agency_id = TFA.toptier_agency_id AND row_num = 1)
    LEFT OUTER JOIN
        global_temp.naics ON (transaction_fpds.naics = naics.code)
    LEFT OUTER JOIN
        global_temp.psc ON (transaction_fpds.product_or_service_code = psc.code)
    LEFT OUTER JOIN (
        SELECT DISTINCT state_alpha, county_numeric, UPPER(county_name) AS county_name
        FROM global_temp.ref_city_county_state_code
    ) AS rl_county_lookup ON (
        rl_county_lookup.state_alpha = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
        AND rl_county_lookup.county_numeric = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
    )
    LEFT OUTER JOIN (
        SELECT DISTINCT state_alpha, county_numeric, UPPER(county_name) AS county_name
        FROM global_temp.ref_city_county_state_code
    ) AS pop_county_lookup ON (
        pop_county_lookup.state_alpha = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
        AND pop_county_lookup.county_numeric = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
    )
    -- THIS JOIN HAPPENS ON "country_name" -> "country_code" intentionally to handle bad data
    LEFT OUTER JOIN
        global_temp.ref_country_code AS pop_country_lookup ON (
            pop_country_lookup.country_code = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA')
            OR pop_country_lookup.country_name = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c)
        )
    -- THIS JOIN HAPPENS ON "country_name" -> "country_code" intentionally to handle bad data
    LEFT OUTER JOIN
        global_temp.ref_country_code AS rl_country_lookup ON (
            rl_country_lookup.country_code = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code, 'USA')
            OR rl_country_lookup.country_name = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code)
        )
    LEFT OUTER JOIN
        raw.recipient_lookup PRL ON (
            PRL.recipient_hash = REGEXP_REPLACE(MD5(UPPER(
                CASE
                    WHEN COALESCE(transaction_fpds.ultimate_parent_uei, transaction_fabs.ultimate_parent_uei) IS NOT NULL
                        THEN CONCAT('uei-', COALESCE(transaction_fpds.ultimate_parent_uei, transaction_fabs.ultimate_parent_uei))
                    WHEN COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) IS NOT NULL
                        THEN CONCAT('duns-', COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide))
                    ELSE CONCAT('name-', COALESCE(transaction_fpds.ultimate_parent_legal_enti, transaction_fabs.ultimate_parent_legal_enti, ''))
                END
            )), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
        )
    LEFT OUTER JOIN (
        SELECT recipient_hash, uei, SORT_ARRAY(COLLECT_SET(recipient_level)) AS recipient_levels
        FROM raw.recipient_profile
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
        SELECT code, name, fips, MAX(id)
        FROM global_temp.state_data
        GROUP BY code, name, fips
    ) POP_STATE_LOOKUP ON (
        POP_STATE_LOOKUP.code = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
    )
    LEFT OUTER JOIN
        global_temp.ref_population_county POP_STATE_POPULATION ON (
            POP_STATE_POPULATION.state_code = POP_STATE_LOOKUP.fips
            AND POP_STATE_POPULATION.county_number = '000'
        )
    LEFT OUTER JOIN
        global_temp.ref_population_county POP_COUNTY_POPULATION ON (
            POP_COUNTY_POPULATION.state_code = POP_STATE_LOOKUP.fips
            AND POP_COUNTY_POPULATION.county_number = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
        )
    LEFT OUTER JOIN
        global_temp.ref_population_cong_district POP_DISTRICT_POPULATION ON (
            POP_DISTRICT_POPULATION.state_code = POP_STATE_LOOKUP.fips
            AND POP_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
        )
    LEFT OUTER JOIN (
        SELECT code, name, fips, MAX(id)
        FROM global_temp.state_data
        GROUP BY code, name, fips
    ) RL_STATE_LOOKUP ON (
        RL_STATE_LOOKUP.code = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
    )
    LEFT OUTER JOIN
        global_temp.ref_population_county RL_STATE_POPULATION ON (
            RL_STATE_POPULATION.state_code = RL_STATE_LOOKUP.fips
            AND RL_STATE_POPULATION.county_number = '000'
        )
    LEFT OUTER JOIN
        global_temp.ref_population_county RL_COUNTY_POPULATION ON (
            RL_COUNTY_POPULATION.state_code = RL_STATE_LOOKUP.fips
            AND RL_COUNTY_POPULATION.county_number = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
        )
    LEFT OUTER JOIN
        global_temp.ref_population_cong_district RL_DISTRICT_POPULATION ON (
            RL_DISTRICT_POPULATION.state_code = RL_STATE_LOOKUP.fips
            AND RL_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
        )
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
            -- "CASE" put in place so that Spark value matches Postgres; can most likely be refactored out in the future
            CASE
                WHEN SIZE(COLLECT_SET(faba.disaster_emergency_fund_code)) > 0
                    THEN SORT_ARRAY(COLLECT_SET(faba.disaster_emergency_fund_code))
                ELSE NULL
            END AS disaster_emergency_fund_codes,
            SORT_ARRAY(COLLECT_SET(taa.treasury_account_identifier)) treasury_account_identifiers,
            SORT_ARRAY(
                COLLECT_SET(
                    CONCAT(
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
                )
            ) AS tas_paths,
            SORT_ARRAY(
                COLLECT_SET(
                    CONCAT(
                        'aid=', COALESCE(taa.agency_id, ''),
                        'main=', COALESCE(taa.main_account_code, ''),
                        'ata=', COALESCE(taa.allocation_transfer_agency_id, ''),
                        'sub=', COALESCE(taa.sub_account_code, ''),
                        'bpoa=', COALESCE(taa.beginning_period_of_availability, ''),
                        'epoa=', COALESCE(taa.ending_period_of_availability, ''),
                        'a=', COALESCE(taa.availability_type_code, '')
                    )
                ),
                TRUE
            ) AS tas_components
        FROM raw.financial_accounts_by_awards AS faba
        INNER JOIN global_temp.treasury_appropriation_account AS taa ON taa.treasury_account_identifier = faba.treasury_account_id
        INNER JOIN global_temp.federal_account AS fa ON fa.id = taa.federal_account_id
        INNER JOIN global_temp.toptier_agency agency ON (fa.parent_toptier_agency_id = agency.toptier_agency_id)
        WHERE faba.award_id IS NOT NULL
        GROUP BY faba.award_id
    ) FED_AND_TRES_ACCT ON (FED_AND_TRES_ACCT.award_id = transaction_normalized.award_id)
    LEFT OUTER JOIN
        global_temp.office AO ON COALESCE(transaction_fpds.awarding_office_code, transaction_fabs.awarding_office_code) = AO.office_code
    LEFT OUTER JOIN
        global_temp.office FO ON COALESCE(transaction_fpds.funding_office_code, transaction_fabs.funding_office_code) = FO.office_code
    WHERE
        transaction_normalized.action_date >= '2000-10-01'
"""
