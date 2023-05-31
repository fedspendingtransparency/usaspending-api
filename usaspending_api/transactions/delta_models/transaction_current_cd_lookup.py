# The order of these fields should always match the order of the
# SELECT statement in "transaction_current_cd_lookup_load_sql_string"
TRANSACTION_CURRENT_CD_LOOKUP_COLUMNS = {
    "transaction_id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL"},
    "recipient_location_congressional_code_current": {"delta": "STRING", "postgres": "TEXT"},
    "pop_congressional_code_current": {"delta": "STRING", "postgres": "TEXT"},
}
TRANSACTION_CURRENT_CD_LOOKUP_DELTA_COLUMNS = {k: v["delta"] for k, v in TRANSACTION_CURRENT_CD_LOOKUP_COLUMNS.items()}

transaction_current_cd_lookup_create_sql_string = fr"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in TRANSACTION_CURRENT_CD_LOOKUP_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""

transaction_current_cd_lookup_load_sql_string = fr"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join([col for col in TRANSACTION_CURRENT_CD_LOOKUP_COLUMNS])}
    )
    SELECT
        transaction_normalized.id AS transaction_id,
        (CASE
            WHEN (
                COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code) <> 'USA'
            ) THEN NULL
            ELSE COALESCE(rl_cd_state_grouped.congressional_district_no, rl_zips.congressional_district_no, rl_cd_zips_grouped.congressional_district_no, rl_cd_city_grouped.congressional_district_no, rl_cd_county_grouped.congressional_district_no)
        END) AS recipient_location_congressional_code_current,
        (CASE
            WHEN (
                transaction_fabs.place_of_performance_scope = 'Foreign'
                OR transaction_fpds.place_of_perform_country_c <> 'USA'
            ) THEN NULL
            WHEN (
                transaction_fabs.place_of_performance_scope = 'Multi-State'
            ) THEN '90'
            WHEN (
                (transaction_fabs.place_of_performance_scope = 'State-wide'
                OR transaction_fpds.place_of_perform_country_c = 'USA')
                AND pop_cd_state_grouped.congressional_district_no IS NOT NULL
            ) THEN pop_cd_state_grouped.congressional_district_no
            WHEN (
                (transaction_fabs.place_of_performance_scope = 'Single ZIP Code'
                OR transaction_fpds.place_of_perform_country_c = 'USA')
                AND COALESCE(pop_zips.congressional_district_no, pop_cd_zips_grouped.congressional_district_no) IS NOT NULL
            ) THEN COALESCE(pop_zips.congressional_district_no, pop_cd_zips_grouped.congressional_district_no)
            WHEN (
                (transaction_fabs.place_of_performance_scope = 'City-wide'
                OR transaction_fpds.place_of_perform_country_c = 'USA')
                AND pop_cd_city_grouped.congressional_district_no IS NOT NULL
            ) THEN pop_cd_city_grouped.congressional_district_no
            WHEN (
                (transaction_fabs.place_of_performance_scope = 'County-wide'
                OR transaction_fpds.place_of_perform_country_c = 'USA')
                AND pop_cd_county_grouped.congressional_district_no IS NOT NULL
            ) THEN pop_cd_county_grouped.congressional_district_no
            ELSE NULL
        END) AS pop_congressional_code_current
    FROM
        int.transaction_normalized
    LEFT OUTER JOIN
        int.transaction_fabs ON (transaction_normalized.id = transaction_fabs.transaction_id AND transaction_normalized.is_fpds = false)
    LEFT OUTER JOIN
        int.transaction_fpds ON (transaction_normalized.id = transaction_fpds.transaction_id AND transaction_normalized.is_fpds = true)
    LEFT OUTER JOIN
        global_temp.cd_state_grouped pop_cd_state_grouped ON (
            pop_cd_state_grouped.state_abbreviation=COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
            AND pop_cd_state_grouped.congressional_district_no <> '90'
        )
    LEFT OUTER JOIN
        global_temp.cd_state_grouped rl_cd_state_grouped ON (
            rl_cd_state_grouped.state_abbreviation=COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
            AND rl_cd_state_grouped.congressional_district_no <> '90'
        )
    LEFT OUTER JOIN
        raw.zips pop_zips ON (
            pop_zips.zip5=COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5)
            AND pop_zips.zip_last4=COALESCE(transaction_fpds.place_of_perform_zip_last4, transaction_fabs.place_of_perform_zip_last4)
        )
    LEFT OUTER JOIN
        raw.zips rl_zips ON (
            rl_zips.zip5=COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5)
            AND rl_zips.zip_last4=COALESCE(transaction_fpds.legal_entity_zip_last4, transaction_fabs.legal_entity_zip_last4)
        )
    LEFT OUTER JOIN
        global_temp.cd_zips_grouped pop_cd_zips_grouped ON (
            pop_cd_zips_grouped.zip5=COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5)
            AND pop_cd_zips_grouped.state_abbreviation=COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
        )
    LEFT OUTER JOIN
        global_temp.cd_zips_grouped rl_cd_zips_grouped ON (
            rl_cd_zips_grouped.zip5=COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5)
            AND rl_cd_zips_grouped.state_abbreviation=COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
        )
    LEFT OUTER JOIN
        global_temp.cd_city_grouped pop_cd_city_grouped ON (
            pop_cd_city_grouped.city_name=UPPER(TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city)))
            AND pop_cd_city_grouped.state_abbreviation=COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
        )
    LEFT OUTER JOIN
        global_temp.cd_city_grouped rl_cd_city_grouped ON (
            rl_cd_city_grouped.city_name=UPPER(TRIM(TRAILING FROM COALESCE(transaction_fpds.legal_entity_city_name, transaction_fabs.legal_entity_city_name)))
            AND rl_cd_city_grouped.state_abbreviation=COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
        )
    LEFT OUTER JOIN
        global_temp.cd_county_grouped pop_cd_county_grouped ON (
            pop_cd_county_grouped.county_number=LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AND pop_cd_county_grouped.state_abbreviation=COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code)
        )
    LEFT OUTER JOIN
        global_temp.cd_county_grouped rl_cd_county_grouped ON (
            rl_cd_county_grouped.county_number=LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AND rl_cd_county_grouped.state_abbreviation=COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code)
        )
"""
