from usaspending_api.awards.v2.lookups.lookups import all_awards_types_to_category

AWARD_SEARCH_COLUMNS = {
    "treasury_account_identifiers": {"delta": "ARRAY<INTEGER>", "postgres": "[INTEGER]"},
    "award_id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL"},
    "category": {"delta": "STRING", "postgres": "TEXT"},
    "type": {"delta": "STRING", "postgres": "TEXT"},
    "type_description": {"delta": "STRING", "postgres": "TEXT"},
    "generated_unique_award_id": {"delta": "STRING", "postgres": "TEXT"},
    "display_award_id": {"delta": "STRING", "postgres": "TEXT"},
    "update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "piid": {"delta": "STRING", "postgres": "TEXT"},
    "fain": {"delta": "STRING", "postgres": "TEXT"},
    "uri": {"delta": "STRING", "postgres": "TEXT"},
    "award_amount": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)"},
    "total_obligation": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)"},
    "description": {"delta": "STRING", "postgres": "TEXT"},
    "total_obl_bin": {"delta": "STRING", "postgres": "TEXT"},
    "total_subsidy_cost": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)"},
    "total_loan_value": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)"},
    "recipient_hash": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_levels": {"delta": "ARRAY<STRING>", "postgres": "[TEXT]"},
    "recipient_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_unique_id": {"delta": "STRING", "postgres": "TEXT"},
    "parent_recipient_unique_id": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_uei": {"delta": "STRING", "postgres": "TEXT"},
    "parent_uei": {"delta": "STRING", "postgres": "TEXT"},
    "business_categories": {"delta": "ARRAY<STRING>", "postgres": "[TEXT]"},
    "action_date": {"delta": "DATE", "postgres": "DATE"},
    "fiscal_year": {"delta": "INTEGER", "postgres": "INTEGER"},
    "last_modified_date": {"delta": "DATE", "postgres": "DATE"},
    "period_of_performance_start_date": {"delta": "DATE", "postgres": "DATE"},
    "period_of_performance_current_end_date": {"delta": "DATE", "postgres": "DATE"},
    "date_signed": {"delta": "DATE", "postgres": "DATE"},
    "ordering_period_end_date": {"delta": "DATE", "postgres": "DATE"},
    "original_loan_subsidy_cost": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)"},
    "face_value_loan_guarantee": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)"},
    "awarding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "awarding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_subtier_agency_name": {"delta": " STRING", "postgres": " STRING"},
    "funding_subtier_agency_name": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_toptier_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "awarding_subtier_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_subtier_agency_code": {"delta": "STRING", "postgres": "TEXT"},
    "funding_toptier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "funding_subtier_agency_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "recipient_location_country_code": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_country_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_county_code": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_congressional_code": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_zip5": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_city_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_state_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_state_fips": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_location_state_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "recipient_location_county_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "recipient_location_congressional_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "pop_country_name": {"delta": "STRING", "postgres": "TEXT"},
    "pop_country_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_state_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_county_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_county_name": {"delta": "STRING", "postgres": "TEXT"},
    "pop_city_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_zip5": {"delta": "STRING", "postgres": "TEXT"},
    "pop_congressional_code": {"delta": "STRING", "postgres": "TEXT"},
    "pop_city_name": {"delta": "STRING", "postgres": "TEXT"},
    "pop_state_name": {"delta": "STRING", "postgres": "TEXT"},
    "pop_state_fips": {"delta": "STRING", "postgres": "TEXT"},
    "pop_state_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "pop_county_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "pop_congressional_population": {"delta": "INTEGER", "postgres": "INTEGER"},
    "cfda_program_title": {"delta": "STRING", "postgres": "TEXT"},
    "cfda_number": {"delta": "STRING", "postgres": "TEXT"},
    "cfdas": {"delta": "ARRAY<STRING>", "postgres": "[TEXT]"},
    "sai_number": {"delta": "STRING", "postgres": "TEXT"},
    "type_of_contract_pricing": {"delta": "STRING", "postgres": "TEXT"},
    "extent_competed": {"delta": "STRING", "postgres": "TEXT"},
    "type_set_aside": {"delta": "STRING", "postgres": "TEXT"},
    "product_or_service_code": {"delta": "STRING", "postgres": "TEXT"},
    "product_or_service_description": {"delta": "STRING", "postgres": "TEXT"},
    "naics_code": {"delta": "STRING", "postgres": "TEXT"},
    "naics_description": {"delta": "STRING", "postgres": "TEXT"},
    "tas_paths": {"delta": "ARRAY<STRING>", "postgres": "[TEXT]"},
    "tas_components": {"delta": "ARRAY<STRING>", "postgres": "[TEXT]"},
    "disaster_emergency_fund_codes": {"delta": "ARRAY<STRING>", "postgres": "[TEXT]"},
    "covid_spending_by_defc": {"delta": "STRING", "postgres": "TEXT"},
    "total_covid_outlay": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)"},
    "total_covid_obligation": {"delta": "NUMERIC(23, 2)", "postgres": "NUMERIC(23, 2)"},
}
AWARD_SEARCH_DELTA_COLUMNS = {k: v["delta"] for k, v in AWARD_SEARCH_COLUMNS.items()}
AWARD_SEARCH_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in AWARD_SEARCH_COLUMNS.items()}

award_search_create_sql_string = fr"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in AWARD_SEARCH_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""

award_search_load_sql_string = fr"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
        (
            {",".join([col for col in AWARD_SEARCH_DELTA_COLUMNS])}
        )
    SELECT
  TREASURY_ACCT.treasury_account_identifiers,
  awards.id AS award_id,
  awards.category,
  awards.type,
  awards.type_description,
  awards.generated_unique_award_id,
  CASE
    WHEN awards.type IN ('02', '03', '04', '05', '06', '10', '07', '08', '09', '11') AND awards.fain IS NOT NULL THEN awards.fain
    WHEN awards.piid IS NOT NULL THEN awards.piid  -- contracts. Did it this way to easily handle IDV contracts
    ELSE awards.uri
  END AS display_award_id,
  awards.update_date,
  awards.piid,
  awards.fain AS fain,
  awards.uri AS uri,
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
  RECIPIENT_HASH_AND_LEVELS.recipient_hash,
  RECIPIENT_HASH_AND_LEVELS.recipient_levels,
  UPPER(COALESCE(recipient_lookup.legal_business_name, transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) AS recipient_name,
  COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AS recipient_unique_id,
  COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id,
  COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) AS recipient_uei,
  COALESCE(transaction_fpds.ultimate_parent_uei, transaction_fabs.ultimate_parent_uei) AS parent_uei,
  latest_transaction.business_categories,

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
  TAA.toptier_code AS awarding_toptier_agency_code,
  TFA.toptier_code AS funding_toptier_agency_code,
  SAA.subtier_code AS awarding_subtier_agency_code,
  SFA.subtier_code AS funding_subtier_agency_code,
  FA_ID.id AS funding_toptier_agency_id,
  latest_transaction.funding_agency_id AS funding_subtier_agency_id,

  rl_country_lookup.country_code AS recipient_location_country_code,
  rl_country_lookup.country_name AS recipient_location_country_name,
  COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) AS recipient_location_state_code,
  LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AS recipient_location_county_code,
  COALESCE(rl_county_lookup.county_name, transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name) AS recipient_location_county_name,
  LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
            AS recipient_location_congressional_code,
  COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5) AS recipient_location_zip5,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.legal_entity_city_name, transaction_fabs.legal_entity_city_name)) AS recipient_location_city_name,
  RL_STATE_LOOKUP.name AS recipient_location_state_name,
  RL_STATE_LOOKUP.fips AS recipient_location_state_fips,
  RL_STATE_POPULATION.latest_population AS recipient_location_state_population,
  RL_COUNTY_POPULATION.latest_population AS recipient_location_county_population,
  RL_DISTRICT_POPULATION.latest_population AS recipient_location_congressional_population,

  pop_country_lookup.country_name AS pop_country_name,
  pop_country_lookup.country_code AS pop_country_code,
  COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) AS pop_state_code,
  LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 3, '0')
            AS pop_county_code,
  COALESCE(pop_county_lookup.county_name, COALESCE(transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na)) AS pop_county_name,
  transaction_fabs.place_of_performance_code AS pop_city_code,
  COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5) AS pop_zip5,
  LPAD(CAST(CAST(REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$', 1) AS SHORT) AS STRING), 2, '0')
            AS pop_congressional_code,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city)) AS pop_city_name,
  POP_STATE_LOOKUP.name AS pop_state_name,
  POP_STATE_LOOKUP.fips AS pop_state_fips,
  POP_STATE_POPULATION.latest_population AS pop_state_population,
  POP_COUNTY_POPULATION.latest_population AS pop_county_population,
  POP_DISTRICT_POPULATION.latest_population AS pop_congressional_population,

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
  TREASURY_ACCT.disaster_emergency_fund_codes,
  DEFC.covid_spending_by_defc,
  DEFC.total_covid_outlay,
  DEFC.total_covid_obligation
FROM
  raw.awards
INNER JOIN
  raw.transaction_normalized AS latest_transaction
    ON (awards.latest_transaction_id = latest_transaction.id)
LEFT OUTER JOIN
  raw.transaction_fpds
    ON (awards.latest_transaction_id = transaction_fpds.transaction_id AND latest_transaction.is_fpds = true)
LEFT OUTER JOIN
  raw.transaction_fabs
    ON (awards.latest_transaction_id = transaction_fabs.transaction_id AND latest_transaction.is_fpds = false)
LEFT OUTER JOIN
  raw.recipient_lookup ON recipient_lookup.recipient_hash = REGEXP_REPLACE(MD5(UPPER(
     CASE
       WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
       WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
       ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal, ''))
    END)), '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$', '\$1-\$2-\$3-\$4-\$5')
LEFT OUTER JOIN
  global_temp.psc ON (transaction_fpds.product_or_service_code = psc.code)
  LEFT OUTER JOIN
    (SELECT
      award_id, COLLECT_SET(DISTINCT TO_JSON(NAMED_STRUCT('cfda_number', cfda_number, 'cfda_program_title', cfda_title))) as cfdas
      FROM
         raw.transaction_fabs tf
       INNER JOIN raw.transaction_normalized tn ON
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
  (SELECT a1.id, a1.toptier_agency_id FROM global_temp.agency a1 ORDER BY a1.toptier_flag DESC, a1.id LIMIT 1) AS FA_ID
    ON (FA_ID.toptier_agency_id = TFA.toptier_agency_id)
LEFT OUTER JOIN
    global_temp.ref_country_code AS pop_country_lookup ON (
        pop_country_lookup.country_code = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA')
        OR pop_country_lookup.country_name = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c)
)
LEFT OUTER JOIN
   global_temp.ref_country_code AS rl_country_lookup on (
      rl_country_lookup.country_code = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code, 'USA')
      OR rl_country_lookup.country_name = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code))
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
LEFT OUTER JOIN (
        SELECT recipient_hash, uei, SORT_ARRAY(COLLECT_SET(recipient_level)) AS recipient_levels
        FROM raw.recipient_profile
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
        COLLECT_SET(
            TO_JSON(NAMED_STRUCT('defc', GROUPED_BY_DEFC.def_code, 'outlay', GROUPED_BY_DEFC.outlay, 'obligation', GROUPED_BY_DEFC.obligation))
        ) AS covid_spending_by_defc,
        sum(GROUPED_BY_DEFC.outlay) AS total_covid_outlay,
        sum(GROUPED_BY_DEFC.obligation) AS total_covid_obligation
    FROM (
        SELECT
            faba.award_id,
            faba.disaster_emergency_fund_code AS def_code,
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
            raw.financial_accounts_by_awards AS faba
        INNER JOIN
            global_temp.disaster_emergency_fund_code AS defc ON (faba.disaster_emergency_fund_code = defc.code AND defc.group_name = 'covid_19')
        INNER JOIN
            global_temp.submission_attributes AS sa ON (faba.submission_id = sa.submission_id AND sa.reporting_period_start >= '2020-04-01')
        INNER JOIN
            global_temp.dabs_submission_window_schedule AS dsws ON (sa.submission_window_id = dsws.id AND dsws.submission_reveal_date <= now())
        GROUP BY
            faba.award_id, faba.disaster_emergency_fund_code
    ) AS GROUPED_BY_DEFC
    WHERE
        GROUPED_BY_DEFC.award_id IS NOT NULL
    GROUP BY
        GROUPED_BY_DEFC.award_id
) DEFC on DEFC.award_id = awards.id
LEFT OUTER JOIN (
  SELECT
    faba.award_id,
    COLLECT_SET(
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
    ) AS tas_paths,
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
    ) AS tas_components,
    -- "CASE" put in place so that Spark value matches Postgres; can most likely be refactored out in the future
    CASE
        WHEN SIZE(COLLECT_SET(faba.disaster_emergency_fund_code)) > 0
            THEN SORT_ARRAY(COLLECT_SET(faba.disaster_emergency_fund_code))
        ELSE NULL
    END AS disaster_emergency_fund_codes,
    COLLECT_SET(taa.treasury_account_identifier) AS treasury_account_identifiers
  FROM
    global_temp.treasury_appropriation_account taa
  INNER JOIN raw.financial_accounts_by_awards faba ON (taa.treasury_account_identifier = faba.treasury_account_id)
  INNER JOIN global_temp.federal_account fa ON (taa.federal_account_id = fa.id)
  INNER JOIN global_temp.toptier_agency agency ON (fa.parent_toptier_agency_id = agency.toptier_agency_id)
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
) TREASURY_ACCT ON (TREASURY_ACCT.award_id = awards.id)
WHERE
    -- Make sure that the data matches the different Award Type matviews' current state
    (
        latest_transaction.action_date >= '2007-10-01'
        AND (
            awards.type IN ({str(list(all_awards_types_to_category)).replace("[", "").replace("]", "")})
            OR awards.type LIKE 'IDV%'
        )
    )
    -- Make sure that we also pick up the current state of Pre2008 matview
    OR (
        latest_transaction.action_date >= '2000-10-01'
        AND latest_transaction.action_date < '2007-10-01'
    )
"""
