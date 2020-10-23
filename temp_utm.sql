DROP MATERIALIZED VIEW IF EXISTS universal_transaction_matview_proto;
CREATE MATERIALIZED VIEW universal_transaction_matview_proto AS (
SELECT
  tas.treasury_account_identifiers,

  transaction_normalized.id AS transaction_id,
  transaction_normalized.action_date::date,
  DATE(transaction_normalized.action_date::date + interval '3 months') AS fiscal_action_date,
  transaction_normalized.last_modified_date::date,
  transaction_normalized.fiscal_year,
  awards.certified_date AS award_certified_date,
  FY(awards.certified_date) AS award_fiscal_year,
  transaction_normalized.type,
  transaction_normalized.award_id,
  transaction_normalized.update_date,
  awards.category AS award_category,

  COALESCE(CASE
    WHEN transaction_normalized.type IN('07','08') THEN awards.total_subsidy_cost
    ELSE awards.total_obligation
  END, 0)::NUMERIC(23, 2) AS award_amount,
  COALESCE(CASE
    WHEN transaction_normalized.type IN('07','08') THEN transaction_normalized.original_loan_subsidy_cost
    ELSE transaction_normalized.federal_action_obligation
  END, 0)::NUMERIC(23, 2) AS generated_pragmatic_obligation,
  awards.fain,
  awards.uri,
  awards.piid,
  awards.update_date AS award_update_date,
  awards.generated_unique_award_id,
  awards.type_description,
  awards.period_of_performance_start_date,
  awards.period_of_performance_current_end_date,
  COALESCE(transaction_normalized.federal_action_obligation, 0)::NUMERIC(23, 2) AS federal_action_obligation,
  COALESCE(transaction_normalized.original_loan_subsidy_cost, 0)::NUMERIC(23, 2) AS original_loan_subsidy_cost,
  COALESCE(transaction_normalized.face_value_loan_guarantee, 0)::NUMERIC(23, 2) AS face_value_loan_guarantee,
  transaction_normalized.description AS transaction_description,
  transaction_normalized.modification_number,

  pop_country_lookup.country_name AS pop_country_name,
  pop_country_lookup.country_code AS pop_country_code,
  COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) AS pop_state_code,
  LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0') AS pop_county_code,
  COALESCE(pop_county_lookup.county_name, transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na) AS pop_county_name,
  COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5) AS pop_zip5,
  LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0') AS pop_congressional_code,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city)) AS pop_city_name,

  rl_country_lookup.country_code AS recipient_location_country_code,
  rl_country_lookup.country_name AS recipient_location_country_name,
  COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) AS recipient_location_state_code,
  LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0') AS recipient_location_county_code,
  COALESCE(rl_county_lookup.county_name, transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name) AS recipient_location_county_name,
  LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0') AS recipient_location_congressional_code,
  COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5) AS recipient_location_zip5,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.legal_entity_city_name, transaction_fabs.legal_entity_city_name)) AS recipient_location_city_name,

  transaction_fpds.naics AS naics_code,
  naics.description AS naics_description,
  transaction_fpds.product_or_service_code,
  psc.description AS product_or_service_description,
  transaction_fpds.type_of_contract_pricing,
  transaction_fpds.type_set_aside,
  transaction_fpds.extent_competed,
  transaction_fpds.detached_award_proc_unique,
  transaction_fpds.ordering_period_end_date,
  transaction_fabs.cfda_number,
  transaction_fabs.afa_generated_unique,
  transaction_fabs.cfda_title AS cfda_title,
  references_cfda.id AS cfda_id,

  COALESCE(recipient_lookup.recipient_hash, MD5(UPPER(
    CASE
      WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
      ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) END
  ))::uuid) AS recipient_hash,
  UPPER(COALESCE(recipient_lookup.recipient_name, transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) AS recipient_name,
  COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AS recipient_unique_id,
  COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id,
  transaction_normalized.business_categories,

  transaction_normalized.awarding_agency_id,
  transaction_normalized.funding_agency_id,
  AA.toptier_agency_id AS awarding_toptier_agency_id,
  FA.toptier_agency_id AS funding_toptier_agency_id,
  TAA.name AS awarding_toptier_agency_name,
  TFA.name AS funding_toptier_agency_name,
  SAA.name AS awarding_subtier_agency_name,
  SFA.name AS funding_subtier_agency_name,
  TAA.abbreviation AS awarding_toptier_agency_abbreviation,
  TFA.abbreviation AS funding_toptier_agency_abbreviation,
  SAA.abbreviation AS awarding_subtier_agency_abbreviation,
  SFA.abbreviation AS funding_subtier_agency_abbreviation,

  TREASURY_ACCT.tas_paths,
  TREASURY_ACCT.tas_components,
  FEDERAL_ACCT.federal_accounts,
  FEDERAL_ACCT.defc AS disaster_emergency_fund_codes,
  UPPER(PRL.legal_business_name) AS parent_recipient_name,
  PRL.recipient_hash AS parent_recipient_hash,

    CASE
    WHEN COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) IS NOT NULL AND LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0') IS NOT NULL
      THEN CONCAT('{"country_code":"', rl_country_lookup.country_code,
        '","state_code":"', COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code),
        '","state_fips":"', RL_STATE_LOOKUP.fips,
        '","county_code":"', LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0'),
        '","county_name":"', COALESCE(rl_county_lookup.county_name, transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name),
        '","population":"', RL_COUNTY_POPULATION.latest_population, '"}'
      )
    ELSE NULL
  END AS recipient_location_county_agg_key,
  CASE
    WHEN COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) IS NOT NULL AND LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0') IS NOT NULL
      THEN CONCAT('{"country_code":"', rl_country_lookup.country_code,
        '","state_code":"', COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code),
        '","state_fips":"', RL_STATE_LOOKUP.fips,
        '","congressional_code":"', LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0'),
        '","population":"', RL_DISTRICT_POPULATION.latest_population, '"}'
      )
    ELSE NULL
  END AS recipient_location_congressional_agg_key,
  CASE
    WHEN COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) IS NOT NULL
      THEN CONCAT('{"country_code":"', rl_country_lookup.country_code,
        '","state_code":"', COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code),
        '","state_name":"', RL_STATE_LOOKUP.name,
        '","population":"', RL_STATE_POPULATION.latest_population, '"}'
      )
    ELSE NULL
  END AS recipient_location_state_agg_key,

    CASE
    WHEN COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) IS NOT NULL AND COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co) IS NOT NULL
      THEN CONCAT('{"country_code":"', pop_country_lookup.country_code,
        '","state_code":"', COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code),
        '","state_fips":"', POP_STATE_LOOKUP.fips,
        '","county_code":"', LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0'),
        '","county_name":"', COALESCE(pop_county_lookup.county_name, transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na),
        '","population":"', POP_COUNTY_POPULATION.latest_population, '"}'
      )
    ELSE NULL
  END AS pop_county_agg_key,
  CASE
    WHEN COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) IS NOT NULL AND LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0') IS NOT NULL
      THEN CONCAT('{"country_code":"', pop_country_lookup.country_code,
        '","state_code":"', COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code),
        '","state_fips":"', POP_STATE_LOOKUP.fips,
        '","congressional_code":"', LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0'),
        '","population":"', POP_DISTRICT_POPULATION.latest_population, '"}'
      )
    ELSE NULL
  END AS pop_congressional_agg_key,
  CASE
    WHEN COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) IS NOT NULL
      THEN CONCAT('{"country_code":"', pop_country_lookup.country_code,
        '","state_code":"', COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code),
        '","state_name":"', POP_STATE_LOOKUP.name,
        '","population":"', POP_STATE_POPULATION.latest_population, '"}'
      )
    ELSE NULL
  END AS pop_state_agg_key,
  CASE
    WHEN pop_country_lookup.country_code IS NOT NULL
      THEN CONCAT('{"country_code":"', pop_country_lookup.country_code,
        '","country_name":"', pop_country_lookup.country_name, '"}'
      )
    ELSE NULL
  END AS pop_country_agg_key,

    CASE
    WHEN TAA.name IS NOT NULL
      THEN CONCAT('{"name":"', TAA.name,
        '","abbreviation":"', TAA.abbreviation,
        '","id":"', (SELECT a.id FROM agency a WHERE a.toptier_agency_id = TAA.toptier_agency_id AND a.toptier_flag = TRUE), '"}'
      )
    ELSE NULL
  END AS awarding_toptier_agency_agg_key,
  CASE
    WHEN TFA.name IS NOT NULL
      THEN CONCAT('{"name":"', TFA.name,
        '","abbreviation":"', TFA.abbreviation,
        '","id":"', (SELECT a.id FROM agency a WHERE a.toptier_agency_id = TFA.toptier_agency_id AND a.toptier_flag = TRUE), '"}'
      )
    ELSE NULL
  END AS funding_toptier_agency_agg_key,
  CASE
    WHEN SAA.name IS NOT NULL
      THEN CONCAT('{"name":"', SAA.name,
        '","abbreviation":"', SAA.abbreviation,
        '","id":"', transaction_normalized.awarding_agency_id, '"}'
      )
    ELSE NULL
  END AS awarding_subtier_agency_agg_key,
  CASE
    WHEN SFA.name IS NOT NULL
      THEN CONCAT('{"name":"', SFA.name,
        '","abbreviation":"', SFA.abbreviation,
        '","id":"', transaction_normalized.funding_agency_id, '"}'
      )
    ELSE NULL
  END AS funding_subtier_agency_agg_key,
    CASE
    WHEN transaction_fpds.product_or_service_code IS NOT NULL
      THEN CONCAT(
        '{"code":"', transaction_fpds.product_or_service_code,
        '","description":"', psc.description, '"}'
      )
    ELSE NULL
  END AS psc_agg_key,
  CASE
    WHEN transaction_fpds.naics IS NOT NULL
      THEN CONCAT('{"code":"', transaction_fpds.naics, '","description":"', naics.description, '"}')
    ELSE NULL
  END AS naics_agg_key,
  CASE
    WHEN RECIPIENT_HASH_AND_LEVEL.recipient_hash IS NULL or RECIPIENT_HASH_AND_LEVEL.recipient_level IS NULL
      THEN CONCAT('{"hash_with_level": "","name":"', UPPER(COALESCE(recipient_lookup.recipient_name, transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)), '","unique_id":"', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu), '"}')
    ELSE
      CONCAT(
        '{"hash_with_level":"', CONCAT(RECIPIENT_HASH_AND_LEVEL.recipient_hash, '-', RECIPIENT_HASH_AND_LEVEL.recipient_level),
        '","name":"', UPPER(COALESCE(recipient_lookup.recipient_name, transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)),
        '","unique_id":"', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu), '"}'
      )
  END AS recipient_agg_key



FROM
  transaction_normalized
LEFT OUTER JOIN
  transaction_fabs ON (transaction_normalized.id = transaction_fabs.transaction_id AND transaction_normalized.is_fpds = false)
LEFT OUTER JOIN
  transaction_fpds ON (transaction_normalized.id = transaction_fpds.transaction_id AND transaction_normalized.is_fpds = true)
LEFT OUTER JOIN
  references_cfda ON (transaction_fabs.cfda_number = references_cfda.program_number)
LEFT OUTER JOIN
  (SELECT
    recipient_hash,
    legal_business_name AS recipient_name,
    duns
  FROM recipient_lookup AS rlv
  ) recipient_lookup ON recipient_lookup.duns = COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AND COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
LEFT OUTER JOIN
  awards ON (transaction_normalized.award_id = awards.id)
LEFT OUTER JOIN
  agency AS AA ON (transaction_normalized.awarding_agency_id = AA.id)
LEFT OUTER JOIN
  toptier_agency AS TAA ON (AA.toptier_agency_id = TAA.toptier_agency_id)
LEFT OUTER JOIN
  subtier_agency AS SAA ON (AA.subtier_agency_id = SAA.subtier_agency_id)
LEFT OUTER JOIN
  agency AS FA ON (transaction_normalized.funding_agency_id = FA.id)
LEFT OUTER JOIN
  toptier_agency AS TFA ON (FA.toptier_agency_id = TFA.toptier_agency_id)
LEFT OUTER JOIN
  subtier_agency AS SFA ON (FA.subtier_agency_id = SFA.subtier_agency_id)
LEFT OUTER JOIN
  naics ON (transaction_fpds.naics = naics.code)
LEFT OUTER JOIN
  psc ON (transaction_fpds.product_or_service_code = psc.code)
LEFT OUTER JOIN (
  SELECT
    faba.award_id,
    ARRAY_AGG(DISTINCT taa.treasury_account_identifier) treasury_account_identifiers
  FROM
    treasury_appropriation_account taa
    INNER JOIN financial_accounts_by_awards faba ON taa.treasury_account_identifier = faba.treasury_account_id
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
) tas ON (tas.award_id = transaction_normalized.award_id)
LEFT OUTER JOIN
 (SELECT DISTINCT ON (state_alpha, county_numeric) state_alpha, county_numeric, UPPER(county_name) AS county_name FROM ref_city_county_state_code) AS rl_county_lookup on
   rl_county_lookup.state_alpha = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) and
   rl_county_lookup.county_numeric = LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0')
LEFT OUTER JOIN
 (SELECT DISTINCT ON (state_alpha, county_numeric) state_alpha, county_numeric, UPPER(county_name) AS county_name FROM ref_city_county_state_code) AS pop_county_lookup on
   pop_county_lookup.state_alpha = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) and
   pop_county_lookup.county_numeric = LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0')
LEFT OUTER JOIN
   ref_country_code AS pop_country_lookup on (
      pop_country_lookup.country_code = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA')
      OR pop_country_lookup.country_name = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c))
LEFT OUTER JOIN
   ref_country_code AS rl_country_lookup on (
      rl_country_lookup.country_code = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code, 'USA')
      OR rl_country_lookup.country_name = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code))
LEFT JOIN recipient_lookup PRL ON (PRL.duns = COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide))
LEFT JOIN LATERAL (
  SELECT recipient_hash, recipient_level, recipient_unique_id
  FROM recipient_profile
  WHERE (
    recipient_hash = COALESCE(recipient_lookup.recipient_hash, MD5(UPPER(CASE WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu)) ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) END))::uuid)
    or recipient_unique_id = recipient_lookup.duns) AND
           recipient_name NOT IN (
             'MULTIPLE RECIPIENTS',
             'REDACTED DUE TO PII',
             'MULTIPLE FOREIGN RECIPIENTS',
             'PRIVATE INDIVIDUAL',
             'INDIVIDUAL RECIPIENT',
             'MISCELLANEOUS FOREIGN AWARDEES'
           ) AND recipient_name IS NOT NULL
  ORDER BY CASE
             WHEN recipient_level = 'C' then 0
             WHEN recipient_level = 'R' then 1
             ELSE 2
           END ASC
  LIMIT 1
) RECIPIENT_HASH_AND_LEVEL ON TRUE
LEFT JOIN (
  SELECT   code, name, fips, MAX(id)
  FROM     state_data
  GROUP BY code, name, fips
) POP_STATE_LOOKUP ON (POP_STATE_LOOKUP.code = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code))
LEFT JOIN ref_population_county POP_STATE_POPULATION ON (POP_STATE_POPULATION.state_code = POP_STATE_LOOKUP.fips AND POP_STATE_POPULATION.county_number = '000')
LEFT JOIN ref_population_county POP_COUNTY_POPULATION ON (POP_COUNTY_POPULATION.state_code = POP_STATE_LOOKUP.fips AND POP_COUNTY_POPULATION.county_number = LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0'))
LEFT JOIN ref_population_cong_district POP_DISTRICT_POPULATION ON (POP_DISTRICT_POPULATION.state_code = POP_STATE_LOOKUP.fips AND POP_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0'))
LEFT JOIN (
  SELECT   code, name, fips, MAX(id)
  FROM     state_data
  GROUP BY code, name, fips
) RL_STATE_LOOKUP ON (RL_STATE_LOOKUP.code = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code))
LEFT JOIN ref_population_county RL_STATE_POPULATION ON (RL_STATE_POPULATION.state_code = RL_STATE_LOOKUP.fips AND RL_STATE_POPULATION.county_number = '000')
LEFT JOIN ref_population_county RL_COUNTY_POPULATION ON (RL_COUNTY_POPULATION.state_code = RL_STATE_LOOKUP.fips AND RL_COUNTY_POPULATION.county_number = LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0'))
LEFT JOIN ref_population_cong_district RL_DISTRICT_POPULATION ON (RL_DISTRICT_POPULATION.state_code = RL_STATE_LOOKUP.fips AND RL_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST((REGEXP_MATCH(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0'))
LEFT JOIN (
  SELECT
    faba.award_id,
    ARRAY_AGG(
      DISTINCT CONCAT(
        'agency=', agency.toptier_code,
        'faaid=', fa.agency_identifier,
        'famain=', fa.main_account_code,
        'aid=', taa.agency_id,
        'main=', taa.main_account_code,
        'ata=', taa.allocation_transfer_agency_id,
        'sub=', taa.sub_account_code,
        'bpoa=', taa.beginning_period_of_availability,
        'epoa=', taa.ending_period_of_availability,
        'a=', taa.availability_type_code
       )
     ) tas_paths,
     ARRAY_AGG(
      DISTINCT CONCAT(
        'aid=', taa.agency_id,
        'main=', taa.main_account_code,
        'ata=', taa.allocation_transfer_agency_id,
        'sub=', taa.sub_account_code,
        'bpoa=', taa.beginning_period_of_availability,
        'epoa=', taa.ending_period_of_availability,
        'a=', taa.availability_type_code
       )
     ) tas_components
 FROM
   treasury_appropriation_account taa
   INNER JOIN financial_accounts_by_awards faba ON (taa.treasury_account_identifier = faba.treasury_account_id)
   INNER JOIN federal_account fa ON (taa.federal_account_id = fa.id)
   INNER JOIN toptier_agency agency ON (fa.parent_toptier_agency_id = agency.toptier_agency_id)
 WHERE
   faba.award_id IS NOT NULL
 GROUP BY
   faba.award_id
) TREASURY_ACCT ON (TREASURY_ACCT.award_id = transaction_normalized.award_id)
LEFT JOIN (
  SELECT
    faba.award_id,
    JSONB_AGG(
      DISTINCT JSONB_BUILD_OBJECT(
        'id', fa.id,
        'account_title', fa.account_title,
        'federal_account_code', fa.federal_account_code
      )
    ) federal_accounts,
    ARRAY_AGG(DISTINCT disaster_emergency_fund_code) FILTER (WHERE disaster_emergency_fund_code IS NOT NULL) defc
  FROM
    federal_account fa
    INNER JOIN treasury_appropriation_account taa ON fa.id = taa.federal_account_id
    INNER JOIN financial_accounts_by_awards faba ON taa.treasury_account_identifier = faba.treasury_account_id
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
) FEDERAL_ACCT ON (FEDERAL_ACCT.award_id = transaction_normalized.award_id)
WHERE
  transaction_normalized.action_date >= '2000-10-01');

CREATE UNIQUE INDEX temp_idx_dev_6052_matview_proto_id ON universal_transaction_matview_proto(transaction_id);