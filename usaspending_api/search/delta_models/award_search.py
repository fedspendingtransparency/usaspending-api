award_search_sql_string = """
SELECT
  tas.treasury_account_identifiers,
  awards.id AS award_id,
  awards.category,
  awards.type,
  awards.type_description,
  awards.generated_unique_award_id,
  awards.piid AS display_award_id,
  awards.update_date,
  awards.piid,
  awards.fain AS fain,
  awards.uri AS uri,
  COALESCE(awards.total_obligation, 0) AS award_amount,
  COALESCE(awards.total_obligation, 0) AS total_obligation,
  awards.description,
  COALESCE(awards.total_obligation, 0) AS total_obl_bin,
  COALESCE(awards.total_subsidy_cost, 0) AS total_subsidy_cost,
  COALESCE(awards.total_loan_value, 0) AS total_loan_value,

  recipient_profile.recipient_hash,
  recipient_profile.recipient_levels,
  UPPER(COALESCE(recipient_lookup.recipient_name, transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) AS recipient_name,
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
  (SELECT a1.id FROM agency a1 WHERE a1.toptier_agency_id = (SELECT a2.toptier_agency_id FROM agency a2 WHERE a2.id = latest_transaction.funding_agency_id) ORDER BY a1.toptier_flag DESC, a1.id LIMIT 1) AS funding_toptier_agency_id,
  latest_transaction.funding_agency_id AS funding_subtier_agency_id,

  rl_country_lookup.country_code AS recipient_location_country_code,
  rl_country_lookup.country_name AS recipient_location_country_name,
  COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) AS recipient_location_state_code,
  LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 3, '0') AS recipient_location_county_code,
  COALESCE(rl_county_lookup.county_name, transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name) AS recipient_location_county_name,
  LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 2, '0') AS recipient_location_congressional_code,
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
  LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 3, '0') AS pop_county_code,
  COALESCE(pop_county_lookup.county_name, COALESCE(transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na)) AS pop_county_name,
  transaction_fabs.place_of_performance_code AS pop_city_code,
  COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5) AS pop_zip5,
  LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 2, '0') AS pop_congressional_code,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city)) AS pop_city_name,
  POP_STATE_LOOKUP.name AS pop_state_name,
  POP_STATE_LOOKUP.fips AS pop_state_fips,
  POP_STATE_POPULATION.latest_population AS pop_state_population,
  POP_COUNTY_POPULATION.latest_population AS pop_county_population,
  POP_DISTRICT_POPULATION.latest_population AS pop_congressional_population,

  transaction_fabs.cfda_title AS cfda_program_title,
  transaction_fabs.cfda_number AS cfda_number,
  transaction_cfdas.cfdas AS cfdas,


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
  awards
INNER JOIN
  transaction_normalized AS latest_transaction
    ON (awards.latest_transaction_id = latest_transaction.id)
LEFT OUTER JOIN
  transaction_fpds
    ON (awards.latest_transaction_id = transaction_fpds.transaction_id AND latest_transaction.is_fpds = true)
LEFT OUTER JOIN
  transaction_fabs
    ON (awards.latest_transaction_id = transaction_fabs.transaction_id AND latest_transaction.is_fpds = false)
LEFT OUTER JOIN
  (SELECT
    legal_business_name AS recipient_name,
    duns,
    recipient_hash
  FROM recipient_lookup AS rlv
  ) recipient_lookup ON recipient_lookup.recipient_hash = MD5(UPPER(
     CASE
       WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei))
       WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
       ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) END))
LEFT OUTER JOIN
  psc ON (transaction_fpds.product_or_service_code = psc.code)
  LEFT OUTER JOIN
    (SELECT
      award_id, COLLECT_SET(DISTINCT TO_JSON('cfda_number', cfda_number, 'cfda_program_title', cfda_title)) as cfdas
      FROM
         transaction_fabs tf
       INNER JOIN transaction_normalized tn ON
         tf.transaction_id = tn.id
       GROUP BY
         award_id
    ) AS transaction_cfdas ON awards.id = transaction_cfdas.award_id
LEFT OUTER JOIN
  agency AS AA
    ON (awards.awarding_agency_id = AA.id)
LEFT OUTER JOIN
  toptier_agency AS TAA
    ON (AA.toptier_agency_id = TAA.toptier_agency_id)
LEFT OUTER JOIN
  subtier_agency AS SAA
    ON (AA.subtier_agency_id = SAA.subtier_agency_id)
LEFT OUTER JOIN
  agency AS FA ON (awards.funding_agency_id = FA.id)
LEFT OUTER JOIN
  toptier_agency AS TFA
    ON (FA.toptier_agency_id = TFA.toptier_agency_id)
LEFT OUTER JOIN
  subtier_agency AS SFA
    ON (FA.subtier_agency_id = SFA.subtier_agency_id)
LEFT OUTER JOIN (
  SELECT
    faba.award_id,
    COLLECT_SET(DISTINCT taa.treasury_account_identifier) treasury_account_identifiers
  FROM
    treasury_appropriation_account taa
    INNER JOIN financial_accounts_by_awards faba ON taa.treasury_account_identifier = faba.treasury_account_id
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
) tas ON (tas.award_id = awards.id)
LEFT OUTER JOIN
   ref_country_code AS pop_country_lookup on (
      pop_country_lookup.country_code = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA')
      OR pop_country_lookup.country_name = COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c))
LEFT OUTER JOIN
   ref_country_code AS rl_country_lookup on (
      rl_country_lookup.country_code = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code, 'USA')
      OR rl_country_lookup.country_name = COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code))
LEFT OUTER JOIN
 (SELECT DISTINCT (state_alpha, county_numeric) state_alpha, county_numeric, UPPER(county_name) AS county_name FROM ref_city_county_state_code) AS rl_county_lookup ON 
   rl_county_lookup.state_alpha = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) and
   rl_county_lookup.county_numeric = LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 3, '0')
LEFT OUTER JOIN
 (SELECT DISTINCT (state_alpha, county_numeric) state_alpha, county_numeric, UPPER(county_name) AS county_name FROM ref_city_county_state_code) AS pop_county_lookup on
   pop_county_lookup.state_alpha = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) and
   pop_county_lookup.county_numeric = LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 3, '0')
LEFT JOIN
 (SELECT code, name, fips, MAX(id) FROM state_data GROUP BY code, name, fips) AS POP_STATE_LOOKUP
 ON (POP_STATE_LOOKUP.code = COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.legal_entity_state_code))
LEFT JOIN
 (SELECT code, name, fips, MAX(id) FROM state_data GROUP BY code, name, fips) AS RL_STATE_LOOKUP
 ON (RL_STATE_LOOKUP.code = COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code))
LEFT JOIN ref_population_county AS POP_STATE_POPULATION 
  ON (POP_STATE_POPULATION.state_code = POP_STATE_LOOKUP.fips AND POP_STATE_POPULATION.county_number = '000')
LEFT JOIN ref_population_county AS POP_COUNTY_POPULATION
  ON (POP_COUNTY_POPULATION.state_code = POP_STATE_LOOKUP.fips AND POP_COUNTY_POPULATION.county_number = LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 3, '0'))
LEFT JOIN ref_population_county AS RL_STATE_POPULATION
  ON (RL_STATE_POPULATION.state_code = RL_STATE_LOOKUP.fips AND RL_STATE_POPULATION.county_number = '000')
LEFT JOIN ref_population_county AS RL_COUNTY_POPULATION
  ON (RL_COUNTY_POPULATION.state_code = RL_STATE_LOOKUP.fips AND RL_COUNTY_POPULATION.county_number = LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 3, '0'))
LEFT JOIN ref_population_cong_district AS POP_DISTRICT_POPULATION
  ON (POP_DISTRICT_POPULATION.state_code = POP_STATE_LOOKUP.fips AND POP_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 2, '0'))
LEFT JOIN ref_population_cong_district AS RL_DISTRICT_POPULATION
  ON (RL_DISTRICT_POPULATION.state_code = RL_STATE_LOOKUP.fips AND RL_DISTRICT_POPULATION.congressional_district = LPAD(CAST(CAST((REGEXP_EXTRACT(COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional), '^[A-Z]*(\\d+)(?:\\.\\d+)?$'))[1] AS short) AS STRING), 2, '0'))
LEFT JOIN LATERAL (
  SELECT recipient_hash, recipient_unique_id, COLLECT_SET(recipient_level) AS recipient_levels
  FROM recipient_profile
  WHERE (recipient_hash = COALESCE(recipient_lookup.recipient_hash, MD5(UPPER(CASE WHEN COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(transaction_fpds.awardee_or_recipient_uei, transaction_fabs.uei)) WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu)) ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) END)))
   ) AND
      recipient_name NOT IN (
      'MULTIPLE RECIPIENTS',
      'REDACTED DUE TO PII',
      'MULTIPLE FOREIGN RECIPIENTS',
      'PRIVATE INDIVIDUAL',
      'INDIVIDUAL RECIPIENT',
      'MISCELLANEOUS FOREIGN AWARDEES'
    ) AND recipient_name IS NOT NULL
    AND recipient_level != 'P'
  GROUP BY recipient_hash, recipient_unique_id
) recipient_profile ON TRUE
LEFT JOIN (
  SELECT
        GROUPED_BY_DEFC.award_id,
        COLLECT_SET(
            TO_JSON('defc', GROUPED_BY_DEFC.def_code, 'outlay', GROUPED_BY_DEFC.outlay, 'obligation', GROUPED_BY_DEFC.obligation)
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
            financial_accounts_by_awards AS faba
        INNER JOIN
            disaster_emergency_fund_code AS defc ON (faba.disaster_emergency_fund_code = defc.code AND defc.group_name = 'covid_19')
        INNER JOIN
            submission_attributes AS sa ON (faba.submission_id = sa.submission_id AND sa.reporting_period_start >= '2020-04-01')
        INNER JOIN
            dabs_submission_window_schedule AS dsws ON (sa.submission_window_id = dsws.id AND dsws.submission_reveal_date <= now())
        GROUP BY
            faba.award_id, faba.disaster_emergency_fund_code
    ) AS GROUPED_BY_DEFC
    WHERE
        GROUPED_BY_DEFC.award_id IS NOT NULL
    GROUP BY
        GROUPED_BY_DEFC.award_id
) DEFC on DEFC.award_id = awards.id
LEFT JOIN (
  SELECT
    faba.award_id,
    COLLECT_SET(
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
    COLLECT_SET(
      DISTINCT CONCAT(
        'aid=', taa.agency_id,
        'main=', taa.main_account_code,
        'ata=', taa.allocation_transfer_agency_id,
        'sub=', taa.sub_account_code,
        'bpoa=', taa.beginning_period_of_availability,
        'epoa=', taa.ending_period_of_availability,
        'a=', taa.availability_type_code
      )
    ) tas_components,
    COLLECT_SET(DISTINCT faba.disaster_emergency_fund_code) FILTER (WHERE faba.disaster_emergency_fund_code IS NOT NULL) disaster_emergency_fund_codes
  FROM
    treasury_appropriation_account taa
  INNER JOIN financial_accounts_by_awards faba ON (taa.treasury_account_identifier = faba.treasury_account_id)
  INNER JOIN federal_account fa ON (taa.federal_account_id = fa.id)
  INNER JOIN toptier_agency agency ON (fa.parent_toptier_agency_id = agency.toptier_agency_id)
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
) TREASURY_ACCT ON (TREASURY_ACCT.award_id = awards.id)
WHERE
  latest_transaction.action_date >= '2007-10-01'
"""
