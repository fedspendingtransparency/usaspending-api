DROP VIEW IF EXISTS award_delta_view;

CREATE VIEW award_delta_view AS
SELECT
  awards.id AS award_id,
  awards.generated_unique_award_id,
    CASE
    WHEN awards.type IN ('02', '03', '04', '05', '06', '10', '07', '08', '09', '11') AND awards.fain IS NOT NULL THEN awards.fain
    WHEN awards.piid IS NOT NULL THEN awards.piid  -- contracts. Did it this way to easily handle IDV contracts
    ELSE awards.uri
  END AS display_award_id,

  awards.category,
  awards.type,
  awards.type_description,
  awards.piid,
  awards.fain,
  awards.uri,
  awards.total_obligation,
  awards.description,
  obligation_to_enum(awards.total_obligation) AS total_obl_bin,
  awards.total_subsidy_cost,
  awards.total_loan_value,

  awards.recipient_id,
  UPPER(COALESCE(recipient_lookup.recipient_name, transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) AS recipient_name,
  COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AS recipient_unique_id,
  COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id,
  legal_entity.business_categories,

  latest_transaction.action_date,
  latest_transaction.fiscal_year,
  latest_transaction.last_modified_date,
  awards.period_of_performance_start_date,
  awards.period_of_performance_current_end_date,
  awards.date_signed,
  transaction_fpds.ordering_period_end_date,

  transaction_fabs.original_loan_subsidy_cost,
  transaction_fabs.face_value_loan_guarantee,

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

  CASE WHEN COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code) = 'UNITED STATES' THEN 'USA' ELSE COALESCE(transaction_fpds.legal_entity_country_code, transaction_fabs.legal_entity_country_code, 'USA') END AS recipient_location_country_code,
  COALESCE(transaction_fpds.legal_entity_country_name, transaction_fabs.legal_entity_country_name) AS recipient_location_country_name,
  COALESCE(transaction_fpds.legal_entity_state_code, transaction_fabs.legal_entity_state_code) AS recipient_location_state_code,
  COALESCE(transaction_fpds.legal_entity_county_code, transaction_fabs.legal_entity_county_code) AS recipient_location_county_code,
  COALESCE(transaction_fpds.legal_entity_county_name, transaction_fabs.legal_entity_county_name) AS recipient_location_county_name,
  COALESCE(transaction_fpds.legal_entity_congressional, transaction_fabs.legal_entity_congressional) AS recipient_location_congressional_code,
  COALESCE(transaction_fpds.legal_entity_zip5, transaction_fabs.legal_entity_zip5) AS recipient_location_zip5,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.legal_entity_city_name, transaction_fabs.legal_entity_city_name)) AS recipient_location_city_name,


  CASE WHEN COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c) = 'UNITED STATES' THEN 'USA' ELSE COALESCE(transaction_fpds.place_of_perform_country_c, transaction_fabs.place_of_perform_country_c, 'USA') END AS pop_country_code,
  COALESCE(transaction_fpds.place_of_perform_country_n, transaction_fabs.place_of_perform_country_n) AS pop_country_name,
  COALESCE(transaction_fpds.place_of_performance_state, transaction_fabs.place_of_perfor_state_code) AS pop_state_code,
  COALESCE(transaction_fpds.place_of_perform_county_co, transaction_fabs.place_of_perform_county_co) AS pop_county_code,
  COALESCE(transaction_fpds.place_of_perform_county_na, transaction_fabs.place_of_perform_county_na) AS pop_county_name,
  COALESCE(transaction_fpds.place_of_performance_zip5, transaction_fabs.place_of_performance_zip5) AS pop_zip5,
  COALESCE(transaction_fpds.place_of_performance_congr, transaction_fabs.place_of_performance_congr) AS pop_congressional_code,
  TRIM(TRAILING FROM COALESCE(transaction_fpds.place_of_perform_city_name, transaction_fabs.place_of_performance_city)) AS pop_city_name,

  transaction_fabs.cfda_number,
  transaction_fabs.sai_number,
  transaction_fpds.pulled_from,
  transaction_fpds.type_of_contract_pricing,
  transaction_fpds.extent_competed,
  transaction_fpds.type_set_aside,

  transaction_fpds.product_or_service_code,
  psc.description AS product_or_service_description,
  transaction_fpds.naics AS naics_code,
  transaction_fpds.naics_description,
  REPLACE(REPLACE(tas.treasury_account_identifiers::text, '{', '['), '}', ']') AS treasury_account_identifiers
FROM
  awards
JOIN
  transaction_normalized AS latest_transaction
    ON (awards.latest_transaction_id = latest_transaction.id)
LEFT JOIN
  transaction_fabs
    ON (awards.latest_transaction_id = transaction_fabs.transaction_id AND latest_transaction.is_fpds = false)
LEFT JOIN
  transaction_fpds
    ON (awards.latest_transaction_id = transaction_fpds.transaction_id AND latest_transaction.is_fpds = true)
JOIN
  legal_entity
    ON (awards.recipient_id = legal_entity.legal_entity_id)
LEFT JOIN
  (SELECT
    recipient_hash,
    legal_business_name AS recipient_name,
    duns
  FROM recipient_lookup AS rlv
  ) recipient_lookup ON recipient_lookup.duns = COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AND COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
LEFT JOIN
  references_location AS place_of_performance ON (awards.place_of_performance_id = place_of_performance.location_id)
LEFT JOIN
  psc ON (transaction_fpds.product_or_service_code = psc.code)
LEFT JOIN
  agency AS AA
    ON (awards.awarding_agency_id = AA.id)
LEFT JOIN
  toptier_agency AS TAA
    ON (AA.toptier_agency_id = TAA.toptier_agency_id)
LEFT JOIN
  subtier_agency AS SAA
    ON (AA.subtier_agency_id = SAA.subtier_agency_id)
LEFT JOIN
  agency AS FA ON (awards.funding_agency_id = FA.id)
LEFT JOIN
  toptier_agency AS TFA
    ON (FA.toptier_agency_id = TFA.toptier_agency_id)
LEFT JOIN
  subtier_agency AS SFA
    ON (FA.subtier_agency_id = SFA.subtier_agency_id)
LEFT JOIN( SELECT faba.award_id,
 array_agg(DISTINCT taa_1.treasury_account_identifier) AS treasury_account_identifiers
    FROM treasury_appropriation_account taa_1
        JOIN financial_accounts_by_awards faba ON taa_1.treasury_account_identifier = faba.treasury_account_id
    WHERE faba.award_id IS NOT NULL
    GROUP BY faba.award_id) tas ON tas.award_id = awards.id
WHERE
  latest_transaction.action_date >= '2000-10-01'
ORDER BY awards.total_obligation DESC NULLS LAST;
