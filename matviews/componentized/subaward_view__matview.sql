CREATE MATERIALIZED VIEW subaward_view_temp AS
SELECT
  id AS subaward_id,
  id AS broker_subaward_id,
  to_tsvector(CONCAT_WS(' ',
    COALESCE(recipient_lookup.recipient_name, sub.recipient_name),
    psc.description,
    sub.description
  )) AS keyword_ts_vector,
  to_tsvector(CONCAT_WS(' ', piid, fain, subaward_number)) AS award_ts_vector,
  to_tsvector(COALESCE(recipient_lookup.recipient_name, sub.recipient_name, '')) AS recipient_name_ts_vector,

  tas.treasury_account_identifiers,

  latest_transaction_id,
  last_modified_date,
  subaward_number,
  COALESCE(amount, 0)::NUMERIC(23, 2) AS amount,
  obligation_to_enum(COALESCE(amount, 0)) AS total_obl_bin,
  sub.description,
  fy(action_date) AS fiscal_year,
  action_date,
  award_report_fy_month,
  award_report_fy_year,

  sub.award_id,
  sub.unique_award_key AS generated_unique_award_id,
  awarding_agency_id,
  funding_agency_id,
  awarding_toptier_agency_name,
  awarding_subtier_agency_name,
  funding_toptier_agency_name,
  funding_subtier_agency_name,
  awarding_toptier_agency_abbreviation,
  funding_toptier_agency_abbreviation,
  awarding_subtier_agency_abbreviation,
  funding_subtier_agency_abbreviation,

  recipient_unique_id,
  dba_name,
  parent_recipient_unique_id,
  UPPER(COALESCE(recipient_lookup.recipient_name, sub.recipient_name)) AS recipient_name,
  UPPER(COALESCE(parent_recipient_lookup.recipient_name, parent_recipient_name)) AS parent_recipient_name,
  business_type_code,
  business_type_description,

  award_type,
  prime_award_type,

  cfda_id,
  piid,
  fain,

  business_categories,
  prime_recipient_name,

  type_of_contract_pricing,
  extent_competed,
  type_set_aside,
  product_or_service_code,
  psc.description AS product_or_service_description,
  cfda_number,
  cfda_title,

  recipient_location_country_name,
  COALESCE(recipient_location_country_code,'USA') AS recipient_location_country_code,
  recipient_location_city_name,
  recipient_location_state_code,
  recipient_location_state_name,
  LPAD(CAST(CAST((REGEXP_MATCH(recipient_location_county_code, '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0') AS recipient_location_county_code,
  recipient_location_county_name,
  LEFT(COALESCE(recipient_location_zip4, ''), 5) AS recipient_location_zip5,
  recipient_location_street_address,
  LPAD(CAST(CAST((REGEXP_MATCH(recipient_location_congressional_code, '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0') AS recipient_location_congressional_code,

  pop_country_name,
  COALESCE(pop_country_code,'USA') AS pop_country_code,
  pop_state_code,
  pop_state_name,
  LPAD(CAST(CAST((REGEXP_MATCH(pop_county_code, '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 3, '0') AS pop_county_code,
  pop_county_name,
  pop_city_code,
  pop_city_name,
  LEFT(COALESCE(pop_zip4, ''), 5) AS pop_zip5,
  pop_street_address,
  LPAD(CAST(CAST((REGEXP_MATCH(pop_congressional_code, '^[A-Z]*(\d+)(?:\.\d+)?$'))[1] AS smallint) AS text), 2, '0') AS pop_congressional_code
FROM
  subaward AS sub
LEFT OUTER JOIN psc ON product_or_service_code = psc.code
LEFT OUTER JOIN
  (SELECT
    legal_business_name AS recipient_name,
    duns
  FROM recipient_lookup AS rlv
  ) recipient_lookup ON recipient_lookup.duns = recipient_unique_id AND recipient_unique_id IS NOT NULL
LEFT OUTER JOIN
  (SELECT
    legal_business_name AS recipient_name,
    duns
  FROM recipient_lookup AS rlv
  ) parent_recipient_lookup ON parent_recipient_lookup.duns = parent_recipient_unique_id AND parent_recipient_unique_id IS NOT NULL
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
) tas ON (tas.award_id = sub.award_id)
ORDER BY
  amount DESC NULLS LAST WITH DATA;
