-- Needs to be present in the Postgres DB if data needs to be retrieved for Elasticsearch
DROP VIEW IF EXISTS transaction_delta_view;

CREATE VIEW transaction_delta_view AS
SELECT
  UTM.transaction_id,
  FPDS.detached_award_proc_unique,
  FABS.afa_generated_unique,

  CASE
    WHEN FPDS.detached_award_proc_unique IS NOT NULL THEN 'CONT_TX_' || UPPER(FPDS.detached_award_proc_unique)
    WHEN FABS.afa_generated_unique IS NOT NULL THEN 'ASST_TX_' || UPPER(FABS.afa_generated_unique)
    ELSE NULL  -- if this happens: Activate Batsignal
  END AS generated_unique_transaction_id,

  CASE
    WHEN UTM.type IN ('02', '03', '04', '05', '06', '10', '07', '08', '09', '11') AND UTM.fain IS NOT NULL THEN UTM.fain
    WHEN UTM.piid IS NOT NULL THEN UTM.piid  -- contracts. Did it this way to easily handle IDV contracts
    ELSE UTM.uri
  END AS display_award_id,

  TM.update_date,
  UTM.modification_number,
  AWD.generated_unique_award_id,
  UTM.award_id,
  UTM.piid,
  UTM.fain,
  UTM.uri,
  UTM.transaction_description AS award_description,

  UTM.product_or_service_code,
  UTM.product_or_service_description,
  UTM.naics_code,
  UTM.naics_description,
  AWD.type_description,
  UTM.award_category,

  UTM.recipient_unique_id,
  UTM.recipient_name,
  UTM.recipient_hash,

  UTM.parent_recipient_unique_id,
  UPPER(PRL.legal_business_name) AS parent_recipient_name,
  PRL.recipient_hash as parent_recipient_hash,

  UTM.action_date,
  DATE(UTM.action_date + interval '3 months') AS fiscal_action_date,
  AWD.period_of_performance_start_date,
  AWD.period_of_performance_current_end_date,
  FPDS.ordering_period_end_date,
  UTM.fiscal_year AS transaction_fiscal_year,
  AWD.fiscal_year AS award_fiscal_year,
  AWD.total_obligation AS award_amount,
  UTM.federal_action_obligation AS transaction_amount,
  UTM.face_value_loan_guarantee,
  UTM.original_loan_subsidy_cost,
  UTM.generated_pragmatic_obligation,

  UTM.awarding_agency_id,
  UTM.funding_agency_id,
  TAA_LOOKUP.id AS awarding_toptier_agency_id,
  TFA_LOOKUP.id AS funding_toptier_agency_id,
  SAA_LOOKUP.id AS awarding_subtier_agency_id,
  SFA_LOOKUP.id AS funding_subtier_agency_id,
  AA.toptier_agency_id AS awarding_toptier_id,
  FA.toptier_agency_id AS funding_toptier_id,
  AA.subtier_agency_id AS awarding_subtier_id,
  FA.subtier_agency_id AS funding_subtier_id,
  UTM.awarding_toptier_agency_name,
  UTM.funding_toptier_agency_name,
  UTM.awarding_subtier_agency_name,
  UTM.funding_subtier_agency_name,
  UTM.awarding_toptier_agency_abbreviation,
  UTM.funding_toptier_agency_abbreviation,
  UTM.awarding_subtier_agency_abbreviation,
  UTM.funding_subtier_agency_abbreviation,
  TAA.toptier_code AS awarding_toptier_agency_code,
  TFA.toptier_code AS funding_toptier_agency_code,
  SAA.subtier_code AS awarding_subtier_agency_code,
  SFA.subtier_code AS funding_subtier_agency_code,

  CFDA.id AS cfda_id,
  UTM.cfda_number,
  UTM.cfda_title,
  '' AS cfda_popular_name,
  UTM.type_of_contract_pricing,
  UTM.type_set_aside,
  UTM.extent_competed,
  UTM.type,

  UTM.pop_country_code,
  UTM.pop_country_name,
  UTM.pop_state_code,
  UTM.pop_county_code,
  UTM.pop_county_name,
  UTM.pop_zip5,
  UTM.pop_congressional_code,
  UTM.pop_city_name,

  UTM.recipient_location_country_code,
  UTM.recipient_location_country_name,
  UTM.recipient_location_state_code,
  UTM.recipient_location_county_code,
  UTM.recipient_location_county_name,
  UTM.recipient_location_zip5,
  UTM.recipient_location_congressional_code,
  UTM.recipient_location_city_name,

  TREASURY_ACCT.treasury_accounts,
  FEDERAL_ACCT.federal_accounts,
  UTM.business_categories

FROM universal_transaction_matview UTM
INNER JOIN transaction_normalized TM ON (UTM.transaction_id = TM.id)
LEFT JOIN transaction_fpds FPDS ON (UTM.transaction_id = FPDS.transaction_id)
LEFT JOIN transaction_fabs FABS ON (UTM.transaction_id = FABS.transaction_id)
LEFT JOIN awards AWD ON (UTM.award_id = AWD.id)
-- Similar joins are already performed on universal_transaction_matview, however, to avoid making the matview larger
-- than needed they have been placed here. Feel free to phase out if the columns gained from the following joins are
-- added to the universal_transaction_matview.
LEFT JOIN agency AA ON (TM.awarding_agency_id = AA.id)
LEFT JOIN agency FA ON (TM.funding_agency_id = FA.id)
LEFT JOIN toptier_agency TAA ON (AA.toptier_agency_id = TAA.toptier_agency_id)
LEFT JOIN subtier_agency SAA ON (AA.subtier_agency_id = SAA.subtier_agency_id)
LEFT JOIN toptier_agency TFA ON (FA.toptier_agency_id = TFA.toptier_agency_id)
LEFT JOIN subtier_agency SFA ON (FA.subtier_agency_id = SFA.subtier_agency_id)
-- The next four joins are needed to get the Agency ID for all combinations of Awarding / Funding
-- and Toptier / Subtier agencies
LEFT JOIN (
    SELECT id, toptier_agency_id, toptier_flag
    FROM agency
    WHERE toptier_flag = true
) TAA_LOOKUP on (AA.toptier_agency_id = TAA_LOOKUP.toptier_agency_id)
LEFT JOIN (
    SELECT id, toptier_agency_id, toptier_flag
    FROM agency
    WHERE toptier_flag = true
) TFA_LOOKUP on (FA.toptier_agency_id = TFA_LOOKUP.toptier_agency_id)
LEFT JOIN (
    SELECT id, subtier_agency_id, toptier_flag
    FROM agency
    WHERE toptier_flag = false
) SAA_LOOKUP on (AA.subtier_agency_id = SAA_LOOKUP.subtier_agency_id)
LEFT JOIN (
    SELECT id, subtier_agency_id, toptier_flag
    FROM agency
    WHERE toptier_flag = false
) SFA_LOOKUP on (FA.subtier_agency_id = SFA_LOOKUP.subtier_agency_id)
LEFT JOIN references_cfda CFDA ON (FABS.cfda_number = CFDA.program_number)
LEFT JOIN recipient_lookup PRL ON (PRL.duns = UTM.parent_recipient_unique_id AND UTM.parent_recipient_unique_id IS NOT NULL)
LEFT JOIN (
  SELECT
    faba.award_id,
    JSONB_AGG(
      DISTINCT JSONB_BUILD_OBJECT(
        'aid', taa.agency_id,
        'ata', taa.allocation_transfer_agency_id,
        'main', taa.main_account_code,
        'sub', taa.sub_account_code,
        'bpoa', taa.beginning_period_of_availability,
        'epoa', taa.ending_period_of_availability,
        'a', taa.availability_type_code
       )
     ) treasury_accounts
 FROM
   federal_account fa
   INNER JOIN treasury_appropriation_account taa ON (fa.id = taa.federal_account_id)
   INNER JOIN financial_accounts_by_awards faba ON (taa.treasury_account_identifier = faba.treasury_account_id)
 WHERE
   faba.award_id IS NOT NULL
 GROUP BY
   faba.award_id
) TREASURY_ACCT ON (TREASURY_ACCT.award_id = UTM.award_id)
LEFT JOIN (
  SELECT
    faba.award_id,
    JSONB_AGG(
      DISTINCT JSONB_BUILD_OBJECT(
        'id', fa.id,
        'account_title', fa.account_title,
        'federal_account_code', fa.federal_account_code
      )
    ) federal_accounts
  FROM
    federal_account fa
    INNER JOIN treasury_appropriation_account taa ON fa.id = taa.federal_account_id
    INNER JOIN financial_accounts_by_awards faba ON taa.treasury_account_identifier = faba.treasury_account_id
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
) FEDERAL_ACCT ON (FEDERAL_ACCT.award_id = UTM.award_id);
