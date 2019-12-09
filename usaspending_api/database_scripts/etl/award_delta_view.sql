DROP VIEW IF EXISTS award_delta_view;

CREATE VIEW award_delta_view AS
WITH views AS (
    SELECT mv_contract_award_search.*, a.generated_unique_award_id, a.update_date
        FROM mv_contract_award_search INNER JOIN awards a ON
            a.id = mv_contract_award_search.award_id
    UNION
    SELECT mv_grant_award_search.*, a.generated_unique_award_id, a.update_date
        FROM mv_grant_award_search INNER JOIN awards a ON
            a.id = mv_grant_award_search.award_id
    UNION
    SELECT mv_directpayment_award_search.*, a.generated_unique_award_id, a.update_date
        FROM mv_directpayment_award_search INNER JOIN awards a ON
         a.id = mv_directpayment_award_search.award_id
    UNION
    SELECT mv_idv_award_search.*, a.generated_unique_award_id, a.update_date
        FROM mv_idv_award_search INNER JOIN awards a ON
            a.id = mv_idv_award_search.award_id
    UNION
    SELECT mv_loan_award_search.*, a.generated_unique_award_id, a.update_date
        FROM mv_loan_award_search INNER JOIN awards a ON
            a.id = mv_loan_award_search.award_id
    UNION
    SELECT mv_other_award_search.*, a.generated_unique_award_id, a.update_date
        FROM mv_other_award_search INNER JOIN awards a ON a.id = mv_other_award_search.award_id
)
SELECT
  DISTINCT views.award_id,
  views.generated_unique_award_id,
    CASE
    WHEN views.type IN ('02', '03', '04', '05', '06', '10', '07', '08', '09', '11') AND views.fain IS NOT NULL THEN views.fain
    WHEN views.piid IS NOT NULL THEN views.piid  -- contracts. Did it this way to easily handle IDV contracts
    ELSE views.uri
  END AS display_award_id,

  views.category,
  views.type,
  views.type_description,
  views.piid,
  views.fain,
  views.uri,
  views.total_obligation,
  views.description,
  views.total_obl_bin,
  views.total_subsidy_cost,
  views.total_loan_value,
  views.update_date,

  views.recipient_id,
  views.recipient_name,
  views.recipient_unique_id,
  CONCAT(rp.recipient_hash, '-', case when views.parent_recipient_unique_id is null then 'R' else 'C' end) as recipient_hash,
  views.parent_recipient_unique_id,
  views.business_categories,

  views.action_date,
  views.fiscal_year,
  views.last_modified_date,
  views.period_of_performance_start_date,
  views.period_of_performance_current_end_date,
  views.date_signed,
  views.ordering_period_end_date,

  views.original_loan_subsidy_cost,
  views.face_value_loan_guarantee,

  views.awarding_agency_id,
  views.funding_agency_id,
  views.awarding_toptier_agency_name,
  views.funding_toptier_agency_name,
  views.awarding_subtier_agency_name,
  views.funding_subtier_agency_name,
  views.awarding_toptier_agency_code,
  views.funding_toptier_agency_code,
  views.awarding_subtier_agency_code,
  views.funding_subtier_agency_code,

  views.recipient_location_country_code,
  views.recipient_location_country_name,
  views.recipient_location_state_code,
  views.recipient_location_county_code,
  views.recipient_location_county_name,
  views.recipient_location_congressional_code,
  views.recipient_location_zip5,
  views.recipient_location_city_name,

  views.pop_country_code,
  views.pop_country_name,
  views.pop_state_code,
  views.pop_county_code,
  views.pop_county_name,
  views.pop_zip5,
  views.pop_congressional_code,
  views.pop_city_name,

  views.cfda_number,
  views.sai_number,
  views.pulled_from,
  views.type_of_contract_pricing,
  views.extent_competed,
  views.type_set_aside,

  views.product_or_service_code,
  views.product_or_service_description,
  views.naics_code,
  views.naics_description,
  TREASURY_ACCT.treasury_accounts
  FROM views
  LEFT JOIN
  (SELECT
    recipient_hash,
    legal_business_name AS recipient_name,
    duns
  FROM recipient_lookup AS rlv
  ) recipient_lookup ON recipient_lookup.duns = views.recipient_unique_id AND views.recipient_unique_id IS NOT NULL
  LEFT JOIN
  recipient_profile rp  on recipient_lookup.recipient_hash = rp.recipient_hash
  LEFT OUTER JOIN (
  SELECT
    faba.award_id,
    JSONB_AGG(
      JSONB_BUILD_OBJECT(
        'aid', taa.agency_id,
        'ata', taa.allocation_transfer_agency_id,
        'main', taa.main_account_code,
        'sub', taa.sub_account_code,
        'bpoa', taa.beginning_period_of_availability,
        'epoa', taa.beginning_period_of_availability,
        'a', taa.availability_type_code
      )
    ) treasury_accounts
  FROM
    federal_account fa
    INNER JOIN treasury_appropriation_account taa ON fa.id = taa.federal_account_id
    INNER JOIN financial_accounts_by_awards faba ON taa.treasury_account_identifier = faba.treasury_account_id
  WHERE
    faba.award_id IS NOT NULL
  GROUP BY
    faba.award_id
 TREASURY_ACCT ON (FED_ACCT.award_id = views.award_id);