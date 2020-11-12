DROP VIEW IF EXISTS award_delta_view;
CREATE VIEW award_delta_view AS
SELECT
  vw_es_award_search.award_id,
  vw_es_award_search.generated_unique_award_id,
  vw_es_award_search.display_award_id,

  vw_es_award_search.category,
  vw_es_award_search.type,
  vw_es_award_search.type_description,
  vw_es_award_search.piid,
  vw_es_award_search.fain,
  vw_es_award_search.uri,
  vw_es_award_search.total_obligation,
  vw_es_award_search.description,
  vw_es_award_search.award_amount,
  vw_es_award_search.total_subsidy_cost,
  vw_es_award_search.total_loan_value,
  vw_es_award_search.update_date,

  vw_es_award_search.recipient_name,
  vw_es_award_search.recipient_unique_id,
  vw_es_award_search.recipient_hash,
  vw_es_award_search.recipient_agg_key,

  vw_es_award_search.parent_recipient_unique_id,
  vw_es_award_search.business_categories,

  vw_es_award_search.action_date,
  vw_es_award_search.fiscal_year,
  vw_es_award_search.last_modified_date,
  vw_es_award_search.period_of_performance_start_date,
  vw_es_award_search.period_of_performance_current_end_date,
  vw_es_award_search.date_signed,
  vw_es_award_search.ordering_period_end_date,

  vw_es_award_search.original_loan_subsidy_cost,
  vw_es_award_search.face_value_loan_guarantee,

  vw_es_award_search.awarding_agency_id,
  vw_es_award_search.funding_agency_id,
  vw_es_award_search.awarding_toptier_agency_name,
  vw_es_award_search.funding_toptier_agency_name,
  vw_es_award_search.awarding_subtier_agency_name,
  vw_es_award_search.funding_subtier_agency_name,
  vw_es_award_search.awarding_toptier_agency_code,
  vw_es_award_search.funding_toptier_agency_code,
  vw_es_award_search.awarding_subtier_agency_code,
  vw_es_award_search.funding_subtier_agency_code,
  vw_es_award_search.funding_toptier_agency_agg_key,
  vw_es_award_search.funding_subtier_agency_agg_key,

  vw_es_award_search.recipient_location_country_code,
  vw_es_award_search.recipient_location_country_name,
  vw_es_award_search.recipient_location_state_code,
  vw_es_award_search.recipient_location_county_code,
  vw_es_award_search.recipient_location_county_name,
  vw_es_award_search.recipient_location_congressional_code,
  vw_es_award_search.recipient_location_zip5,
  vw_es_award_search.recipient_location_city_name,

  vw_es_award_search.pop_country_code,
  vw_es_award_search.pop_country_name,
  vw_es_award_search.pop_state_code,
  vw_es_award_search.pop_county_code,
  vw_es_award_search.pop_county_name,
  vw_es_award_search.pop_zip5,
  vw_es_award_search.pop_congressional_code,
  vw_es_award_search.pop_city_name,
  vw_es_award_search.pop_city_code,

  vw_es_award_search.cfda_number,
  vw_es_award_search.cfda_program_title as cfda_title,
  
  vw_es_award_search.sai_number,
  vw_es_award_search.type_of_contract_pricing,
  vw_es_award_search.extent_competed,
  vw_es_award_search.type_set_aside,

  vw_es_award_search.product_or_service_code,
  vw_es_award_search.product_or_service_description,
  vw_es_award_search.naics_code,
  vw_es_award_search.naics_description,

  vw_es_award_search.recipient_location_county_agg_key,
  vw_es_award_search.recipient_location_congressional_agg_key,
  vw_es_award_search.recipient_location_state_agg_key,

  vw_es_award_search.pop_county_agg_key,
  vw_es_award_search.pop_congressional_agg_key,
  vw_es_award_search.pop_state_agg_key,

  vw_es_award_search.tas_paths,
  vw_es_award_search.tas_components,
  vw_es_award_search.disaster_emergency_fund_codes,
  vw_es_award_search.total_covid_outlay,
  vw_es_award_search.total_covid_obligation
FROM vw_es_award_search
;
