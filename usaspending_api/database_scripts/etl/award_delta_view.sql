DROP VIEW IF EXISTS award_delta_view;
CREATE VIEW award_delta_view AS
SELECT
  vw_es_award_search.award_id,
  vw_es_award_search.generated_unique_award_id,
  CASE
    WHEN vw_es_award_search.type IN ('02', '03', '04', '05', '06', '10', '07', '08', '09', '11') AND vw_es_award_search.fain IS NOT NULL THEN vw_es_award_search.fain
    WHEN vw_es_award_search.piid IS NOT NULL THEN vw_es_award_search.piid  -- contracts. Did it this way to easily handle IDV contracts
    ELSE vw_es_award_search.uri
  END AS display_award_id,

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
  vw_es_award_search.recipient_levels,
  CASE	  
    WHEN vw_es_award_search.recipient_hash IS NULL or vw_es_award_search.recipient_levels IS NULL	
      THEN	
        CONCAT(	
          '{"name":"', vw_es_award_search.recipient_name,	
          '","unique_id":"', vw_es_award_search.recipient_unique_id,	
          '","hash":"","levels":""}'	
        )	
    ELSE	
      CONCAT(	
        '{"name":"', vw_es_award_search.recipient_name,	
        '","unique_id":"', vw_es_award_search.recipient_unique_id,	
        '","hash":"', vw_es_award_search.recipient_hash,	
        '","levels":"', vw_es_award_search.recipient_levels, '"}'	
      )	
  END AS recipient_agg_key,

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
  CASE
    WHEN vw_es_award_search.funding_toptier_agency_name IS NOT NULL
      THEN CONCAT(
        '{"name":"', vw_es_award_search.funding_toptier_agency_name,
        '","code":"', vw_es_award_search.funding_toptier_agency_code,
        '","id":"', (SELECT a1.id FROM agency a1 WHERE a1.toptier_agency_id = (SELECT a2.toptier_agency_id FROM agency a2 WHERE a2.id = vw_es_award_search.funding_agency_id) ORDER BY a1.toptier_flag DESC, a1.id LIMIT 1), '"}'
      )
    ELSE NULL
  END AS funding_toptier_agency_agg_key,
  CASE
    WHEN vw_es_award_search.funding_subtier_agency_name IS NOT NULL
      THEN CONCAT(
        '{"name":"', vw_es_award_search.funding_subtier_agency_name,
        '","code":"', vw_es_award_search.funding_subtier_agency_code,
        '","id":"', (SELECT a1.id FROM agency a1 WHERE a1.toptier_agency_id = (SELECT a2.toptier_agency_id FROM agency a2 WHERE a2.id = vw_es_award_search.funding_agency_id) ORDER BY a1.toptier_flag DESC, a1.id LIMIT 1), '"}'
      )
    ELSE NULL
  END AS funding_subtier_agency_agg_key,

  vw_es_award_search.recipient_location_country_code,
  vw_es_award_search.recipient_location_country_name,
  vw_es_award_search.recipient_location_state_code,
  vw_es_award_search.recipient_location_state_name,
  vw_es_award_search.recipient_location_state_fips,
  vw_es_award_search.recipient_location_state_population,
  vw_es_award_search.recipient_location_county_code,
  vw_es_award_search.recipient_location_county_name,
  vw_es_award_search.recipient_location_county_population,
  vw_es_award_search.recipient_location_congressional_code,
  vw_es_award_search.recipient_location_congressional_population,
  vw_es_award_search.recipient_location_zip5,
  vw_es_award_search.recipient_location_city_name,

  vw_es_award_search.pop_country_code,
  vw_es_award_search.pop_country_name,
  vw_es_award_search.pop_state_code,
  vw_es_award_search.pop_state_name,
  vw_es_award_search.pop_state_fips,
  vw_es_award_search.pop_state_population,
  vw_es_award_search.pop_county_code,
  vw_es_award_search.pop_county_name,
  vw_es_award_search.pop_county_population,
  vw_es_award_search.pop_zip5,
  vw_es_award_search.pop_congressional_code,
  vw_es_award_search.pop_congressional_population,
  vw_es_award_search.pop_city_name,
  vw_es_award_search.pop_city_code,

  vw_es_award_search.cfda_number,
  vw_es_award_search.cfda_program_title as cfda_title,
  vw_es_award_search.cfda_id,
  vw_es_award_search.cfda_url,
  CASE
    WHEN vw_es_award_search.cfda_number IS NOT NULL
      THEN CONCAT(
        '{"code":"', vw_es_award_search.cfda_number,
        '","description":"', cfda_program_title,
        '","id":"', vw_es_award_search.cfda_id,
        '","url":"', CASE WHEN vw_es_award_search.cfda_url = 'None;' THEN NULL ELSE vw_es_award_search.cfda_url END, '"}'
      )
    ELSE NULL
  END AS cfda_agg_key,

  vw_es_award_search.sai_number,
  vw_es_award_search.type_of_contract_pricing,
  vw_es_award_search.extent_competed,
  vw_es_award_search.type_set_aside,

  vw_es_award_search.product_or_service_code,
  vw_es_award_search.product_or_service_description,
  vw_es_award_search.naics_code,
  vw_es_award_search.naics_description,

  CASE
    WHEN
        vw_es_award_search.recipient_location_state_code IS NOT NULL
        AND vw_es_award_search.recipient_location_county_code IS NOT NULL
      THEN CONCAT(
        '{"country_code":"', vw_es_award_search.recipient_location_country_code,
        '","state_code":"', vw_es_award_search.recipient_location_state_code,
        '","state_fips":"', vw_es_award_search.recipient_location_state_fips,
        '","county_code":"', vw_es_award_search.recipient_location_county_code,
        '","county_name":"', vw_es_award_search.recipient_location_county_name,
        '","population":"', vw_es_award_search.recipient_location_county_population, '"}'
      )
    ELSE NULL
  END AS recipient_location_county_agg_key,
  CASE
    WHEN
        vw_es_award_search.recipient_location_state_code IS NOT NULL
        AND vw_es_award_search.recipient_location_congressional_code IS NOT NULL
      THEN CONCAT(
        '{"country_code":"', vw_es_award_search.recipient_location_country_code,
        '","state_code":"', vw_es_award_search.recipient_location_state_code,
        '","state_fips":"', vw_es_award_search.recipient_location_state_fips,
        '","congressional_code":"', vw_es_award_search.recipient_location_congressional_code,
        '","population":"', vw_es_award_search.recipient_location_congressional_population, '"}'
      )
    ELSE NULL
  END AS recipient_location_congressional_agg_key,
  CASE
    WHEN vw_es_award_search.recipient_location_state_code IS NOT NULL
      THEN CONCAT(
        '{"country_code":"', vw_es_award_search.recipient_location_country_code,
        '","state_code":"', vw_es_award_search.recipient_location_state_code,
        '","state_name":"', vw_es_award_search.recipient_location_state_name,
        '","population":"', vw_es_award_search.recipient_location_state_population, '"}'
      )
    ELSE NULL
  END AS recipient_location_state_agg_key,

  CASE
    WHEN vw_es_award_search.pop_state_code IS NOT NULL AND vw_es_award_search.pop_county_code IS NOT NULL
      THEN CONCAT(
        '{"country_code":"', vw_es_award_search.pop_country_code,
        '","state_code":"', vw_es_award_search.pop_state_code,
        '","state_fips":"', vw_es_award_search.pop_state_fips,
        '","county_code":"', vw_es_award_search.pop_county_code,
        '","county_name":"', vw_es_award_search.pop_county_name,
        '","population":"', vw_es_award_search.pop_county_population, '"}'
      )
    ELSE NULL
  END AS pop_county_agg_key,
  CASE
    WHEN vw_es_award_search.pop_state_code IS NOT NULL AND vw_es_award_search.pop_congressional_code IS NOT NULL
      THEN CONCAT(
        '{"country_code":"', vw_es_award_search.pop_country_code,
        '","state_code":"', vw_es_award_search.pop_state_code,
        '","state_fips":"', vw_es_award_search.pop_state_fips,
        '","congressional_code":"', vw_es_award_search.pop_congressional_code,
        '","population":"', vw_es_award_search.pop_congressional_population, '"}'
      )
    ELSE NULL
  END AS pop_congressional_agg_key,
  CASE
    WHEN vw_es_award_search.pop_state_code IS NOT NULL
      THEN CONCAT(
        '{"country_code":"', vw_es_award_search.pop_country_code,
        '","state_code":"', vw_es_award_search.pop_state_code,
         '","state_name":"', vw_es_award_search.pop_state_name,
        '","population":"', vw_es_award_search.pop_state_population, '"}'
      )
    ELSE NULL
  END AS pop_state_agg_key,

  vw_es_award_search.tas_paths,
  vw_es_award_search.tas_components,
  vw_es_award_search.disaster_emergency_fund_codes AS disaster_emergency_fund_codes,
  vw_es_award_search.total_covid_outlay,
  vw_es_award_search.total_covid_obligation
FROM vw_es_award_search
;
