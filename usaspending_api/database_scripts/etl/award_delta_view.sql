DROP VIEW IF EXISTS award_delta_view;
CREATE VIEW award_delta_view AS
with closed_periods as (
    select
        w.submission_fiscal_year,
        w.submission_fiscal_month,
        w.is_quarter
    from dabs_submission_window_schedule w
    where
    	-- For COVID, look only after a certain date (likely on/after 2020-04-01, or the start of FY2020 FYP07)
        w.period_start_date >= '2020-04-01' -- using older date to see data surface
        and w.submission_reveal_date <= now() -- change "period end" with "window close"
    order by w.submission_fiscal_year desc, w.submission_fiscal_month desc
),
eligible_submissions as (
	select s.reporting_fiscal_year, s.reporting_fiscal_period, s.submission_id
    from submission_attributes s
    inner join closed_periods c
    	on c.submission_fiscal_year = s.reporting_fiscal_year
    	and c.submission_fiscal_month = s.reporting_fiscal_period
    	and c.is_quarter = s.quarter_format_flag
),
eligible_file_c_records as (
    select
        faba.disaster_emergency_fund_code,
        faba.treasury_account_id,
        faba.object_class_id,
        faba.award_id,
        faba.transaction_obligated_amount,
        faba.gross_outlay_amount_by_award_cpe,
        s.reporting_fiscal_year,
        s.reporting_fiscal_period,
        s.submission_id
    from financial_accounts_by_awards faba
    inner join submission_attributes s on faba.submission_id=s.submission_id
    inner join disaster_emergency_fund_code defc
    	on defc.group_name = 'covid_19'
		and defc.code = faba.disaster_emergency_fund_code
),
fy_final_outlay_balances_by_dim as (
   -- Rule: If a balance is not zero at the end of the year, it must be reported in the
   -- final period's submission (month or quarter), otherwise assume it to be zero
   select
   	faba.award_id,
   	sum(faba.gross_outlay_amount_by_award_cpe) as prior_fys_outlay
   from eligible_file_c_records faba
   where faba.reporting_fiscal_period = 12
   and (faba.gross_outlay_amount_by_award_cpe is not null and faba.gross_outlay_amount_by_award_cpe != 0) -- negative or positive balance
   group by
       faba.award_id,
       faba.reporting_fiscal_period
),
current_fy_outlay_balance_by_dim as (
   select
       faba.award_id,
       faba.reporting_fiscal_year,
       faba.reporting_fiscal_period,
       sum(faba.gross_outlay_amount_by_award_cpe) as current_fy_outlay
   from eligible_file_c_records faba
   where
   	faba.reporting_fiscal_period != 12 -- don't duplicate the year-end period's value if in unclosed period 01 or 02 (since there is no P01 submission)
   	and (faba.reporting_fiscal_year, faba.reporting_fiscal_period) in (
   		-- Most recent closed period
	    	select c.submission_fiscal_year, c.submission_fiscal_month
	    	from closed_periods c
	    	order by c.submission_fiscal_year desc, c.submission_fiscal_month desc
	    	limit 1
   	)
   	and (faba.gross_outlay_amount_by_award_cpe is not null and faba.gross_outlay_amount_by_award_cpe != 0) -- negative or positive balance
   group by
       faba.award_id,
       faba.reporting_fiscal_year,
       faba.reporting_fiscal_period
)
SELECT
  vw_award_search.award_id,
  a.generated_unique_award_id,
    CASE
    WHEN vw_award_search.type IN ('02', '03', '04', '05', '06', '10', '07', '08', '09', '11') AND vw_award_search.fain IS NOT NULL THEN vw_award_search.fain
    WHEN vw_award_search.piid IS NOT NULL THEN vw_award_search.piid  -- contracts. Did it this way to easily handle IDV contracts
    ELSE vw_award_search.uri
  END AS display_award_id,

  vw_award_search.category,
  vw_award_search.type,
  vw_award_search.type_description,
  vw_award_search.piid,
  vw_award_search.fain,
  vw_award_search.uri,
  vw_award_search.total_obligation,
  vw_award_search.description,
  vw_award_search.award_amount,
  vw_award_search.total_subsidy_cost,
  vw_award_search.total_loan_value,
  a.update_date,

  vw_award_search.recipient_name,
  vw_award_search.recipient_unique_id,
  recipient_lookup.recipient_hash,
  vw_award_search.parent_recipient_unique_id,
  vw_award_search.business_categories,

  vw_award_search.action_date,
  vw_award_search.fiscal_year,
  vw_award_search.last_modified_date,
  vw_award_search.period_of_performance_start_date,
  vw_award_search.period_of_performance_current_end_date,
  vw_award_search.date_signed,
  vw_award_search.ordering_period_end_date,

  vw_award_search.original_loan_subsidy_cost,
  vw_award_search.face_value_loan_guarantee,

  vw_award_search.awarding_agency_id,
  vw_award_search.funding_agency_id,
  vw_award_search.awarding_toptier_agency_name,
  vw_award_search.funding_toptier_agency_name,
  vw_award_search.awarding_subtier_agency_name,
  vw_award_search.funding_subtier_agency_name,
  vw_award_search.awarding_toptier_agency_code,
  vw_award_search.funding_toptier_agency_code,
  vw_award_search.awarding_subtier_agency_code,
  vw_award_search.funding_subtier_agency_code,

  vw_award_search.recipient_location_country_code,
  vw_award_search.recipient_location_country_name,
  vw_award_search.recipient_location_state_code,
  vw_award_search.recipient_location_county_code,
  vw_award_search.recipient_location_county_name,
  vw_award_search.recipient_location_congressional_code,
  vw_award_search.recipient_location_zip5,
  vw_award_search.recipient_location_city_name,

  vw_award_search.pop_country_code,
  vw_award_search.pop_country_name,
  vw_award_search.pop_state_code,
  vw_award_search.pop_county_code,
  vw_award_search.pop_county_name,
  vw_award_search.pop_zip5,
  vw_award_search.pop_congressional_code,
  vw_award_search.pop_city_name,
  vw_award_search.pop_city_code,

  vw_award_search.cfda_number,
  vw_award_search.sai_number,
  vw_award_search.type_of_contract_pricing,
  vw_award_search.extent_competed,
  vw_award_search.type_set_aside,

  vw_award_search.product_or_service_code,
  vw_award_search.product_or_service_description,
  vw_award_search.naics_code,
  vw_award_search.naics_description,
  TREASURY_ACCT.tas_paths,
  TREASURY_ACCT.tas_components,
  DEFC.disaster_emergency_fund_codes as disaster_emergency_fund_codes,
  DEFC.total_covid_obligation,
  DEFC.total_covid_outlay
FROM vw_award_search
INNER JOIN awards a ON (a.id = vw_award_search.award_id)
LEFT JOIN (
  SELECT
    recipient_hash,
    legal_business_name AS recipient_name,
    duns
  FROM
    recipient_lookup AS rlv
) recipient_lookup ON (recipient_lookup.duns = vw_award_search.recipient_unique_id AND vw_award_search.recipient_unique_id IS NOT NULL)
LEFT JOIN (
    select
        faba.award_id,
        coalesce(sum(faba.transaction_obligated_amount), 0) as total_covid_obligation,
        coalesce(sum(ffy.prior_fys_outlay), 0) + coalesce(sum(cfy.current_fy_outlay), 0) as total_covid_outlay,
        ARRAY_AGG(DISTINCT faba.disaster_emergency_fund_code) AS disaster_emergency_fund_codes
        from eligible_file_c_records faba
        left join fy_final_outlay_balances_by_dim ffy on ffy.award_id = faba.award_id
        left join current_fy_outlay_balance_by_dim cfy on cfy.award_id = faba.award_id
        group by faba.award_id
) DEFC ON (DEFC.award_id = vw_award_search.award_id)
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
) TREASURY_ACCT ON (TREASURY_ACCT.award_id = vw_award_search.award_id);
