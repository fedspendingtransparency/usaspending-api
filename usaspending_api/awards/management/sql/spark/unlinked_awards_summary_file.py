summary_file = """
select
	pdta.name as agency_name,
	pdta.toptier_code as toptier_code,
	rao.fiscal_year,
	rao.fiscal_period,
	null as percent_of_total_budget,
	max(sa.published_date) as most_recent_update,
	null as missing_tas_accounts_count,
	sum(rao.total_diff_approp_ocpa_obligated_amounts) as reporting_difference_in_obligations,
	sum(rao.unlinked_procurement_c_awards + rao.unlinked_procurement_d_awards) as unlinked_contract_award_count,
	sum(rao.unlinked_assistance_c_awards + rao.unlinked_assistance_d_awards) as unlinked_assistance_award_count,
	sum(rao.unlinked_procurement_c_awards + rao.unlinked_procurement_d_awards) + sum(rao.unlinked_assistance_c_awards + rao.unlinked_assistance_d_awards) as total_unlinked_awards
from
	global_temp.published_dabs_toptier_agency as pdta
left join global_temp.reporting_agency_overview rao
on
	rao.toptier_code = pdta.toptier_code
left join global_temp.submission_attributes sa
on
	sa.toptier_code = pdta.toptier_code
group by
	pdta.name,
	pdta.toptier_code,
	rao.fiscal_year,
	rao.fiscal_period
"""
