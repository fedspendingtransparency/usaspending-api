file_d2_sql_string = """
with valid_file_c as (
select
	distinct
          ta.toptier_code,
	faba.award_id,
	faba.distinct_award_key,
	case
		when faba.piid is not null then true
		else false
	end as is_fpds,
	sa.reporting_fiscal_year as fiscal_year,
	sa.reporting_fiscal_quarter as fiscal_quarter,
	sa.reporting_fiscal_period as fiscal_period,
	sa.quarter_format_flag
from
	int.financial_accounts_by_awards as faba
inner join
            global_temp.submission_attributes as sa
            on
	faba.submission_id = sa.submission_id
inner join
            global_temp.dabs_submission_window_schedule as dsws on
	(
                sa.submission_window_id = dsws.id
		and dsws.submission_reveal_date <= now()
            )
inner join
            global_temp.treasury_appropriation_account as taa on
	(taa.treasury_account_identifier = faba.treasury_account_id)
inner join
            global_temp.toptier_agency as ta on
	(taa.funding_toptier_agency_id = ta.toptier_agency_id)
where
	faba.transaction_obligated_amount is not null
	and sa.reporting_fiscal_year >= 2017
  ),
  quarterly_flag as (
select
	distinct
          toptier_code,
	fiscal_year,
	fiscal_quarter,
	quarter_format_flag
from
	valid_file_c
where
	quarter_format_flag = true
  ),
  valid_file_d as (
select
	fa.toptier_code,
	a.award_id as award_id,
	ts.detached_award_proc_unique as contract_transaction_unique_key,
	ts.generated_unique_award_id as contract_award_unique_key,
	ts.piid as award_id_piid,
	ts.modification_number as modification_number,
	ts.transaction_number as transaction_number,
	a.is_fpds,
	ts.fiscal_year,
	case
        when ql.quarter_format_flag = true then ts.fiscal_quarter * 3
		when ts.fiscal_period = 1 then 2
		else ts.fiscal_period
	end as fiscal_period
from
	rpt.award_search as a
inner join
            (
	select
		*,
		date_part('quarter', tn.action_date + interval '3' month) as fiscal_quarter,
		date_part('month', tn.action_date + interval '3' month) as fiscal_period
	from
		rpt.transaction_search as tn
	where
		tn.action_date >= '2016-10-01'
		and tn.awarding_agency_id is not null
            ) as ts on
	(ts.award_id = a.award_id)
inner join lateral (
	select
		ta.toptier_code
	from
		global_temp.agency as ag
	inner join global_temp.toptier_agency as ta on
		(ag.toptier_agency_id = ta.toptier_agency_id)
	where
		ag.id = a.awarding_agency_id
        ) as fa on
	true
left outer join
            quarterly_flag as ql on
	(
                fa.toptier_code = ql.toptier_code
		and ts.fiscal_year = ql.fiscal_year
		and ts.fiscal_quarter = ql.fiscal_quarter
            )
where
	(
                (a.type in ('07', '08')
		and a.total_subsidy_cost > 0)
	or a.type not in ('07', '08')
            )
		and a.certified_date >= '2016-10-01'
	group by
		fa.toptier_code,
		a.award_id,
		ts.detached_award_proc_unique,
		ts.generated_unique_award_id,
		ts.piid,
		ts.modification_number,
		ts.transaction_number,
		a.is_fpds,
		ts.fiscal_year,
		case
            when ql.quarter_format_flag = true then ts.fiscal_quarter * 3
			when ts.fiscal_period = 1 then 2
			else ts.fiscal_period
		end
  )
    select
	toptier_code,
	award_id,
	contract_transaction_unique_key,
	contract_award_unique_key,
	award_id_piid,
	modification_number,
	transaction_number,
	fiscal_year,
	fiscal_period
from
	valid_file_d as vfd
where
	not exists (
	select
		1
	from
		int.financial_accounts_by_awards as faba
	where
		faba.award_id = vfd.award_id
      )
	and is_fpds = false
"""
