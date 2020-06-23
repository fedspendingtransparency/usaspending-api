defc_sql = """
    with eligible_file_c_records as (
        select
            faba.gross_outlay_amount_by_award_cpe,
            faba.transaction_obligated_amount,
            faba.disaster_emergency_fund_code,
            s.reporting_fiscal_year,
            s.reporting_fiscal_period,
            s.submission_id
        from financial_accounts_by_awards faba
        inner join submission_attributes s on faba.submission_id=s.submission_id
        where {award_id_sql} and faba.disaster_emergency_fund_code is not null
    ),
    closed_periods as (
        -- This is NOT right, since period dates are not the same as submission window close dates,
        -- but an approximation for illustration purposes until we have the reporting dates table up
        select
            distinct concat(p.reporting_fiscal_year::text,
            lpad(p.reporting_fiscal_period::text, 2, '0')) as fyp,
            p.reporting_fiscal_year, p.reporting_fiscal_period
        from submission_attributes p
        where now()::date > p.reporting_period_end -- change "period end" with "window close"
        order by p.reporting_fiscal_year desc, p.reporting_fiscal_period desc
    ),
    fy_final_outlay_balances as (
        -- Rule: If a balance is not zero at the end of the year, it must be reported in the
        -- final period's submission (month or quarter), otherwise assume it to be zero
        select sum(faba.gross_outlay_amount_by_award_cpe) as prior_fys_outlay,
            faba.disaster_emergency_fund_code
        from eligible_file_c_records faba
        group by
            faba.disaster_emergency_fund_code,
            faba.reporting_fiscal_period
        having faba.reporting_fiscal_period = 12
        and sum(faba.gross_outlay_amount_by_award_cpe) > 0
    ),
    current_fy_outlay_balance as (
        select
            faba.reporting_fiscal_year,
            faba.reporting_fiscal_period,
            faba.disaster_emergency_fund_code,
            sum(faba.gross_outlay_amount_by_award_cpe) as current_fy_outlay
        from eligible_file_c_records faba
        group by
            faba.reporting_fiscal_year,
            faba.reporting_fiscal_period,
            faba.disaster_emergency_fund_code
        having concat(faba.reporting_fiscal_year::text, lpad(faba.reporting_fiscal_period::text, 2, '0')) in
        (select max(fyp) from closed_periods) and sum(faba.gross_outlay_amount_by_award_cpe) > 0)
    select
        faba.disaster_emergency_fund_code,
        coalesce(ffy.prior_fys_outlay, 0) + coalesce(cfy.current_fy_outlay, 0) as total_outlay,
        coalesce(sum(faba.transaction_obligated_amount), 0) as obligated_amount
    from eligible_file_c_records faba
    left join fy_final_outlay_balances ffy on ffy.disaster_emergency_fund_code = faba.disaster_emergency_fund_code
    left join current_fy_outlay_balance cfy
        on cfy.reporting_fiscal_period != 12 -- don't duplicate the year-end period's value if in unclosed period 01
        and cfy.disaster_emergency_fund_code = faba.disaster_emergency_fund_code
    group by faba.disaster_emergency_fund_code, total_outlay;
    """
