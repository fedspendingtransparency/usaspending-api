defc_sql = """
    with eligible_file_c_records as (
        select
            faba.gross_outlay_amount_by_award_cpe,
            faba.transaction_obligated_amount,
            faba.disaster_emergency_fund_code,
            s.reporting_fiscal_year,
            s.reporting_fiscal_period,
            s.submission_id,
            row_number() over(partition by disaster_emergency_fund_code) AS rownum
        from financial_accounts_by_awards faba
        inner join submission_attributes s on faba.submission_id=s.submission_id
        where {award_id_sql} and faba.disaster_emergency_fund_code is not null
    ),
    closed_periods as (
    select
        w.submission_fiscal_year,
        w.submission_fiscal_month,
        w.is_quarter
    from dabs_submission_window_schedule w
    where
        w.submission_reveal_date <= now() -- change "period end" with "window close"
    order by w.submission_fiscal_year desc, w.submission_fiscal_month desc
    ),
    fy_final_outlay_balances as (
        -- Rule: If a balance is not zero at the end of the year, it must be reported in the
        -- final period's submission (month or quarter), otherwise assume it to be zero
        select sum(faba.gross_outlay_amount_by_award_cpe) as prior_fys_outlay,
            faba.disaster_emergency_fund_code
        from eligible_file_c_records faba
        where faba.reporting_fiscal_period = 12
        group by
            faba.disaster_emergency_fund_code,
            faba.reporting_fiscal_period
    ),
    current_fy_outlay_balance as (
        select
            faba.disaster_emergency_fund_code,
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
            faba.disaster_emergency_fund_code,
            faba.reporting_fiscal_year,
            faba.reporting_fiscal_period
    )
    select
        faba.disaster_emergency_fund_code,
        coalesce(sum(faba.transaction_obligated_amount), 0) as obligated_amount,
        coalesce(sum(ffy.prior_fys_outlay), 0) + coalesce(sum(cfy.current_fy_outlay), 0) as total_outlay
    from eligible_file_c_records faba
    left join fy_final_outlay_balances ffy on ffy.disaster_emergency_fund_code = faba.disaster_emergency_fund_code and faba.rownum = 1
    left join current_fy_outlay_balance cfy on cfy.disaster_emergency_fund_code = faba.disaster_emergency_fund_code and faba.rownum = 1
    where faba.rownum = 1
    group by faba.disaster_emergency_fund_code
    order by obligated_amount desc
    """
