file_c_sql_string = """
    with valid_file_c as (
    select
        distinct
    ta.toptier_code,
        faba.piid,
        faba.fain,
        faba.uri,
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
    )
    select
        toptier_code,
        piid,
        fain,
        uri,
        award_id,
        is_fpds,
        fiscal_year,
        fiscal_period
    from
        valid_file_c
    where
        award_id is null
"""
