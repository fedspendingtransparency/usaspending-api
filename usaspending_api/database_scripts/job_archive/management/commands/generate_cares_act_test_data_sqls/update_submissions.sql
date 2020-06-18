-- This script is about converting base quarterly submissions to monthly submissions.


-- LOG: Convert quarterly submission_attributes to monthly for FY{filter_fiscal_year}P{filter_fiscal_period}
update
    submission_attributes
set
    reporting_period_start = '{reporting_period_start}'::date,
    reporting_period_end = '{reporting_period_end}'::date,
    quarter_format_flag = false
where
    reporting_fiscal_year = {filter_fiscal_year} and
    reporting_fiscal_period = {filter_fiscal_period} and
    submission_id % 3 != 0;  -- only update the submissions we cloned

-- SPLIT --

-- LOG: Convert quarterly appropriation_account_balances to monthly for FY{filter_fiscal_year}P{filter_fiscal_period}
update
    appropriation_account_balances as aab
set
    reporting_period_start = '{reporting_period_start}'::date,
    reporting_period_end = '{reporting_period_end}'::date
from
    submission_attributes as sa
where
    sa.submission_id = aab.submission_id and
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa.submission_id % 3 != 0;  -- only update base a records linked to base submissions

-- SPLIT --

-- LOG: Convert quarterly financial_accounts_by_program_activity_object_class to monthly for FY{filter_fiscal_year}P{filter_fiscal_period}
update
    financial_accounts_by_program_activity_object_class as f
set
    reporting_period_start = '{reporting_period_start}'::date,
    reporting_period_end = '{reporting_period_end}'::date
from
    submission_attributes as sa
where
    sa.submission_id = f.submission_id and
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa.submission_id % 3 != 0;  -- only update base b records linked to base submissions

-- SPLIT --

-- LOG: Convert quarterly financial_accounts_by_awards to monthly for FY{filter_fiscal_year}P{filter_fiscal_period}
update
    financial_accounts_by_awards as f
set
    reporting_period_start = '{reporting_period_start}'::date,
    reporting_period_end = '{reporting_period_end}'::date,
    transaction_obligated_amount = f.transaction_obligated_amount * {adjustment_ratio}
from
    submission_attributes as sa
where
    sa.submission_id = f.submission_id and
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa.submission_id % 3 != 0;  -- only update base c records linked to base submissions
