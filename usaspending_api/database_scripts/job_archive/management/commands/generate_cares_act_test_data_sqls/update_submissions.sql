-- Jira Ticket Number(s): DEV-5343
--
--     This script converts base quarterly submissions to monthly submissions.
--
-- Expected CLI:
--
--     None.  Execution is controlled by a supporting Python script.
--
-- Purpose:
--
--     This script generates or participates in the generation of sample CARES Act data for testing
--     and development purposes.  It generates these data from existing data by duplicating and
--     modifying existing submissions and File A/B/C records.  Data points are adjusted in an attempt
--     to make them seem realistic and true to their actual source submissions.
--
--     These data will not be perfect, obviously, but they should be sufficient for testing.
--
-- Life expectancy:
--
--     This file should live until CARES Act features have gone live.
--
--     Be sure to delete all files/directories associated with this ticket:
--         - job_archive/management/commands/generate_cares_act_test_copy_submissions.py
--         - job_archive/management/commands/generate_cares_act_test_def_codes.py
--         - job_archive/management/commands/generate_cares_act_test_helpers.py
--         - job_archive/management/commands/generate_cares_act_test_monthly_submissions.py
--         - job_archive/management/commands/generate_cares_act_test_data_sqls


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
    sa.quarter_format_flag is true and
    sa.toptier_code::int % 3 != 0;  -- only update base a records linked to base submissions

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
    sa.quarter_format_flag is true and
    sa.toptier_code::int % 3 != 0;  -- only update base b records linked to base submissions

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
    sa.quarter_format_flag is true and
    sa.toptier_code::int % 3 != 0;  -- only update base c records linked to base submissions

-- SPLIT --

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
    quarter_format_flag is true and
    toptier_code::int % 3 != 0;  -- only update the submissions we cloned
