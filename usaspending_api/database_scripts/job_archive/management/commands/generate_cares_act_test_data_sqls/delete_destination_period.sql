-- Jira Ticket Number(s): DEV-5343
--
--     Delete submissions and File A, B, and C records for a specific fiscal year and month.
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


-- LOG: Delete financial_accounts_by_awards for FY{filter_fiscal_year}P{filter_fiscal_period}
delete
from    financial_accounts_by_awards
where   submission_id in (
            select  submission_id
            from    submission_attributes
            where   reporting_fiscal_year = {filter_fiscal_year} and
                    reporting_fiscal_period = {filter_fiscal_period}
        );

-- SPLIT --

-- LOG: Delete financial_accounts_by_program_activity_object_class for FY{filter_fiscal_year}P{filter_fiscal_period}
delete
from    financial_accounts_by_program_activity_object_class
where   submission_id in (
            select  submission_id
            from    submission_attributes
            where   reporting_fiscal_year = {filter_fiscal_year} and
                    reporting_fiscal_period = {filter_fiscal_period}
        );

-- SPLIT --

-- LOG: Delete appropriation_account_balances for FY{filter_fiscal_year}P{filter_fiscal_period}
delete
from    appropriation_account_balances
where   submission_id in (
            select  submission_id
            from    submission_attributes
            where   reporting_fiscal_year = {filter_fiscal_year} and
                    reporting_fiscal_period = {filter_fiscal_period}
        );

-- SPLIT --

-- LOG: Delete submission_attributes for FY{filter_fiscal_year}P{filter_fiscal_period}
delete
from    submission_attributes
where   reporting_fiscal_year = {filter_fiscal_year} and
        reporting_fiscal_period = {filter_fiscal_period};
