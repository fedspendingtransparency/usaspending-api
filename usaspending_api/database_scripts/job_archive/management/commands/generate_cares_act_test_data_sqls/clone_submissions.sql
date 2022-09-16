-- Jira Ticket Number(s): DEV-5343
--
--     This script clones about 2/3 of base quarterly submissions into monthly submissions for
--     other periods in the quarter.
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


-- LOG: Generate monthly submission_attributes for FY{filter_fiscal_year}P{reporting_fiscal_period}
insert into submission_attributes (
    submission_id,
    certified_date,
    toptier_code,
    reporting_period_start,
    reporting_period_end,
    reporting_fiscal_year,
    reporting_fiscal_quarter,
    reporting_fiscal_period,
    quarter_format_flag,
    create_date,
    update_date,
    reporting_agency_name,
    published_date,
    is_final_balances_for_fy,
    submission_window_id,
    _base_submission_id
)
select
    submission_id + {submission_id_shift},  -- to prevent id collisions
    certified_date,
    toptier_code,
    '{reporting_period_start}'::date,
    '{reporting_period_end}'::date,
    reporting_fiscal_year,
    reporting_fiscal_quarter,
    {reporting_fiscal_period},
    false,  -- quarter_format_flag
    create_date,
    update_date,
    reporting_agency_name,
    '{reporting_period_end}'::date - interval '5 days',  -- published_date
    is_final_balances_for_fy,
    (SELECT id FROM dabs_submission_window_schedule dsws WHERE dsws.submission_fiscal_year = {destination_fiscal_year} AND dsws.submission_fiscal_month = {destination_fiscal_period} AND dsws.is_quarter = FALSE) AS submission_window_id,
    submission_id  -- _base_submission_id
from
    submission_attributes
where
    reporting_fiscal_year = {filter_fiscal_year} and
    reporting_fiscal_period = {filter_fiscal_period} and
    quarter_format_flag is true and
    toptier_code::int % 3 != 0;  -- only clone about half of the submissions

-- SPLIT --

-- LOG: Generate monthly appropriation_account_balances for FY{filter_fiscal_year}P{reporting_fiscal_period}
insert into appropriation_account_balances (
    data_source,
    budget_authority_unobligated_balance_brought_forward_fyb,
    adjustments_to_unobligated_balance_brought_forward_cpe,
    budget_authority_appropriated_amount_cpe,
    borrowing_authority_amount_total_cpe,
    contract_authority_amount_total_cpe,
    spending_authority_from_offsetting_collections_amount_cpe,
    other_budgetary_resources_amount_cpe,
    total_budgetary_resources_amount_cpe,
    gross_outlay_amount_by_tas_cpe,
    deobligations_recoveries_refunds_by_tas_cpe,
    unobligated_balance_cpe,
    status_of_budgetary_resources_total_cpe,
    obligations_incurred_total_by_tas_cpe,
    reporting_period_start,
    reporting_period_end,
    create_date,
    update_date,
    final_of_fy,
    submission_id,
    treasury_account_identifier
)
select
    abb.data_source,
    abb.budget_authority_unobligated_balance_brought_forward_fyb,
    abb.adjustments_to_unobligated_balance_brought_forward_cpe,
    abb.budget_authority_appropriated_amount_cpe,
    abb.borrowing_authority_amount_total_cpe,
    abb.contract_authority_amount_total_cpe,
    abb.spending_authority_from_offsetting_collections_amount_cpe,
    abb.other_budgetary_resources_amount_cpe,
    abb.total_budgetary_resources_amount_cpe,
    abb.gross_outlay_amount_by_tas_cpe,
    abb.deobligations_recoveries_refunds_by_tas_cpe,
    abb.unobligated_balance_cpe,
    abb.status_of_budgetary_resources_total_cpe,
    abb.obligations_incurred_total_by_tas_cpe,
    '{reporting_period_start}'::date,
    '{reporting_period_end}'::date,
    abb.create_date,
    abb.update_date,
    false,  -- final_of_fy,
    abb.submission_id + {submission_id_shift},  -- to match submission shift
    abb.treasury_account_identifier
from
    appropriation_account_balances as abb
    inner join submission_attributes as sa on sa.submission_id = abb.submission_id
where
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa.quarter_format_flag is true and
    sa.toptier_code::int % 3 != 0;  -- only clone file a records linked to submissions we cloned

-- SPLIT --

-- LOG: Generate monthly financial_accounts_by_program_activity_object_class for FY{filter_fiscal_year}P{reporting_fiscal_period}
insert into financial_accounts_by_program_activity_object_class (
    ussgl480100_undelivered_orders_obligations_unpaid_fyb,
    ussgl480100_undelivered_orders_obligations_unpaid_cpe,
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe,
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe,
    ussgl490100_delivered_orders_obligations_unpaid_fyb,
    ussgl490100_delivered_orders_obligations_unpaid_cpe,
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe,
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe,
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb,
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe,
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe,
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe,
    ussgl490200_delivered_orders_obligations_paid_cpe,
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb,
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe,
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe,
    obligations_undelivered_orders_unpaid_total_fyb,
    obligations_undelivered_orders_unpaid_total_cpe,
    obligations_delivered_orders_unpaid_total_fyb,
    obligations_delivered_orders_unpaid_total_cpe,
    gross_outlays_undelivered_orders_prepaid_total_fyb,
    gross_outlays_undelivered_orders_prepaid_total_cpe,
    gross_outlays_delivered_orders_paid_total_fyb,
    gross_outlays_delivered_orders_paid_total_cpe,
    gross_outlay_amount_by_program_object_class_fyb,
    gross_outlay_amount_by_program_object_class_cpe,
    obligations_incurred_by_program_object_class_cpe,
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe,
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe,
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
    deobligations_recoveries_refund_pri_program_object_class_cpe,
    reporting_period_start,
    reporting_period_end,
    create_date,
    update_date,
    final_of_fy,
    object_class_id,
    program_activity_id,
    submission_id,
    treasury_account_id,
    disaster_emergency_fund_code
)
select
    f.ussgl480100_undelivered_orders_obligations_unpaid_fyb,
    f.ussgl480100_undelivered_orders_obligations_unpaid_cpe,
    f.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe,
    f.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe,
    f.ussgl490100_delivered_orders_obligations_unpaid_fyb,
    f.ussgl490100_delivered_orders_obligations_unpaid_cpe,
    f.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe,
    f.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe,
    f.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb,
    f.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe,
    f.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe,
    f.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe,
    f.ussgl490200_delivered_orders_obligations_paid_cpe,
    f.ussgl490800_authority_outlayed_not_yet_disbursed_fyb,
    f.ussgl490800_authority_outlayed_not_yet_disbursed_cpe,
    f.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe,
    f.obligations_undelivered_orders_unpaid_total_fyb,
    f.obligations_undelivered_orders_unpaid_total_cpe,
    f.obligations_delivered_orders_unpaid_total_fyb,
    f.obligations_delivered_orders_unpaid_total_cpe,
    f.gross_outlays_undelivered_orders_prepaid_total_fyb,
    f.gross_outlays_undelivered_orders_prepaid_total_cpe,
    f.gross_outlays_delivered_orders_paid_total_fyb,
    f.gross_outlays_delivered_orders_paid_total_cpe,
    f.gross_outlay_amount_by_program_object_class_fyb,
    f.gross_outlay_amount_by_program_object_class_cpe,
    f.obligations_incurred_by_program_object_class_cpe,
    f.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe,
    f.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe,
    f.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
    f.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
    f.deobligations_recoveries_refund_pri_program_object_class_cpe,
    '{reporting_period_start}'::date,
    '{reporting_period_end}'::date,
    f.create_date,
    f.update_date,
    false,  -- f.final_of_fy
    f.object_class_id,
    f.program_activity_id,
    f.submission_id + {submission_id_shift},  -- to match submission shift
    f.treasury_account_id,
    f.disaster_emergency_fund_code
from
    financial_accounts_by_program_activity_object_class as f
    inner join submission_attributes as sa on sa.submission_id = f.submission_id
where
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa.quarter_format_flag is true and
    sa.toptier_code::int % 3 != 0;  -- only clone file a records linked to submissions we cloned

-- SPLIT --

-- LOG: Generate monthly financial_accounts_by_awards for FY{filter_fiscal_year}P{reporting_fiscal_period}
insert into financial_accounts_by_awards (
    data_source,
    piid,
    parent_award_id,
    fain,
    uri,
    ussgl480100_undelivered_orders_obligations_unpaid_fyb,
    ussgl480100_undelivered_orders_obligations_unpaid_cpe,
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe,
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe,
    ussgl490100_delivered_orders_obligations_unpaid_fyb,
    ussgl490100_delivered_orders_obligations_unpaid_cpe,
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe,
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe,
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb,
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe,
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe,
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe,
    ussgl490200_delivered_orders_obligations_paid_cpe,
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb,
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe,
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe,
    obligations_undelivered_orders_unpaid_total_cpe,
    obligations_delivered_orders_unpaid_total_fyb,
    obligations_delivered_orders_unpaid_total_cpe,
    gross_outlays_undelivered_orders_prepaid_total_fyb,
    gross_outlays_undelivered_orders_prepaid_total_cpe,
    gross_outlays_delivered_orders_paid_total_fyb,
    gross_outlay_amount_by_award_fyb,
    gross_outlay_amount_by_award_cpe,
    obligations_incurred_total_by_award_cpe,
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe,
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe,
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
    deobligations_recoveries_refunds_of_prior_year_by_award_cpe,
    obligations_undelivered_orders_unpaid_total_fyb,
    gross_outlays_delivered_orders_paid_total_cpe,
    transaction_obligated_amount,
    reporting_period_start,
    reporting_period_end,
    create_date,
    update_date,
    award_id,
    object_class_id,
    program_activity_id,
    submission_id,
    treasury_account_id,
    disaster_emergency_fund_code,
    distinct_award_key
)
select
    f.data_source,
    f.piid,
    f.parent_award_id,
    f.fain,
    f.uri,
    f.ussgl480100_undelivered_orders_obligations_unpaid_fyb,
    f.ussgl480100_undelivered_orders_obligations_unpaid_cpe,
    f.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe,
    f.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe,
    f.ussgl490100_delivered_orders_obligations_unpaid_fyb,
    f.ussgl490100_delivered_orders_obligations_unpaid_cpe,
    f.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe,
    f.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe,
    f.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb,
    f.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe,
    f.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe,
    f.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe,
    f.ussgl490200_delivered_orders_obligations_paid_cpe,
    f.ussgl490800_authority_outlayed_not_yet_disbursed_fyb,
    f.ussgl490800_authority_outlayed_not_yet_disbursed_cpe,
    f.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe,
    f.obligations_undelivered_orders_unpaid_total_cpe,
    f.obligations_delivered_orders_unpaid_total_fyb,
    f.obligations_delivered_orders_unpaid_total_cpe,
    f.gross_outlays_undelivered_orders_prepaid_total_fyb,
    f.gross_outlays_undelivered_orders_prepaid_total_cpe,
    f.gross_outlays_delivered_orders_paid_total_fyb,
    f.gross_outlay_amount_by_award_fyb,
    f.gross_outlay_amount_by_award_cpe,
    f.obligations_incurred_total_by_award_cpe,
    f.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe,
    f.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe,
    f.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
    f.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
    f.deobligations_recoveries_refunds_of_prior_year_by_award_cpe,
    f.obligations_undelivered_orders_unpaid_total_fyb,
    f.gross_outlays_delivered_orders_paid_total_cpe,
    f.transaction_obligated_amount * {adjustment_ratio},
    '{reporting_period_start}'::date,
    '{reporting_period_end}'::date,
    f.create_date,
    f.update_date,
    f.award_id,
    f.object_class_id,
    f.program_activity_id,
    f.submission_id + {submission_id_shift},  -- to match submission shift
    f.treasury_account_id,
    f.disaster_emergency_fund_code,
    UPPER(CONCAT(piid, '|', parent_award_id, '|', fain, '|', uri))
from
    financial_accounts_by_awards as f
    inner join submission_attributes as sa on sa.submission_id = f.submission_id
where
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa.quarter_format_flag is true and
    sa.toptier_code::int % 3 != 0;  -- only clone file a records linked to submissions we cloned
