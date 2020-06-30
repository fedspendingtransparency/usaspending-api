-- Jira Ticket Number(s): DEV-5343
--
--     This script clones File B and C records and applies DEF codes to them.
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


-- LOG: Generate financial_accounts_by_program_activity_object_class for DEFC "{disaster_emergency_fund_code}"
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
    f.ussgl480100_undelivered_orders_obligations_unpaid_fyb * {adjustment_ratio},
    f.ussgl480100_undelivered_orders_obligations_unpaid_cpe * {adjustment_ratio},
    f.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe * {adjustment_ratio},
    f.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe * {adjustment_ratio},
    f.ussgl490100_delivered_orders_obligations_unpaid_fyb * {adjustment_ratio},
    f.ussgl490100_delivered_orders_obligations_unpaid_cpe * {adjustment_ratio},
    f.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe * {adjustment_ratio},
    f.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe * {adjustment_ratio},
    f.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb * {adjustment_ratio},
    f.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe * {adjustment_ratio},
    f.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe * {adjustment_ratio},
    f.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe * {adjustment_ratio},
    f.ussgl490200_delivered_orders_obligations_paid_cpe * {adjustment_ratio},
    f.ussgl490800_authority_outlayed_not_yet_disbursed_fyb * {adjustment_ratio},
    f.ussgl490800_authority_outlayed_not_yet_disbursed_cpe * {adjustment_ratio},
    f.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe * {adjustment_ratio},
    f.obligations_undelivered_orders_unpaid_total_fyb * {adjustment_ratio},
    f.obligations_undelivered_orders_unpaid_total_cpe * {adjustment_ratio},
    f.obligations_delivered_orders_unpaid_total_fyb * {adjustment_ratio},
    f.obligations_delivered_orders_unpaid_total_cpe * {adjustment_ratio},
    f.gross_outlays_undelivered_orders_prepaid_total_fyb * {adjustment_ratio},
    f.gross_outlays_undelivered_orders_prepaid_total_cpe * {adjustment_ratio},
    f.gross_outlays_delivered_orders_paid_total_fyb * {adjustment_ratio},
    f.gross_outlays_delivered_orders_paid_total_cpe * {adjustment_ratio},
    f.gross_outlay_amount_by_program_object_class_fyb * {adjustment_ratio},
    f.gross_outlay_amount_by_program_object_class_cpe * {adjustment_ratio},
    f.obligations_incurred_by_program_object_class_cpe * {adjustment_ratio},
    f.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe * {adjustment_ratio},
    f.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe * {adjustment_ratio},
    f.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe * {adjustment_ratio},
    f.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe * {adjustment_ratio},
    f.deobligations_recoveries_refund_pri_program_object_class_cpe * {adjustment_ratio},
    f.reporting_period_start,
    f.reporting_period_end,
    f.create_date,
    f.update_date,
    f.final_of_fy,
    f.object_class_id,
    f.program_activity_id,
    f.submission_id,
    f.treasury_account_id,
    '{disaster_emergency_fund_code}'
from
    financial_accounts_by_program_activity_object_class as f
    inner join submission_attributes as sa on sa.submission_id = f.submission_id
where
    (sa._base_submission_id + f.treasury_account_id + f.object_class_id + f.program_activity_id) % {divisor} = 0 and
    f.disaster_emergency_fund_code is null and
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa._base_submission_id % 24 != 0;  -- Make sure a few submissions receive no DEF codes.

-- SPLIT --

-- LOG: Generate financial_accounts_by_awards for DEFC "{disaster_emergency_fund_code}"
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
    disaster_emergency_fund_code
)
select
    f.data_source,
    f.piid,
    f.parent_award_id,
    f.fain,
    f.uri,
    f.ussgl480100_undelivered_orders_obligations_unpaid_fyb * {adjustment_ratio},
    f.ussgl480100_undelivered_orders_obligations_unpaid_cpe * {adjustment_ratio},
    f.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe * {adjustment_ratio},
    f.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe * {adjustment_ratio},
    f.ussgl490100_delivered_orders_obligations_unpaid_fyb * {adjustment_ratio},
    f.ussgl490100_delivered_orders_obligations_unpaid_cpe * {adjustment_ratio},
    f.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe * {adjustment_ratio},
    f.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe * {adjustment_ratio},
    f.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb * {adjustment_ratio},
    f.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe * {adjustment_ratio},
    f.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe * {adjustment_ratio},
    f.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe * {adjustment_ratio},
    f.ussgl490200_delivered_orders_obligations_paid_cpe * {adjustment_ratio},
    f.ussgl490800_authority_outlayed_not_yet_disbursed_fyb * {adjustment_ratio},
    f.ussgl490800_authority_outlayed_not_yet_disbursed_cpe * {adjustment_ratio},
    f.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe * {adjustment_ratio},
    f.obligations_undelivered_orders_unpaid_total_cpe * {adjustment_ratio},
    f.obligations_delivered_orders_unpaid_total_fyb * {adjustment_ratio},
    f.obligations_delivered_orders_unpaid_total_cpe * {adjustment_ratio},
    f.gross_outlays_undelivered_orders_prepaid_total_fyb * {adjustment_ratio},
    f.gross_outlays_undelivered_orders_prepaid_total_cpe * {adjustment_ratio},
    f.gross_outlays_delivered_orders_paid_total_fyb * {adjustment_ratio},
    f.gross_outlay_amount_by_award_fyb * {adjustment_ratio},
    f.gross_outlay_amount_by_award_cpe * {adjustment_ratio},
    f.obligations_incurred_total_by_award_cpe * {adjustment_ratio},
    f.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe * {adjustment_ratio},
    f.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe * {adjustment_ratio},
    f.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe * {adjustment_ratio},
    f.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe * {adjustment_ratio},
    f.deobligations_recoveries_refunds_of_prior_year_by_award_cpe * {adjustment_ratio},
    f.obligations_undelivered_orders_unpaid_total_fyb * {adjustment_ratio},
    f.gross_outlays_delivered_orders_paid_total_cpe * {adjustment_ratio},
    f.transaction_obligated_amount * {adjustment_ratio},
    f.reporting_period_start,
    f.reporting_period_end,
    f.create_date,
    f.update_date,
    f.award_id,
    f.object_class_id,
    f.program_activity_id,
    f.submission_id,
    f.treasury_account_id,
    '{disaster_emergency_fund_code}'
from
    financial_accounts_by_awards as f
    inner join submission_attributes as sa on sa.submission_id = f.submission_id
where
    (sa._base_submission_id + f.treasury_account_id + f.object_class_id + f.program_activity_id) % {divisor} = 0 and
    f.disaster_emergency_fund_code is null and
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa._base_submission_id % 24 != 0;  -- Make sure a few submissions receive no DEF codes.

-- SPLIT --

-- LOG: Adjust base financial_accounts_by_program_activity_object_class amounts for DEFC "{disaster_emergency_fund_code}"
update
    financial_accounts_by_program_activity_object_class as f
set
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = ussgl480100_undelivered_orders_obligations_unpaid_fyb * (1.0 - {adjustment_ratio}),
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = ussgl480100_undelivered_orders_obligations_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl490100_delivered_orders_obligations_unpaid_fyb = ussgl490100_delivered_orders_obligations_unpaid_fyb * (1.0 - {adjustment_ratio}),
    ussgl490100_delivered_orders_obligations_unpaid_cpe = ussgl490100_delivered_orders_obligations_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb * (1.0 - {adjustment_ratio}),
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe * (1.0 - {adjustment_ratio}),
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe * (1.0 - {adjustment_ratio}),
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe * (1.0 - {adjustment_ratio}),
    ussgl490200_delivered_orders_obligations_paid_cpe = ussgl490200_delivered_orders_obligations_paid_cpe * (1.0 - {adjustment_ratio}),
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = ussgl490800_authority_outlayed_not_yet_disbursed_fyb * (1.0 - {adjustment_ratio}),
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = ussgl490800_authority_outlayed_not_yet_disbursed_cpe * (1.0 - {adjustment_ratio}),
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe * (1.0 - {adjustment_ratio}),
    obligations_undelivered_orders_unpaid_total_fyb = obligations_undelivered_orders_unpaid_total_fyb * (1.0 - {adjustment_ratio}),
    obligations_undelivered_orders_unpaid_total_cpe = obligations_undelivered_orders_unpaid_total_cpe * (1.0 - {adjustment_ratio}),
    obligations_delivered_orders_unpaid_total_fyb = obligations_delivered_orders_unpaid_total_fyb * (1.0 - {adjustment_ratio}),
    obligations_delivered_orders_unpaid_total_cpe = obligations_delivered_orders_unpaid_total_cpe * (1.0 - {adjustment_ratio}),
    gross_outlays_undelivered_orders_prepaid_total_fyb = gross_outlays_undelivered_orders_prepaid_total_fyb * (1.0 - {adjustment_ratio}),
    gross_outlays_undelivered_orders_prepaid_total_cpe = gross_outlays_undelivered_orders_prepaid_total_cpe * (1.0 - {adjustment_ratio}),
    gross_outlays_delivered_orders_paid_total_fyb = gross_outlays_delivered_orders_paid_total_fyb * (1.0 - {adjustment_ratio}),
    gross_outlays_delivered_orders_paid_total_cpe = gross_outlays_delivered_orders_paid_total_cpe * (1.0 - {adjustment_ratio}),
    gross_outlay_amount_by_program_object_class_fyb = gross_outlay_amount_by_program_object_class_fyb * (1.0 - {adjustment_ratio}),
    gross_outlay_amount_by_program_object_class_cpe = gross_outlay_amount_by_program_object_class_cpe * (1.0 - {adjustment_ratio}),
    obligations_incurred_by_program_object_class_cpe = obligations_incurred_by_program_object_class_cpe * (1.0 - {adjustment_ratio}),
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe * (1.0 - {adjustment_ratio}),
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe * (1.0 - {adjustment_ratio}),
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe * (1.0 - {adjustment_ratio}),
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe * (1.0 - {adjustment_ratio}),
    deobligations_recoveries_refund_pri_program_object_class_cpe = deobligations_recoveries_refund_pri_program_object_class_cpe * (1.0 - {adjustment_ratio})
from
    submission_attributes as sa
where
    (sa._base_submission_id + f.treasury_account_id + f.object_class_id + f.program_activity_id) % {divisor} = 0 and
    f.disaster_emergency_fund_code is null and
    sa.submission_id = f.submission_id and
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa._base_submission_id % 24 != 0;  -- Make sure a few submissions receive no DEF codes.

-- SPLIT --

-- LOG: Adjust base financial_accounts_by_awards amounts for DEFC "{disaster_emergency_fund_code}"
update
    financial_accounts_by_awards as f
set
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = ussgl480100_undelivered_orders_obligations_unpaid_fyb * (1.0 - {adjustment_ratio}),
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = ussgl480100_undelivered_orders_obligations_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl490100_delivered_orders_obligations_unpaid_fyb = ussgl490100_delivered_orders_obligations_unpaid_fyb * (1.0 - {adjustment_ratio}),
    ussgl490100_delivered_orders_obligations_unpaid_cpe = ussgl490100_delivered_orders_obligations_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe * (1.0 - {adjustment_ratio}),
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb * (1.0 - {adjustment_ratio}),
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe * (1.0 - {adjustment_ratio}),
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe * (1.0 - {adjustment_ratio}),
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe * (1.0 - {adjustment_ratio}),
    ussgl490200_delivered_orders_obligations_paid_cpe = ussgl490200_delivered_orders_obligations_paid_cpe * (1.0 - {adjustment_ratio}),
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = ussgl490800_authority_outlayed_not_yet_disbursed_fyb * (1.0 - {adjustment_ratio}),
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = ussgl490800_authority_outlayed_not_yet_disbursed_cpe * (1.0 - {adjustment_ratio}),
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe * (1.0 - {adjustment_ratio}),
    obligations_undelivered_orders_unpaid_total_cpe = obligations_undelivered_orders_unpaid_total_cpe * (1.0 - {adjustment_ratio}),
    obligations_delivered_orders_unpaid_total_fyb = obligations_delivered_orders_unpaid_total_fyb * (1.0 - {adjustment_ratio}),
    obligations_delivered_orders_unpaid_total_cpe = obligations_delivered_orders_unpaid_total_cpe * (1.0 - {adjustment_ratio}),
    gross_outlays_undelivered_orders_prepaid_total_fyb = gross_outlays_undelivered_orders_prepaid_total_fyb * (1.0 - {adjustment_ratio}),
    gross_outlays_undelivered_orders_prepaid_total_cpe = gross_outlays_undelivered_orders_prepaid_total_cpe * (1.0 - {adjustment_ratio}),
    gross_outlays_delivered_orders_paid_total_fyb = gross_outlays_delivered_orders_paid_total_fyb * (1.0 - {adjustment_ratio}),
    gross_outlay_amount_by_award_fyb = gross_outlay_amount_by_award_fyb * (1.0 - {adjustment_ratio}),
    gross_outlay_amount_by_award_cpe = gross_outlay_amount_by_award_cpe * (1.0 - {adjustment_ratio}),
    obligations_incurred_total_by_award_cpe = obligations_incurred_total_by_award_cpe * (1.0 - {adjustment_ratio}),
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe * (1.0 - {adjustment_ratio}),
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe * (1.0 - {adjustment_ratio}),
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe * (1.0 - {adjustment_ratio}),
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe * (1.0 - {adjustment_ratio}),
    deobligations_recoveries_refunds_of_prior_year_by_award_cpe = deobligations_recoveries_refunds_of_prior_year_by_award_cpe * (1.0 - {adjustment_ratio}),
    obligations_undelivered_orders_unpaid_total_fyb = obligations_undelivered_orders_unpaid_total_fyb * (1.0 - {adjustment_ratio}),
    gross_outlays_delivered_orders_paid_total_cpe = gross_outlays_delivered_orders_paid_total_cpe * (1.0 - {adjustment_ratio}),
    transaction_obligated_amount = transaction_obligated_amount * (1.0 - {adjustment_ratio})
from
    submission_attributes as sa
where
    (sa._base_submission_id + f.treasury_account_id + f.object_class_id + f.program_activity_id) % {divisor} = 0 and
    f.disaster_emergency_fund_code is null and
    sa.submission_id = f.submission_id and
    sa.reporting_fiscal_year = {filter_fiscal_year} and
    sa.reporting_fiscal_period = {filter_fiscal_period} and
    sa._base_submission_id % 24 != 0;  -- Make sure a few submissions receive no DEF codes.
