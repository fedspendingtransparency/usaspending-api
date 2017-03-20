"""Storage for raw SQL definitions of special models
   (not corresponding to regular tables)

   Run by migrations/0015_make_pa_oc_cube.py"""

sql = (
    (
        "DROP SEQUENCE IF EXISTS db_table = 'financial_accounts_by_program_activity_object_class_summarized_seq",
        "CREATE SEQUENCE financial_accounts_by_program_activity_object_class_summarized_seq",
    ),
    (
        "DROP MATERIALIZED VIEW IF EXISTS financial_accounts_by_program_activity_object_class_summarized",
        """CREATE MATERIALIZED VIEW financial_accounts_by_program_activity_object_class_summarized AS
SELECT NEXTVAL('financial_accounts_by_program_activity_object_class_summarized_seq') AS id,
       program_activity_code,
       object_class,
       treasury_account_id,
       GROUPING(program_activity_code, object_class, treasury_account_id)::bit(3) AS aggregated_col_bitmap,
       ((GROUPING(program_activity_code, object_class, treasury_account_id)::bit(3) & B'100') = B'100') AS is_summed_on_program_activity_code,
       ((GROUPING(program_activity_code, object_class, treasury_account_id)::bit(3) & B'010') = B'010') AS is_summed_on_object_class,
       ((GROUPING(program_activity_code, object_class, treasury_account_id)::bit(3) & B'001') = B'001') AS is_summed_on_tas,
        SUM(ussgl480100_undelivered_orders_obligations_unpaid_fyb) AS ussgl480100_undelivered_orders_obligations_unpaid_fyb,
        SUM(ussgl480100_undelivered_orders_obligations_unpaid_cpe) AS ussgl480100_undelivered_orders_obligations_unpaid_cpe,
        SUM(ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe) AS ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe,
        SUM(ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe) AS ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe,
        SUM(ussgl490100_delivered_orders_obligations_unpaid_fyb) AS ussgl490100_delivered_orders_obligations_unpaid_fyb,
        SUM(ussgl490100_delivered_orders_obligations_unpaid_cpe) AS ussgl490100_delivered_orders_obligations_unpaid_cpe,
        SUM(ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe) AS ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe,
        SUM(ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe) AS ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe,
        SUM(ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb) AS ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb,
        SUM(ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe) AS ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe,
        SUM(ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe) AS ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe,
        SUM(ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe) AS ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe,
        SUM(ussgl490200_delivered_orders_obligations_paid_cpe) AS ussgl490200_delivered_orders_obligations_paid_cpe,
        SUM(ussgl490800_authority_outlayed_not_yet_disbursed_fyb) AS ussgl490800_authority_outlayed_not_yet_disbursed_fyb,
        SUM(ussgl490800_authority_outlayed_not_yet_disbursed_cpe) AS ussgl490800_authority_outlayed_not_yet_disbursed_cpe,
        SUM(ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe) AS ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe,
        SUM(obligations_undelivered_orders_unpaid_total_fyb) AS obligations_undelivered_orders_unpaid_total_fyb,
        SUM(obligations_undelivered_orders_unpaid_total_cpe) AS obligations_undelivered_orders_unpaid_total_cpe,
        SUM(obligations_delivered_orders_unpaid_total_fyb) AS obligations_delivered_orders_unpaid_total_fyb,
        SUM(obligations_delivered_orders_unpaid_total_cpe) AS obligations_delivered_orders_unpaid_total_cpe,
        SUM(gross_outlays_undelivered_orders_prepaid_total_fyb) AS gross_outlays_undelivered_orders_prepaid_total_fyb,
        SUM(gross_outlays_undelivered_orders_prepaid_total_cpe) AS gross_outlays_undelivered_orders_prepaid_total_cpe,
        SUM(gross_outlays_delivered_orders_paid_total_fyb) AS gross_outlays_delivered_orders_paid_total_fyb,
        SUM(gross_outlays_delivered_orders_paid_total_cpe) AS gross_outlays_delivered_orders_paid_total_cpe,
        SUM(gross_outlay_amount_by_program_object_class_fyb) AS gross_outlay_amount_by_program_object_class_fyb,
        SUM(gross_outlay_amount_by_program_object_class_cpe) AS gross_outlay_amount_by_program_object_class_cpe,
        SUM(obligations_incurred_by_program_object_class_cpe) AS obligations_incurred_by_program_object_class_cpe,
        SUM(ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe) AS ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe,
        SUM(ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe) AS ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe,
        SUM(ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe) AS ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
        SUM(ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe) AS ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
        SUM(deobligations_recoveries_refund_pri_program_object_class_cpe) AS deobligations_recoveries_refund_pri_program_object_class_cpe,
        SUM(drv_obligations_incurred_by_program_object_class) AS drv_obligations_incurred_by_program_object_class,
        SUM(drv_obligations_undelivered_orders_unpaid) AS drv_obligations_undelivered_orders_unpaid
FROM   financial_accounts_by_program_activity_object_class
GROUP BY CUBE (program_activity_code, object_class, treasury_account_id)""", ),
)

# TODO: indexing
