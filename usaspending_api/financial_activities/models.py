from django.db import models

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.models import ObjectClass, RefProgramActivity
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.models import DataSourceTrackedModel


class FinancialAccountsByProgramActivityObjectClass(DataSourceTrackedModel):
    financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    program_activity = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, null=True, db_index=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    object_class = models.ForeignKey(ObjectClass, models.DO_NOTHING, null=True, db_index=True)
    treasury_account = models.ForeignKey(TreasuryAppropriationAccount, models.CASCADE, related_name="program_balances", null=True)
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl490100_delivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl490100_delivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl490200_delivered_orders_obligations_paid_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    obligations_undelivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    obligations_undelivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    obligations_delivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    obligations_delivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    gross_outlays_undelivered_orders_prepaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    gross_outlays_undelivered_orders_prepaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    gross_outlays_delivered_orders_paid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    gross_outlays_delivered_orders_paid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    gross_outlay_amount_by_program_object_class_fyb = models.DecimalField(max_digits=21, decimal_places=2)
    gross_outlay_amount_by_program_object_class_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    obligations_incurred_by_program_object_class_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    deobligations_recoveries_refund_pri_program_object_class_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    drv_obligations_incurred_by_program_object_class = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    drv_obligations_undelivered_orders_unpaid = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_program_activity_object_class'

    # TODO: is the self-joining SQL below do-able via the ORM?
    QUARTERLY_SQL = """
        SELECT
            current.financial_accounts_by_program_activity_object_class_id,
            sub.submission_id AS submission_id,
            current.treasury_account_id,
            current.object_class_id,
            current.program_activity_id,
            current.ussgl480100_undelivered_orders_obligations_unpaid_fyb - COALESCE(previous.ussgl480100_undelivered_orders_obligations_unpaid_fyb, 0) AS ussgl480100_undelivered_orders_obligations_unpaid_fyb,
            current.ussgl480100_undelivered_orders_obligations_unpaid_cpe - COALESCE(previous.ussgl480100_undelivered_orders_obligations_unpaid_cpe, 0) AS ussgl480100_undelivered_orders_obligations_unpaid_cpe,
            current.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe - COALESCE(previous.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe, 0) AS ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe,
            current.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe - COALESCE(previous.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe, 0) AS ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe,
            current.ussgl490100_delivered_orders_obligations_unpaid_fyb - COALESCE(previous.ussgl490100_delivered_orders_obligations_unpaid_fyb, 0) AS ussgl490100_delivered_orders_obligations_unpaid_fyb,
            current.ussgl490100_delivered_orders_obligations_unpaid_cpe - COALESCE(previous.ussgl490100_delivered_orders_obligations_unpaid_cpe, 0) AS ussgl490100_delivered_orders_obligations_unpaid_cpe,
            current.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe - COALESCE(previous.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe, 0) AS ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe,
            current.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe - COALESCE(previous.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe, 0) AS ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe,
            current.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb - COALESCE(previous.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb, 0) AS ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb,
            current.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe - COALESCE(previous.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe, 0) AS ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe,
            current.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe - COALESCE(previous.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe, 0) AS ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe,
            current.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe - COALESCE(previous.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe, 0) AS ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe,
            current.ussgl490200_delivered_orders_obligations_paid_cpe - COALESCE(previous.ussgl490200_delivered_orders_obligations_paid_cpe, 0) AS ussgl490200_delivered_orders_obligations_paid_cpe,
            current.ussgl490800_authority_outlayed_not_yet_disbursed_fyb - COALESCE(previous.ussgl490800_authority_outlayed_not_yet_disbursed_fyb, 0) AS ussgl490800_authority_outlayed_not_yet_disbursed_fyb,
            current.ussgl490800_authority_outlayed_not_yet_disbursed_cpe - COALESCE(previous.ussgl490800_authority_outlayed_not_yet_disbursed_cpe, 0) AS ussgl490800_authority_outlayed_not_yet_disbursed_cpe,
            current.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe - COALESCE(previous.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe, 0) AS ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe,
            current.obligations_undelivered_orders_unpaid_total_fyb - COALESCE(previous.obligations_undelivered_orders_unpaid_total_fyb, 0) AS obligations_undelivered_orders_unpaid_total_fyb,
            current.obligations_undelivered_orders_unpaid_total_cpe - COALESCE(previous.obligations_undelivered_orders_unpaid_total_cpe, 0) AS obligations_undelivered_orders_unpaid_total_cpe,
            current.obligations_delivered_orders_unpaid_total_fyb - COALESCE(previous.obligations_delivered_orders_unpaid_total_fyb, 0) AS obligations_delivered_orders_unpaid_total_fyb,
            current.obligations_delivered_orders_unpaid_total_cpe - COALESCE(previous.obligations_delivered_orders_unpaid_total_cpe, 0) AS obligations_delivered_orders_unpaid_total_cpe,
            current.gross_outlays_undelivered_orders_prepaid_total_fyb - COALESCE(previous.gross_outlays_undelivered_orders_prepaid_total_fyb, 0) AS gross_outlays_undelivered_orders_prepaid_total_fyb,
            current.gross_outlays_undelivered_orders_prepaid_total_cpe - COALESCE(previous.gross_outlays_undelivered_orders_prepaid_total_cpe, 0) AS gross_outlays_undelivered_orders_prepaid_total_cpe,
            current.gross_outlays_delivered_orders_paid_total_fyb - COALESCE(previous.gross_outlays_delivered_orders_paid_total_fyb, 0) AS gross_outlays_delivered_orders_paid_total_fyb,
            current.gross_outlays_delivered_orders_paid_total_cpe - COALESCE(previous.gross_outlays_delivered_orders_paid_total_cpe, 0) AS gross_outlays_delivered_orders_paid_total_cpe,
            current.gross_outlay_amount_by_program_object_class_fyb - COALESCE(previous.gross_outlay_amount_by_program_object_class_fyb, 0) AS gross_outlay_amount_by_program_object_class_fyb,
            current.gross_outlay_amount_by_program_object_class_cpe - COALESCE(previous.gross_outlay_amount_by_program_object_class_cpe, 0) AS gross_outlay_amount_by_program_object_class_cpe,
            current.obligations_incurred_by_program_object_class_cpe - COALESCE(previous.obligations_incurred_by_program_object_class_cpe, 0) AS obligations_incurred_by_program_object_class_cpe,
            current.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe - COALESCE(previous.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe, 0) AS ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe,
            current.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe - COALESCE(previous.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe, 0) AS ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe,
            current.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe - COALESCE(previous.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0) AS ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
            current.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe - COALESCE(previous.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0) AS ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
            current.deobligations_recoveries_refund_pri_program_object_class_cpe - COALESCE(previous.deobligations_recoveries_refund_pri_program_object_class_cpe, 0) AS deobligations_recoveries_refund_pri_program_object_class_cpe
        FROM
            financial_accounts_by_program_activity_object_class AS current
            JOIN submission_attributes AS sub
            ON current.submission_id = sub.submission_id
            LEFT JOIN financial_accounts_by_program_activity_object_class AS previous
            ON current.treasury_account_id = previous.treasury_account_id
            AND current.object_class_id = previous.object_class_id
            AND current.program_activity_id = previous.program_activity_id
            AND previous.submission_id = sub.previous_submission_id
    """

    @classmethod
    def get_quarterly_numbers(cls, current_submission_id=None):
        """
        Return a RawQuerySet of quarterly financial numbers by TAS,
        object class, and program activity.
        """
        if current_submission_id is None:
            return FinancialAccountsByProgramActivityObjectClass.objects.raw(
                cls.QUARTERLY_SQL)
        else:
            sql = cls.QUARTERLY_SQL + ' WHERE current.submission_id = %s'
            return FinancialAccountsByProgramActivityObjectClass.objects.raw(
                sql, [current_submission_id])
