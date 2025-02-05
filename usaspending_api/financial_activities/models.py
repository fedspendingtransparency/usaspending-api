from django.db import models
from django_cte import CTEManager

from usaspending_api.references.models import ObjectClass, RefProgramActivity
from usaspending_api.common.models import DataSourceTrackedModel


class AbstractFinancialAccountsByProgramActivityObjectClass(DataSourceTrackedModel):
    financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    program_activity = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, null=True, db_index=True)
    submission = models.ForeignKey("submissions.SubmissionAttributes", models.CASCADE)
    object_class = models.ForeignKey(ObjectClass, models.DO_NOTHING, null=True, db_index=True)
    treasury_account = models.ForeignKey(
        "accounts.TreasuryAppropriationAccount", models.CASCADE, related_name="program_balances", null=True
    )
    prior_year_adjustment = models.TextField(blank=True, null=True)
    program_activity_reporting_key = models.TextField(
        blank=True, null=True, help_text="A unique identifier for a Program Activity"
    )
    disaster_emergency_fund = models.ForeignKey(
        "references.DisasterEmergencyFundCode",
        models.DO_NOTHING,
        blank=True,
        null=True,
        db_index=True,
        db_column="disaster_emergency_fund_code",
    )
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl480110_rein_undel_ord_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490100_delivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490100_delivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490110_rein_deliv_ord_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490200_delivered_orders_obligations_paid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_undelivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_undelivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_delivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_delivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlays_undelivered_orders_prepaid_total_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlays_undelivered_orders_prepaid_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlays_delivered_orders_paid_total_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlays_delivered_orders_paid_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_program_object_class_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_program_object_class_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_incurred_by_program_object_class_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    deobligations_recoveries_refund_pri_program_object_class_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    drv_obligations_incurred_by_program_object_class = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    drv_obligations_undelivered_orders_unpaid = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        abstract = True


class FinancialAccountsByProgramActivityObjectClassManager(models.Manager):
    def get_queryset(self):
        """
        Get only records from the last submission per TAS per fiscal year.
        """

        return (
            super(FinancialAccountsByProgramActivityObjectClassManager, self)
            .get_queryset()
            .filter(submission__is_final_balances_for_fy=True)
        )


class FinancialAccountsByProgramActivityObjectClass(AbstractFinancialAccountsByProgramActivityObjectClass):
    """Model corresponding to Agency File B"""

    class Meta:
        managed = True
        db_table = "financial_accounts_by_program_activity_object_class"

    objects = CTEManager()
    final_objects = FinancialAccountsByProgramActivityObjectClassManager()
