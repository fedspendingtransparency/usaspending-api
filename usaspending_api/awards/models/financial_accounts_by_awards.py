from django.db import models

from usaspending_api.common.models import DataSourceTrackedModel


class FinancialAccountsByAwards(DataSourceTrackedModel):
    financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    treasury_account = models.ForeignKey("accounts.TreasuryAppropriationAccount", models.CASCADE, null=True)
    submission = models.ForeignKey("submissions.SubmissionAttributes", models.CASCADE)
    award = models.ForeignKey("awards.Award", models.CASCADE, null=True, related_name="financial_set")
    program_activity = models.ForeignKey(
        "references.RefProgramActivity", models.DO_NOTHING, null=True, db_index=True
    )
    object_class = models.ForeignKey("references.ObjectClass", models.DO_NOTHING, null=True, db_index=True)
    piid = models.TextField(blank=True, null=True)
    parent_award_id = models.TextField(blank=True, null=True)
    fain = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True)
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl490100_delivered_orders_obligations_unpaid_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl490100_delivered_orders_obligations_unpaid_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl490200_delivered_orders_obligations_paid_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    obligations_undelivered_orders_unpaid_total_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    obligations_delivered_orders_unpaid_total_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    obligations_delivered_orders_unpaid_total_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    gross_outlays_undelivered_orders_prepaid_total_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    gross_outlays_undelivered_orders_prepaid_total_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    gross_outlays_delivered_orders_paid_total_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    gross_outlay_amount_by_award_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    gross_outlay_amount_by_award_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    obligations_incurred_total_by_award_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    deobligations_recoveries_refunds_of_prior_year_by_award_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    obligations_undelivered_orders_unpaid_total_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    gross_outlays_delivered_orders_paid_total_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    drv_award_id_field_type = models.TextField(blank=True, null=True)
    drv_obligations_incurred_total_by_award = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    transaction_obligated_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "financial_accounts_by_awards"
