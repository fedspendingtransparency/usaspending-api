from django.db import models
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.references.models import RefProgramActivity
from usaspending_api.references.models import RefObjectClassCode


class FinancialAccountsByProgramActivityObjectClass(models.Model):
    financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    program_activity_name = models.CharField(max_length=164)
    program_activity_code = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, db_column='program_activity_code', null=True)
    object_class = models.ForeignKey(RefObjectClassCode, models.DO_NOTHING, db_column='object_class', null=True)
    by_direct_reimbursable_funding_source = models.CharField(max_length=1)
    appropriation_account_balances = models.ForeignKey(AppropriationAccountBalances, models.CASCADE)
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490100_delivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490100_delivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490200_delivered_orders_obligations_paid_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_undelivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_undelivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_delivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_delivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlays_undelivered_orders_prepaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlays_undelivered_orders_prepaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlays_delivered_orders_paid_total_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlays_delivered_orders_paid_total_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlay_amount_by_program_object_class_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlay_amount_by_program_object_class_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_incurred_by_program_object_class_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    deobligations_recoveries_refund_pri_program_object_class_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    drv_obligations_incurred_by_program_object_class = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    drv_obligations_undelivered_orders_unpaid = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_program_activity_object_class'
