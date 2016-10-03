from django.db import models
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.references.models import RefProgramActivity
from usaspending_api.references.models import RefObjectClassCode


class FinancialAccountsByProgramActivityObjectClass(models.Model):
    financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    program_activity_name = models.CharField(max_length=164)
    program_activity_code = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, db_column='program_activity_code')
    object_class = models.ForeignKey(RefObjectClassCode, models.DO_NOTHING, db_column='object_class')
    by_direct_reimbursable_fun = models.CharField(max_length=1)
    appropriation_account_balances = models.ForeignKey(AppropriationAccountBalances, models.DO_NOTHING)
    ussgl480100_undelivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl480100_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl483100_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl488100_upward_adjustm_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490100_delivered_orde_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490100_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl493100_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl498100_upward_adjustm_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl480200_undelivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl480200_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl483200_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl488200_upward_adjustm_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490200_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490800_authority_outl_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl490800_authority_outl_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl498200_upward_adjustm_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_undelivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_delivered_orde_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlays_undelivered_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlays_undelivered_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlays_delivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlays_delivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlay_amount_by_pro_fyb = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlay_amount_by_pro_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_incurred_by_pr_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl487100_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl497100_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl487200_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    ussgl497200_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    deobligations_recov_by_pro_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    drv_obli_inc_by_prog_obj_class = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    drv_obligations_undel_ord_unp = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_program_activity_object_class'
