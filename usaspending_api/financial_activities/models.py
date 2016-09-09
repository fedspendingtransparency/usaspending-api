from django.db import models
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.accounts.models import AppropriationAccountBalances

# Create your models here.

# AJ 09/07/2016...Added 5 tables below for financial_activities

class FinancialAccountsByProgramActivityObjectClass(models.Model):
    financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    program_activity_name = models.CharField(max_length=164)
    # Temporarily commenting out these reference table keys TODO: Get them in here!
    # program_activity_code = models.ForeignKey('RefProgramActivity', models.DO_NOTHING, db_column='program_activity_code')
    # object_class = models.ForeignKey('RefObjectClassCode', models.DO_NOTHING, db_column='object_class')
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
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_program_activity_object_class'

class FinancialAccountsByAwards(models.Model):
    financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    appropriation_account_balances = models.ForeignKey(AppropriationAccountBalances, models.DO_NOTHING)
    program_activity_name = models.CharField(max_length=164, blank=True, null=True)
    # Temporarily commenting out these reference table keys TODO: Get them in here!
    # program_activity_code = models.ForeignKey('RefProgramActivity', models.DO_NOTHING, db_column='program_activity_code', blank=True, null=True)
    # object_class = models.ForeignKey('RefObjectClassCode', models.DO_NOTHING, db_column='object_class')
    by_direct_reimbursable_fun = models.CharField(max_length=1, blank=True, null=True)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    award_type = models.CharField(max_length=30, blank=True, null=True)
    ussgl480100_undelivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl480100_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl483100_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl488100_upward_adjustm_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl490100_delivered_orde_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl490100_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl493100_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl498100_upward_adjustm_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl480200_undelivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl480200_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl483200_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl488200_upward_adjustm_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl490200_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl490800_authority_outl_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl490800_authority_outl_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl498200_upward_adjustm_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_delivered_orde_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlays_undelivered_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlays_undelivered_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlays_delivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlay_amount_by_awa_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlay_amount_by_awa_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_incurred_byawa_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl487100_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl497100_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl487200_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl497200_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    deobligations_recov_by_awa_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_undelivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlays_delivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    drv_award_id_field_type = models.CharField(max_length=10, blank=True, null=True)
    drv_oblig_incur_total_by_award = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards'

class FinancialAccountsByAwardsTransactionObligations(models.Model):
    financial_accounts_by_awards_transaction_obligations_id = models.AutoField(primary_key=True)
    financial_accounts_by_awards = models.ForeignKey(FinancialAccountsByAwards, models.DO_NOTHING)
    transaction_obligated_amou = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards_transaction_obligations'
