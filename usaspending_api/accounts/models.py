from django.db import models

# Table #3 - Treasury Appropriation Accounts.
class TreasuryAppropriationAccount(models.Model):
    treasury_account_identifier = models.AutoField(primary_key=True)
    tas_rendering_label = models.CharField(max_length=22)
    allocation_transfer_agency_id = models.CharField(max_length=3, blank=True, null=True)
    responsible_agency_id = models.CharField(max_length=3)
    beginning_period_of_availa = models.CharField(max_length=4, blank=True, null=True)
    ending_period_of_availabil = models.CharField(max_length=4, blank=True, null=True)
    availability_type_code = models.CharField(max_length=1, blank=True, null=True)
    main_account_code = models.CharField(max_length=4)
    sub_account_code = models.CharField(max_length=3)
    drv_approp_avail_pd_start_date = models.DateField(blank=True, null=True)
    drv_approp_avail_pd_end_date = models.DateField(blank=True, null=True)
    drv_approp_account_exp_status = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'treasury_appropriation_account'

# Table #4 - Appropriation Account Balances
class AppropriationAccountBalances(models.Model):
    appropriation_account_balances_id = models.AutoField(primary_key=True)
    treasury_account_identifier = models.ForeignKey('TreasuryAppropriationAccount', models.DO_NOTHING, db_column='treasury_account_identifier')
    submission_process = models.ForeignKey('SubmissionProcess', models.DO_NOTHING)
    budget_authority_unobligat_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    adjustments_to_unobligated_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    budget_authority_appropria_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    borrowing_authority_amount_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    contract_authority_amount_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    spending_authority_from_of_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    other_budgetary_resources_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    budget_authority_available_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    deobligations_recoveries_r_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    unobligated_balance_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    status_of_budgetary_resour_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    obligations_incurred_total_cpe = models.DecimalField(max_digits=21, decimal_places=0)
    drv_approp_avail_pd_start_date = models.DateField(blank=True, null=True)
    drv_approp_avail_pd_end_date = models.DateField(blank=True, null=True)
    drv_approp_account_exp_status = models.CharField(max_length=10, blank=True, null=True)
    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
    drv_obligations_unpaid_amount = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    drv_other_obligated_amount = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'appropriation_account_balances'
