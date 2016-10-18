from django.db import models
from usaspending_api.submissions.models import SubmissionAttributes


# Table #3 - Treasury Appropriation Accounts.
class TreasuryAppropriationAccount(models.Model):
    treasury_account_identifier = models.AutoField(primary_key=True)
    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
    allocation_transfer_agency_id = models.CharField(max_length=3, blank=True, null=True)
    agency_id = models.CharField(max_length=3)
    beginning_period_of_availability = models.CharField(max_length=4, blank=True, null=True)
    ending_period_of_availability = models.CharField(max_length=4, blank=True, null=True)
    availability_type_code = models.CharField(max_length=1, blank=True, null=True)
    main_account_code = models.CharField(max_length=4)
    sub_account_code = models.CharField(max_length=3)
    gwa_tas = models.CharField(max_length=21, blank=True, null=True)
    gwa_tas_name = models.CharField(max_length=240, blank=True, null=True)
    agency_aid = models.CharField(max_length=3, blank=True, null=True)
    agency_name = models.CharField(max_length=70, blank=True, null=True)
    admin_org = models.CharField(max_length=6, blank=True, null=True)
    admin_org_name = models.CharField(max_length=75, blank=True, null=True)
    fr_entity_type_code = models.CharField(max_length=4, blank=True, null=True)
    fr_entity_description = models.CharField(max_length=50, blank=True, null=True)
    fin_type_2 = models.CharField(max_length=6, blank=True, null=True)
    fin_type_2_description = models.CharField(max_length=50, blank=True, null=True)
    drv_appropriation_availability_period_start_date = models.DateField(blank=True, null=True)
    drv_appropriation_availability_period_end_date = models.DateField(blank=True, null=True)
    drv_appropriation_account_expired_status = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'treasury_appropriation_account'


# Table #4 - Appropriation Account Balances
class AppropriationAccountBalances(models.Model):
    appropriation_account_balances_id = models.AutoField(primary_key=True)
    treasury_account_identifier = models.ForeignKey('TreasuryAppropriationAccount', models.CASCADE, db_column='treasury_account_identifier')
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    budget_authority_unobligated_balance_brought_forward_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    adjustments_to_unobligated_balance_brought_forward_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    budget_authority_appropriated_amount_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    borrowing_authority_amount_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    contract_authority_amount_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    spending_authority_from_offsetting_collections_amount_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    other_budgetary_resources_amount_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    budget_authority_available_amount_total_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    deobligations_recoveries_refunds_by_tas_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    unobligated_balance_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    status_of_budgetary_resources_total_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    obligations_incurred_total_by_tas_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    drv_appropriation_availability_period_start_date = models.DateField(blank=True, null=True)
    drv_appropriation_availability_period_end_date = models.DateField(blank=True, null=True)
    drv_appropriation_account_expired_status = models.CharField(max_length=10, blank=True, null=True)
    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
    drv_obligations_unpaid_amount = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    drv_other_obligated_amount = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'appropriation_account_balances'
