# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from __future__ import unicode_literals

from django.db import models


class AdtAgency(models.Model):
    adt_agency_id = models.AutoField(primary_key=True)
    agency_id = models.IntegerField(blank=True, null=True)
    location_id = models.IntegerField(blank=True, null=True)
    department_parent_id = models.IntegerField(blank=True, null=True)
    sub_tier_parent_id = models.IntegerField(blank=True, null=True)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    agency_code_aac = models.CharField(max_length=6, blank=True, null=True)
    agency_code_cgac = models.CharField(max_length=3, blank=True, null=True)
    agency_code_fpds = models.CharField(max_length=4, blank=True, null=True)
    valid_period_start = models.DateField(blank=True, null=True)
    valid_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_agency'


class AdtAppropriationAccountBalances(models.Model):
    adt_appropriation_account_balances_id = models.AutoField(primary_key=True)
    appropriation_account_balances_id = models.IntegerField(blank=True, null=True)
    treasury_account_identifier = models.IntegerField(blank=True, null=True)
    submission_process_id = models.IntegerField(blank=True, null=True)
    budget_authority_unobligat_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    adjustments_to_unobligated_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    budget_authority_appropria_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    borrowing_authority_amount_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    contract_authority_amount_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    spending_authority_from_of_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    other_budgetary_resources_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    budget_authority_available_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    deobligations_recoveries_r_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    unobligated_balance_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    status_of_budgetary_resour_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_incurred_total_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    drv_approp_avail_pd_start_date = models.DateField(blank=True, null=True)
    drv_approp_avail_pd_end_date = models.DateField(blank=True, null=True)
    drv_approp_account_exp_status = models.CharField(max_length=10, blank=True, null=True)
    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
    drv_obligations_unpaid_amount = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    drv_other_obligated_amount = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_appropriation_account_balances'


class AdtAward(models.Model):
    adt_award_id = models.AutoField(primary_key=True)
    award_id = models.IntegerField(blank=True, null=True)
    financial_assistance_award_id = models.IntegerField(blank=True, null=True)
    procurement_id = models.IntegerField(blank=True, null=True)
    awarding_agency_id = models.IntegerField(blank=True, null=True)
    place_of_performance_relationship_id = models.IntegerField(blank=True, null=True)
    funding_agency_id = models.IntegerField(blank=True, null=True)
    award_recipient_id = models.IntegerField(blank=True, null=True)
    award_type = models.CharField(max_length=30)
    award_description = models.CharField(max_length=4000, blank=True, null=True)
    period_of_performance_star = models.CharField(max_length=8, blank=True, null=True)
    period_of_performance_curr = models.CharField(max_length=8, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_award'


class AdtAwardAction(models.Model):
    adt_award_action_id = models.AutoField(primary_key=True)
    award_action_id = models.IntegerField(blank=True, null=True)
    award_id = models.IntegerField(blank=True, null=True)
    action_date = models.CharField(max_length=8, blank=True, null=True)
    action_type = models.CharField(max_length=1, blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    award_modification_amendme = models.CharField(max_length=50, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_award_action'


class AdtAwardSubawardRelationship(models.Model):
    adt_award_subaward_relationship_id = models.AutoField(primary_key=True)
    award_subaward_relationship_id = models.IntegerField(blank=True, null=True)
    sub_award_id = models.IntegerField(blank=True, null=True)
    award_id = models.IntegerField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_award_subaward_relationship'


class AdtError(models.Model):
    adt_error_id = models.AutoField(primary_key=True)
    error_id = models.IntegerField(blank=True, null=True)
    error_name = models.CharField(max_length=100, blank=True, null=True)
    error_type_id = models.IntegerField(blank=True, null=True)
    error_description = models.CharField(max_length=150, blank=True, null=True)
    job_id = models.IntegerField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_error'


class AdtErrorType(models.Model):
    adt_error_type_id = models.AutoField(primary_key=True)
    error_type_id = models.IntegerField(blank=True, null=True)
    error_type_name = models.CharField(max_length=100, blank=True, null=True)
    error_description = models.CharField(max_length=150, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_error_type'


class AdtFinancialAccountsByAwards(models.Model):
    adt_financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    financial_accounts_by_awards_id = models.IntegerField(blank=True, null=True)
    appropriation_account_balances_id = models.IntegerField(blank=True, null=True)
    program_activity_name = models.CharField(max_length=164, blank=True, null=True)
    program_activity_code = models.CharField(max_length=4, blank=True, null=True)
    object_class = models.CharField(max_length=4)
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
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_financial_accounts_by_awards'


class AdtFinancialAccountsByAwardsTransactionObligations(models.Model):
    adt_financial_accounts_by_awards_transaction_obligations_id = models.AutoField(primary_key=True)
    financial_accounts_by_awards_transaction_obligations_id = models.IntegerField(blank=True, null=True)
    transaction_obligated_amou = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)
    financial_accounts_by_awards_id = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'adt_financial_accounts_by_awards_transaction_obligations'


class AdtFinancialAccountsByProgramActivityObjectClass(models.Model):
    adt_financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    financial_accounts_by_program_activity_object_class_id = models.IntegerField(blank=True, null=True)
    appropriation_account_balances_id = models.IntegerField(blank=True, null=True)
    program_activity_name = models.CharField(max_length=164, blank=True, null=True)
    program_activity_code = models.CharField(max_length=4, blank=True, null=True)
    object_class = models.CharField(max_length=4, blank=True, null=True)
    by_direct_reimbursable_fun = models.CharField(max_length=1, blank=True, null=True)
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
    obligations_undelivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_undelivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_delivered_orde_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_delivered_orde_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlays_undelivered_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlays_undelivered_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlays_delivered_or_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlays_delivered_or_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlay_amount_by_pro_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlay_amount_by_pro_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_incurred_by_pr_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl487100_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl497100_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl487200_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    ussgl497200_downward_adjus_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    deobligations_recov_by_pro_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    drv_obli_inc_by_prog_obj_class = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    drv_obligations_undel_ord_unp = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_financial_accounts_by_program_activity_object_class'


class AdtFinancialAssistanceAward(models.Model):
    adt_financial_assistance_award_id = models.AutoField(primary_key=True)
    financial_assistance_award_id = models.IntegerField(blank=True, null=True)
    award_id = models.IntegerField(blank=True, null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    cfda_number = models.CharField(max_length=7, blank=True, null=True)
    cfda_title = models.CharField(max_length=100, blank=True, null=True)
    business_funds_indicator = models.CharField(max_length=3, blank=True, null=True)
    non_federal_funding_amount = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    total_funding_amount = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    federal_funding_amount = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    assistance_type = models.CharField(max_length=2, blank=True, null=True)
    record_type = models.IntegerField(blank=True, null=True)
    correction_late_delete_ind = models.CharField(max_length=1, blank=True, null=True)
    fiscal_year_and_quarter_co = models.CharField(max_length=5, blank=True, null=True)
    sai_number = models.CharField(max_length=50, blank=True, null=True)
    drv_awd_fin_assist_type_label = models.CharField(max_length=50, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_financial_assistance_award'


class AdtHighlyCompensatedOfficer(models.Model):
    adt_highly_compensated_officer_id = models.AutoField(primary_key=True)
    highly_compensated_officer_id = models.IntegerField(blank=True, null=True)
    legal_entity_id = models.IntegerField(blank=True, null=True)
    high_comp_officer_prefix = models.CharField(max_length=10, blank=True, null=True)
    high_comp_officer_first_na = models.CharField(max_length=35, blank=True, null=True)
    high_comp_officer_middle_i = models.CharField(max_length=1, blank=True, null=True)
    high_comp_officer_last_nam = models.CharField(max_length=35, blank=True, null=True)
    high_comp_officer_sufix = models.CharField(max_length=10, blank=True, null=True)
    high_comp_officer_amount = models.DecimalField(max_digits=38, decimal_places=0, blank=True, null=True)
    high_comp_officer_full_nam = models.CharField(max_length=255, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_highly_compensated_officer'


class AdtJob(models.Model):
    adt_job_id = models.AutoField(primary_key=True)
    job_id = models.IntegerField(blank=True, null=True)
    submission_id = models.IntegerField(blank=True, null=True)
    job_status_id = models.IntegerField(blank=True, null=True)
    job_type_id = models.IntegerField(blank=True, null=True)
    job_name = models.CharField(max_length=100, blank=True, null=True)
    job_description = models.CharField(max_length=500, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_job'


class AdtJobStatus(models.Model):
    adt_job_status_id = models.AutoField(primary_key=True)
    job_status_id = models.IntegerField(blank=True, null=True)
    job_status_name = models.CharField(max_length=100, blank=True, null=True)
    job_description = models.CharField(max_length=500, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_job_status'


class AdtJobType(models.Model):
    adt_job_type_id = models.AutoField(primary_key=True)
    job_type_id = models.IntegerField(blank=True, null=True)
    job_type_name = models.CharField(max_length=100, blank=True, null=True)
    job_type_description = models.CharField(max_length=500, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_job_type'


class AdtLegalEntity(models.Model):
    adt_legal_entity_id = models.AutoField(primary_key=True)
    legal_entity_id = models.IntegerField(blank=True, null=True)
    location_id = models.IntegerField(blank=True, null=True)
    ultimate_parent_legal_entity_id = models.IntegerField(blank=True, null=True)
    awardee_or_recipient_uniqu = models.CharField(max_length=9, blank=True, null=True)
    awardee_or_recipient_legal = models.CharField(max_length=120, blank=True, null=True)
    vendor_doing_as_business_n = models.CharField(max_length=400, blank=True, null=True)
    vendor_phone_number = models.CharField(max_length=30, blank=True, null=True)
    vendor_fax_number = models.CharField(max_length=30, blank=True, null=True)
    business_types = models.CharField(max_length=3, blank=True, null=True)
    limited_liability_corporat = models.CharField(max_length=1, blank=True, null=True)
    sole_proprietorship = models.CharField(max_length=1, blank=True, null=True)
    partnership_or_limited_lia = models.CharField(max_length=1, blank=True, null=True)
    subchapter_s_corporation = models.CharField(max_length=1, blank=True, null=True)
    foundation = models.CharField(max_length=1, blank=True, null=True)
    for_profit_organization = models.CharField(max_length=1, blank=True, null=True)
    nonprofit_organization = models.CharField(max_length=1, blank=True, null=True)
    corporate_entity_tax_exemp = models.CharField(max_length=1, blank=True, null=True)
    corporate_entity_not_tax_e = models.CharField(max_length=1, blank=True, null=True)
    other_not_for_profit_organ = models.CharField(max_length=1, blank=True, null=True)
    sam_exception = models.CharField(max_length=1, blank=True, null=True)
    city_local_government = models.CharField(max_length=1, blank=True, null=True)
    county_local_government = models.CharField(max_length=1, blank=True, null=True)
    inter_municipal_local_gove = models.CharField(max_length=1, blank=True, null=True)
    local_government_owned = models.CharField(max_length=1, blank=True, null=True)
    municipality_local_governm = models.CharField(max_length=1, blank=True, null=True)
    school_district_local_gove = models.CharField(max_length=1, blank=True, null=True)
    township_local_government = models.CharField(max_length=1, blank=True, null=True)
    us_state_government = models.CharField(max_length=1, blank=True, null=True)
    us_federal_government = models.CharField(max_length=1, blank=True, null=True)
    federal_agency = models.CharField(max_length=1, blank=True, null=True)
    federally_funded_research = models.CharField(max_length=1, blank=True, null=True)
    us_tribal_government = models.CharField(max_length=1, blank=True, null=True)
    foreign_government = models.CharField(max_length=1, blank=True, null=True)
    community_developed_corpor = models.CharField(max_length=1, blank=True, null=True)
    labor_surplus_area_firm = models.CharField(max_length=1, blank=True, null=True)
    small_agricultural_coopera = models.CharField(max_length=1, blank=True, null=True)
    international_organization = models.CharField(max_length=1, blank=True, null=True)
    us_government_entity = models.CharField(max_length=1, blank=True, null=True)
    emerging_small_business = models.CharField(max_length=1, blank=True, null=True)
    number_8a_program_participant = models.CharField(db_column='8a_program_participant', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    sba_certified_8_a_joint_ve = models.CharField(max_length=1, blank=True, null=True)
    dot_certified_disadvantage = models.CharField(max_length=1, blank=True, null=True)
    self_certified_small_disad = models.CharField(max_length=1, blank=True, null=True)
    historically_underutilized = models.CharField(max_length=1, blank=True, null=True)
    small_disadvantaged_busine = models.CharField(max_length=1, blank=True, null=True)
    the_ability_one_program = models.CharField(max_length=1, blank=True, null=True)
    historically_black_college = models.CharField(max_length=1, blank=True, null=True)
    number_1862_land_grant_college = models.CharField(db_column='1862_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    number_1890_land_grant_college = models.CharField(db_column='1890_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    number_1994_land_grant_college = models.CharField(db_column='1994_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    minority_institution = models.CharField(max_length=1, blank=True, null=True)
    private_university_or_coll = models.CharField(max_length=1, blank=True, null=True)
    school_of_forestry = models.CharField(max_length=1, blank=True, null=True)
    state_controlled_instituti = models.CharField(max_length=1, blank=True, null=True)
    tribal_college = models.CharField(max_length=1, blank=True, null=True)
    veterinary_college = models.CharField(max_length=1, blank=True, null=True)
    educational_institution = models.CharField(max_length=1, blank=True, null=True)
    alaskan_native_servicing_i = models.CharField(max_length=1, blank=True, null=True)
    community_development_corp = models.CharField(max_length=1, blank=True, null=True)
    native_hawaiian_servicing = models.CharField(max_length=1, blank=True, null=True)
    domestic_shelter = models.CharField(max_length=1, blank=True, null=True)
    manufacturer_of_goods = models.CharField(max_length=1, blank=True, null=True)
    hospital_flag = models.CharField(max_length=1, blank=True, null=True)
    veterinary_hospital = models.CharField(max_length=1, blank=True, null=True)
    hispanic_servicing_institu = models.CharField(max_length=1, blank=True, null=True)
    woman_owned_business = models.CharField(max_length=1, blank=True, null=True)
    minority_owned_business = models.CharField(max_length=1, blank=True, null=True)
    women_owned_small_business = models.CharField(max_length=1, blank=True, null=True)
    economically_disadvantaged = models.CharField(max_length=1, blank=True, null=True)
    joint_venture_women_owned = models.CharField(max_length=1, blank=True, null=True)
    joint_venture_economically = models.CharField(max_length=1, blank=True, null=True)
    veteran_owned_business = models.CharField(max_length=1, blank=True, null=True)
    service_disabled_veteran_o = models.CharField(max_length=1, blank=True, null=True)
    contracts = models.CharField(max_length=1, blank=True, null=True)
    grants = models.CharField(max_length=1, blank=True, null=True)
    receives_contracts_and_gra = models.CharField(max_length=1, blank=True, null=True)
    airport_authority = models.CharField(max_length=1, blank=True, null=True)
    council_of_governments = models.CharField(max_length=1, blank=True, null=True)
    housing_authorities_public = models.CharField(max_length=1, blank=True, null=True)
    interstate_entity = models.CharField(max_length=1, blank=True, null=True)
    planning_commission = models.CharField(max_length=1, blank=True, null=True)
    port_authority = models.CharField(max_length=1, blank=True, null=True)
    transit_authority = models.CharField(max_length=1, blank=True, null=True)
    foreign_owned_and_located = models.CharField(max_length=1, blank=True, null=True)
    american_indian_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    alaskan_native_owned_corpo = models.CharField(max_length=1, blank=True, null=True)
    indian_tribe_federally_rec = models.CharField(max_length=1, blank=True, null=True)
    native_hawaiian_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    tribally_owned_business = models.CharField(max_length=1, blank=True, null=True)
    asian_pacific_american_own = models.CharField(max_length=1, blank=True, null=True)
    black_american_owned_busin = models.CharField(max_length=1, blank=True, null=True)
    hispanic_american_owned_bu = models.CharField(max_length=1, blank=True, null=True)
    native_american_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    subcontinent_asian_asian_i = models.CharField(max_length=1, blank=True, null=True)
    other_minority_owned_busin = models.CharField(max_length=1, blank=True, null=True)
    us_local_government = models.CharField(max_length=1, blank=True, null=True)
    undefinitized_action = models.CharField(max_length=1, blank=True, null=True)
    domestic_or_foreign_entity = models.CharField(max_length=1, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_legal_entity'


class AdtLinkAward(models.Model):
    adt_link_award_id = models.AutoField(primary_key=True)
    financial_accounts_by_awards_id = models.IntegerField(blank=True, null=True)
    link_award_id = models.IntegerField(blank=True, null=True)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_link_award'


class AdtLocation(models.Model):
    adt_location_id = models.AutoField(primary_key=True)
    location_id = models.AutoField()
    location_country_code = models.CharField(max_length=3, blank=True, null=True)
    location_country_name = models.CharField(max_length=100, blank=True, null=True)
    location_state_code = models.CharField(max_length=2, blank=True, null=True)
    location_state_name = models.CharField(max_length=50, blank=True, null=True)
    location_city_code = models.CharField(max_length=5, blank=True, null=True)
    location_city_name = models.CharField(max_length=40, blank=True, null=True)
    location_county_code = models.CharField(max_length=3, blank=True, null=True)
    location_county_name = models.CharField(max_length=40, blank=True, null=True)
    location_address_line1 = models.CharField(max_length=150, blank=True, null=True)
    location_address_line2 = models.CharField(max_length=150, blank=True, null=True)
    location_address_line3 = models.CharField(max_length=55, blank=True, null=True)
    location_foreign_location_description = models.CharField(max_length=100, blank=True, null=True)
    location_zip4 = models.CharField(max_length=10, blank=True, null=True)
    location_congressional_code = models.CharField(max_length=2, blank=True, null=True)
    location_performance_code = models.CharField(max_length=9, blank=True, null=True)
    location_zip_last4 = models.CharField(max_length=4, blank=True, null=True)
    location_zip5 = models.CharField(max_length=5, blank=True, null=True)
    location_foreign_postal_code = models.CharField(max_length=50, blank=True, null=True)
    location_foreign_province = models.CharField(max_length=25, blank=True, null=True)
    location_foreign_city_name = models.CharField(max_length=40, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_location'


class AdtPermission(models.Model):
    adt_permission_id = models.AutoField(primary_key=True)
    permission_id = models.IntegerField(blank=True, null=True)
    user_id = models.IntegerField(blank=True, null=True)
    permission_name = models.CharField(max_length=50, blank=True, null=True)
    permission_description = models.CharField(max_length=150, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_permission'


class AdtPlaceOfPerformanceRelationship(models.Model):
    adt_place_of_performance_relationship_id = models.AutoField(primary_key=True)
    place_of_performance_relationship_id = models.IntegerField(blank=True, null=True)
    award_id = models.IntegerField(blank=True, null=True)
    sub_award_id = models.IntegerField(blank=True, null=True)
    location_id = models.IntegerField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_place_of_performance_relationship'


class AdtProcurement(models.Model):
    adt_procurement_id = models.AutoField(primary_key=True)
    procurement_id = models.IntegerField(blank=True, null=True)
    award_id = models.IntegerField(blank=True, null=True)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True)
    cost_or_pricing_data = models.CharField(max_length=1, blank=True, null=True)
    type_of_contract_pricing = models.CharField(max_length=2, blank=True, null=True)
    contract_award_type = models.CharField(max_length=1, blank=True, null=True)
    naics = models.CharField(max_length=6, blank=True, null=True)
    naics_description = models.CharField(max_length=150, blank=True, null=True)
    period_of_perf_potential_e = models.CharField(max_length=8, blank=True, null=True)
    ordering_period_end_date = models.CharField(max_length=8, blank=True, null=True)
    current_total_value_award = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    potential_total_value_awar = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    referenced_idv_agency_iden = models.CharField(max_length=4, blank=True, null=True)
    idv_type = models.CharField(max_length=1, blank=True, null=True)
    multiple_or_single_award_i = models.CharField(max_length=1, blank=True, null=True)
    type_of_idc = models.CharField(max_length=1, blank=True, null=True)
    a_76_fair_act_action = models.CharField(max_length=1, blank=True, null=True)
    dod_claimant_program_code = models.CharField(max_length=3, blank=True, null=True)
    clinger_cohen_act_planning = models.CharField(max_length=1, blank=True, null=True)
    commercial_item_acquisitio = models.CharField(max_length=1, blank=True, null=True)
    commercial_item_test_progr = models.CharField(max_length=1, blank=True, null=True)
    consolidated_contract = models.CharField(max_length=1, blank=True, null=True)
    contingency_humanitarian_o = models.CharField(max_length=1, blank=True, null=True)
    contract_bundling = models.CharField(max_length=1, blank=True, null=True)
    contract_financing = models.CharField(max_length=1, blank=True, null=True)
    contracting_officers_deter = models.CharField(max_length=1, blank=True, null=True)
    cost_accounting_standards = models.CharField(max_length=1, blank=True, null=True)
    country_of_product_or_serv = models.CharField(max_length=3, blank=True, null=True)
    davis_bacon_act = models.CharField(max_length=1, blank=True, null=True)
    evaluated_preference = models.CharField(max_length=6, blank=True, null=True)
    extent_competed = models.CharField(max_length=3, blank=True, null=True)
    fed_biz_opps = models.CharField(max_length=1, blank=True, null=True)
    foreign_funding = models.CharField(max_length=1, blank=True, null=True)
    government_furnished_equip = models.CharField(max_length=1, blank=True, null=True)
    information_technology_com = models.CharField(max_length=1, blank=True, null=True)
    interagency_contracting_au = models.CharField(max_length=1, blank=True, null=True)
    local_area_set_aside = models.CharField(max_length=1, blank=True, null=True)
    major_program = models.CharField(max_length=100, blank=True, null=True)
    purchase_card_as_payment_m = models.CharField(max_length=1, blank=True, null=True)
    multi_year_contract = models.CharField(max_length=1, blank=True, null=True)
    national_interest_action = models.CharField(max_length=4, blank=True, null=True)
    number_of_actions = models.CharField(max_length=3, blank=True, null=True)
    number_of_offers_received = models.CharField(max_length=3, blank=True, null=True)
    other_statutory_authority = models.CharField(max_length=1, blank=True, null=True)
    performance_based_service = models.CharField(max_length=1, blank=True, null=True)
    place_of_manufacture = models.CharField(max_length=1, blank=True, null=True)
    price_evaluation_adjustmen = models.CharField(max_length=2, blank=True, null=True)
    product_or_service_code = models.CharField(max_length=4, blank=True, null=True)
    program_acronym = models.CharField(max_length=25, blank=True, null=True)
    other_than_full_and_open_c = models.CharField(max_length=3, blank=True, null=True)
    recovered_materials_sustai = models.CharField(max_length=1, blank=True, null=True)
    research = models.CharField(max_length=3, blank=True, null=True)
    sea_transportation = models.CharField(max_length=1, blank=True, null=True)
    service_contract_act = models.CharField(max_length=1, blank=True, null=True)
    small_business_competitive = models.CharField(max_length=1, blank=True, null=True)
    solicitation_identifier = models.CharField(max_length=25, blank=True, null=True)
    solicitation_procedures = models.CharField(max_length=5, blank=True, null=True)
    fair_opportunity_limited_s = models.CharField(max_length=50, blank=True, null=True)
    subcontracting_plan = models.CharField(max_length=1, blank=True, null=True)
    program_system_or_equipmen = models.CharField(max_length=4, blank=True, null=True)
    type_set_aside = models.CharField(max_length=10, blank=True, null=True)
    epa_designated_product = models.CharField(max_length=1, blank=True, null=True)
    walsh_healey_act = models.CharField(max_length=1, blank=True, null=True)
    transaction_number = models.CharField(max_length=6, blank=True, null=True)
    referenced_idv_modificatio = models.CharField(max_length=1, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_procurement'


class AdtRefCgacCode(models.Model):
    adt_ref_cgac_code_id = models.AutoField(primary_key=True)
    agency_code_cgac = models.CharField(max_length=3, blank=True, null=True)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_ref_cgac_code'


class AdtRefCityCountyCode(models.Model):
    adt_ref_city_county_codes_id = models.AutoField(primary_key=True)
    city_county_code_id = models.AutoField()
    state_code = models.CharField(max_length=2, blank=True, null=True)
    city_name = models.CharField(max_length=50, blank=True, null=True)
    city_code = models.CharField(max_length=5, blank=True, null=True)
    county_code = models.CharField(max_length=3, blank=True, null=True)
    county_name = models.CharField(max_length=100, blank=True, null=True)
    type_of_area = models.CharField(max_length=20, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_ref_city_county_code'


class AdtRefCountryCode(models.Model):
    adt_ref_country_code_id = models.AutoField(primary_key=True)
    country_code = models.CharField(max_length=3, blank=True, null=True)
    country_name = models.CharField(max_length=100, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_ref_country_code'


class AdtRefDomainEnumerationType(models.Model):
    adt_ref_domain_enumeration_type_id = models.AutoField(primary_key=True)
    ref_domain_enumeration_type_id = models.IntegerField()
    domain_name = models.CharField(max_length=40, blank=True, null=True)
    enumeration_data_type = models.CharField(max_length=50, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    label = models.CharField(max_length=800, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_ref_domain_enumeration_type'


class AdtRefObjectClassCode(models.Model):
    adt_ref_object_class_code_id = models.AutoField(primary_key=True)
    object_class = models.CharField(max_length=4, blank=True, null=True)
    max_object_class_name = models.CharField(max_length=60, blank=True, null=True)
    label = models.CharField(max_length=100, blank=True, null=True)
    direct_or_reimbursable = models.CharField(max_length=25, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_ref_object_class_code'


class AdtRefProgramActivity(models.Model):
    adt_ref_program_activity_id = models.AutoField(primary_key=True)
    program_activity_code = models.CharField(max_length=4, blank=True, null=True)
    program_activity_name = models.CharField(max_length=164, blank=True, null=True)
    budget_year = models.CharField(max_length=4, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_ref_program_activity'


class AdtRefStateCode(models.Model):
    adt_ref_state_code_id = models.AutoField(primary_key=True)
    state_code = models.CharField(max_length=2, blank=True, null=True)
    state_name = models.CharField(max_length=50, blank=True, null=True)
    state_fips_code = models.CharField(max_length=2, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_ref_state_code'


class AdtRefSubtierAgencyOfficeCode(models.Model):
    adt_ref_subtier_agency_office_code_id = models.AutoField(primary_key=True)
    department_id = models.CharField(max_length=10, blank=True, null=True)
    department_name = models.CharField(max_length=150, blank=True, null=True)
    agency_code = models.CharField(max_length=10, blank=True, null=True)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    contracting_office_code = models.CharField(max_length=6, blank=True, null=True)
    contracting_office_name = models.CharField(max_length=150, blank=True, null=True)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    address_line_1 = models.CharField(max_length=150, blank=True, null=True)
    address_line_2 = models.CharField(max_length=150, blank=True, null=True)
    address_line_3 = models.CharField(max_length=55, blank=True, null=True)
    address_city = models.CharField(max_length=40, blank=True, null=True)
    address_state = models.CharField(max_length=2, blank=True, null=True)
    zip_code = models.CharField(max_length=10, blank=True, null=True)
    country_code = models.CharField(max_length=3, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_ref_subtier_agency_office_code'


class AdtSubAward(models.Model):
    adt_sub_award_id = models.AutoField(primary_key=True)
    sub_award_id = models.IntegerField(blank=True, null=True)
    award_id = models.IntegerField(blank=True, null=True)
    place_of_performance_relationship_id = models.IntegerField(blank=True, null=True)
    legal_entity_id = models.IntegerField(blank=True, null=True)
    sub_awardee_or_recipient_u = models.CharField(max_length=9, blank=True, null=True)
    sub_awardee_ultimate_pa_id = models.CharField(max_length=9, blank=True, null=True)
    sub_awardee_ultimate_paren = models.CharField(max_length=120, blank=True, null=True)
    subawardee_business_type = models.CharField(max_length=255, blank=True, null=True)
    sub_awardee_or_recipient_l = models.CharField(max_length=120, blank=True, null=True)
    subcontract_award_amount = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    cfda_number_and_title = models.CharField(max_length=255, blank=True, null=True)
    prime_award_report_id = models.CharField(max_length=40, blank=True, null=True)
    award_report_month = models.CharField(max_length=25, blank=True, null=True)
    award_report_year = models.CharField(max_length=4, blank=True, null=True)
    rec_model_question1 = models.CharField(max_length=1, blank=True, null=True)
    rec_model_question2 = models.CharField(max_length=1, blank=True, null=True)
    subaward_number = models.CharField(max_length=32, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_sub_award'


class AdtSubmissionAttributes(models.Model):
    adt_submission_attributes_id = models.AutoField(primary_key=True)
    submission_id = models.IntegerField()
    cgac_code = models.CharField(max_length=3, blank=True, null=True)
    submitting_agency = models.CharField(max_length=150, blank=True, null=True)
    submitter_name = models.CharField(max_length=200, blank=True, null=True)
    submission_modification = models.NullBooleanField()
    version_number = models.IntegerField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_submission_attributes'


class AdtSubmissionProcess(models.Model):
    adt_submission_process_id = models.AutoField(primary_key=True)
    submission_process_id = models.IntegerField(blank=True, null=True)
    submission_id = models.IntegerField(blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    file_a_submission = models.NullBooleanField()
    file_b_submission = models.NullBooleanField()
    file_c_submission = models.NullBooleanField()
    file_d1_submission = models.NullBooleanField()
    file_d2_submission = models.NullBooleanField()
    file_e_submission = models.NullBooleanField()
    file_f_submission = models.NullBooleanField()
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_submission_process'


class AdtTreasuryAppropriationAccount(models.Model):
    adt_treasury_appropriation_account_id = models.AutoField(primary_key=True)
    treasury_account_identifier = models.IntegerField(blank=True, null=True)
    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
    allocation_transfer_agency_id = models.CharField(max_length=3, blank=True, null=True)
    responsible_agency_id = models.CharField(max_length=3, blank=True, null=True)
    beginning_period_of_availa = models.CharField(max_length=4, blank=True, null=True)
    ending_period_of_availabil = models.CharField(max_length=4, blank=True, null=True)
    availability_type_code = models.CharField(max_length=1, blank=True, null=True)
    main_account_code = models.CharField(max_length=4, blank=True, null=True)
    sub_account_code = models.CharField(max_length=3, blank=True, null=True)
    drv_approp_avail_pd_start_date = models.DateField(blank=True, null=True)
    drv_approp_avail_pd_end_date = models.DateField(blank=True, null=True)
    drv_approp_account_exp_status = models.CharField(max_length=10, blank=True, null=True)
    dml = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_treasury_appropriation_account'


class AdtUsers(models.Model):
    adt_user_id = models.AutoField(primary_key=True)
    user_id = models.IntegerField(blank=True, null=True)
    title = models.CharField(max_length=20, blank=True, null=True)
    last_name = models.CharField(max_length=50, blank=True, null=True)
    first_name = models.CharField(max_length=50, blank=True, null=True)
    middle_initial = models.CharField(max_length=1, blank=True, null=True)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    cgac_code = models.CharField(max_length=3, blank=True, null=True)
    process = models.CharField(max_length=50, blank=True, null=True)
    password = models.CharField(max_length=50, blank=True, null=True)
    salt = models.CharField(max_length=50, blank=True, null=True)
    login_name = models.CharField(max_length=100, blank=True, null=True)
    last_login_date = models.DateTimeField(blank=True, null=True)
    user_active = models.NullBooleanField()
    incorrect_password_attempts = models.IntegerField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'adt_users'


# AJ 09/07/2016...moved foreign key to the top
# Moved to references app
class Agency(models.Model):
    agency_id = models.AutoField(primary_key=True)
    location = models.ForeignKey('Location', models.DO_NOTHING)
    agency_code_cgac = models.ForeignKey('RefCgacCode', models.DO_NOTHING, db_column='agency_code_cgac', blank=True, null=True)
    agency_code_aac = models.CharField(max_length=6, blank=True, null=True)
    agency_code_fpds = models.CharField(max_length=4, blank=True, null=True)
    department_parent_id = models.IntegerField(blank=True, null=True)
    sub_tier_parent_id = models.IntegerField(blank=True, null=True)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    valid_period_start = models.DateField(blank=True, null=True)
    valid_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'agency'


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

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'appropriation_account_balances'


class Award(models.Model):
    award_id = models.AutoField(primary_key=True)
    financial_assistance_award = models.ForeignKey('FinancialAssistanceAward', models.DO_NOTHING, blank=True, null=True)
    procurement = models.ForeignKey('Procurement', models.DO_NOTHING, blank=True, null=True)
    awarding_agency = models.ForeignKey(Agency, models.DO_NOTHING)
    place_of_performance_relationship = models.ForeignKey('PlaceOfPerformanceRelationship', models.DO_NOTHING)
    award_recipient = models.ForeignKey('LegalEntity', models.DO_NOTHING)
    funding_agency = models.ForeignKey(Agency, models.DO_NOTHING)
    award_type = models.CharField(max_length=30)
    award_description = models.CharField(max_length=4000, blank=True, null=True)
    period_of_performance_star = models.CharField(max_length=8, blank=True, null=True)
    period_of_performance_curr = models.CharField(max_length=8, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'award'


class AwardAction(models.Model):
    award_action_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.DO_NOTHING, blank=True, null=True)
    action_date = models.CharField(max_length=8)
    action_type = models.CharField(max_length=1, blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    award_modification_amendme = models.CharField(max_length=50, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'award_action'


class AwardSubawardRelationship(models.Model):
    award_subaward_relationship_id = models.AutoField(primary_key=True)
    sub_award = models.ForeignKey('SubAward', models.DO_NOTHING)
    award = models.ForeignKey(Award, models.DO_NOTHING)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'award_subaward_relationship'


# AJ 09/07/2016...this table is not needed
# class AwardsData(models.Model):
#    awards_data_id = models.AutoField()
#    username = models.TextField(blank=True, null=True)
#    agencyidentifier = models.TextField(db_column='AgencyIdentifier', blank=True, null=True)  # Field name made lowercase.
#    parentawardid = models.TextField(db_column='ParentAwardId', blank=True, null=True)  # Field name made lowercase.
#    uri = models.TextField(db_column='URI', blank=True, null=True)  # Field name made lowercase.
#    piid = models.TextField(db_column='PIID', blank=True, null=True)  # Field name made lowercase.
#    objectclass = models.TextField(db_column='ObjectClass', blank=True, null=True)  # Field name made lowercase.
#    transactionobligatedamount = models.TextField(db_column='TransactionObligatedAmount', blank=True, null=True)  # Field name made lowercase.
#    programactivitycode = models.TextField(db_column='ProgramActivityCode', blank=True, null=True)  # Field name made lowercase.
#    programactivityname = models.TextField(db_column='ProgramActivityName', blank=True, null=True)  # Field name made lowercase.
#    fain = models.TextField(db_column='FAIN', blank=True, null=True)  # Field name made lowercase.
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'awards_data'

# AJ 09/07/2016...this table is not needed
# class AwardsDataOld(models.Model):
#    vendorfaxnumber = models.TextField(db_column='VendorFaxNumber', blank=True, null=True)  # Field name made lowercase.
#    legalentitycityname = models.TextField(db_column='LegalEntityCityName', blank=True, null=True)  # Field name made lowercase.
#    legalentityzip4 = models.TextField(db_column='LegalEntityZip4', blank=True, null=True)  # Field name made lowercase.
#    parentawardid = models.TextField(db_column='ParentAwardId', blank=True, null=True)  # Field name made lowercase.
#    agencyidentifier = models.TextField(db_column='AgencyIdentifier', blank=True, null=True)  # Field name made lowercase.
#    legalentitystatecode = models.TextField(db_column='LegalEntityStateCode', blank=True, null=True)  # Field name made lowercase.
#    uri = models.TextField(db_column='URI', blank=True, null=True)  # Field name made lowercase.
#    currenttotalvalueofaward = models.TextField(db_column='CurrentTotalValueOfAward', blank=True, null=True)  # Field name made lowercase.
#    idv_type = models.TextField(db_column='IDV Type', blank=True, null=True)  # Field name made lowercase. Field renamed to remove unsuitable characters.
#    piid = models.TextField(db_column='PIID', blank=True, null=True)  # Field name made lowercase.
#    objectclass = models.TextField(db_column='ObjectClass', blank=True, null=True)  # Field name made lowercase.
#    northamericanindustrialclassificationsystemcode = models.TextField(db_column='NorthAmericanIndustrialClassificationSystemCode', blank=True, null=True)  # Field name made lowercase.
#    subaccountcode = models.TextField(db_column='SubAccountCode', blank=True, null=True)  # Field name made lowercase.
#    transactionobligatedamount = models.TextField(db_column='TransactionObligatedAmount', blank=True, null=True)  # Field name made lowercase.
#    legalentitycountrycode = models.TextField(db_column='LegalEntityCountryCode', blank=True, null=True)  # Field name made lowercase.
#    primaryplaceofperformancezip4 = models.TextField(db_column='PrimaryPlaceOfPerformanceZip4', blank=True, null=True)  # Field name made lowercase.
#    periodofperformancestartdate = models.TextField(db_column='PeriodOfPerformanceStartDate', blank=True, null=True)  # Field name made lowercase.
#    awarddescription = models.TextField(db_column='AwardDescription', blank=True, null=True)  # Field name made lowercase.
#    referencedidvagencyidentifier = models.TextField(db_column='ReferencedIDVAgencyIdentifier', blank=True, null=True)  # Field name made lowercase.
#    awardingofficecode = models.TextField(db_column='AwardingOfficeCode', blank=True, null=True)  # Field name made lowercase.
#    actiondate = models.TextField(db_column='ActionDate', blank=True, null=True)  # Field name made lowercase.
#    awardingsubtieragencycode = models.TextField(db_column='AwardingSubTierAgencyCode', blank=True, null=True)  # Field name made lowercase.
#    awardmodificationamendmentnumber = models.TextField(db_column='AwardModificationAmendmentNumber', blank=True, null=True)  # Field name made lowercase.
#    potentialtotalvalueofaward = models.TextField(db_column='PotentialTotalValueOfAward', blank=True, null=True)  # Field name made lowercase.
#    allocationtransferagencyidentifier = models.TextField(db_column='AllocationTransferAgencyIdentifier', blank=True, null=True)  # Field name made lowercase.
#    awardtype = models.TextField(db_column='AwardType', blank=True, null=True)  # Field name made lowercase.
#    programactivitycode = models.TextField(db_column='ProgramActivityCode', blank=True, null=True)  # Field name made lowercase.
#    primaryplaceofperformancecongressionaldistrict = models.TextField(db_column='PrimaryPlaceofPerformanceCongressionalDistrict', blank=True, null=True)  # Field name made lowercase.
#    periodofperformancecurrentenddate = models.TextField(db_column='PeriodOfPerformanceCurrentEndDate', blank=True, null=True)  # Field name made lowercase.
#    programactivityname = models.TextField(db_column='ProgramActivityName', blank=True, null=True)  # Field name made lowercase.
#    availabilitytypecode = models.TextField(db_column='AvailabilityTypeCode', blank=True, null=True)  # Field name made lowercase.
#    mainaccountcode = models.TextField(db_column='MainAccountCode', blank=True, null=True)  # Field name made lowercase.
#    legalentitycongressionaldistrict = models.TextField(db_column='LegalEntityCongressionalDistrict', blank=True, null=True)  # Field name made lowercase.
#    awardeeorrecipientuniqueidentifier = models.TextField(db_column='AwardeeOrRecipientUniqueIdentifier', blank=True, null=True)  # Field name made lowercase.
#    fain = models.TextField(db_column='FAIN', blank=True, null=True)  # Field name made lowercase.
#    awardeeorrecipientlegalentityname = models.TextField(db_column='AwardeeOrRecipientLegalEntityName', blank=True, null=True)  # Field name made lowercase.
#    fundingofficecode = models.TextField(db_column='FundingOfficeCode', blank=True, null=True)  # Field name made lowercase.
#    vendorphonenumber = models.TextField(db_column='VendorPhoneNumber', blank=True, null=True)  # Field name made lowercase.
#    federalactionobligation = models.TextField(db_column='FederalActionObligation', blank=True, null=True)  # Field name made lowercase.
#    periodofperformancepotentialenddate = models.TextField(db_column='PeriodOfPerformancePotentialEndDate', blank=True, null=True)  # Field name made lowercase.
#    fundingsubtieragencycode = models.TextField(db_column='FundingSubTierAgencyCode', blank=True, null=True)  # Field name made lowercase.
#    endingperiodofavailability = models.TextField(db_column='EndingPeriodOfAvailability', blank=True, null=True)  # Field name made lowercase.
#    vendordoingasbusinessname = models.TextField(db_column='VendorDoingAsBusinessName', blank=True, null=True)  # Field name made lowercase.
#    beginningperiodofavailability = models.TextField(db_column='BeginningPeriodOfAvailability', blank=True, null=True)  # Field name made lowercase.
#    legalentityaddressline2 = models.TextField(db_column='LegalEntityAddressLine2', blank=True, null=True)  # Field name made lowercase.
#    legalentityaddressline1 = models.TextField(db_column='LegalEntityAddressLine1', blank=True, null=True)  # Field name made lowercase.
#    vendoraddressline3 = models.TextField(db_column='VendorAddressLine3', blank=True, null=True)  # Field name made lowercase.
#    typeofidc = models.TextField(db_column='TypeofIDC', blank=True, null=True)  # Field name made lowercase.
#    multipleorsingleawardidv = models.TextField(db_column='MultipleorSingleAwardIDV', blank=True, null=True)  # Field name made lowercase.
#    username = models.TextField(blank=True, null=True)
#    awards_data_old_id = models.AutoField()
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'awards_data_old'

# AJ 09/07/2016...moved foreign key to the top
class Error(models.Model):
    error_id = models.AutoField(primary_key=True)
    error_type = models.ForeignKey('ErrorType', models.DO_NOTHING, blank=True, null=True)
    job = models.ForeignKey('Job', models.DO_NOTHING, blank=True, null=True)
    error_name = models.CharField(max_length=100, blank=True, null=True)
    error_description = models.CharField(max_length=150, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'error'


class ErrorType(models.Model):
    error_type_id = models.AutoField(primary_key=True)
    error_type_name = models.CharField(max_length=100, blank=True, null=True)
    error_description = models.CharField(max_length=150, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'error_type'

# AJ 09/07/2016...these Fct tables are not needed
# class FctContextAspectValueSet(models.Model):
#    context_aspect_value_set_id = models.AutoField()
#    context_aspect_concept_id = models.IntegerField(blank=True, null=True)
#    context_value_concept_id = models.IntegerField(blank=True, null=True)
#    context_aspect_value = models.TextField(blank=True, null=True)
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'fct_context_aspect_value_set'


# class FctElement(models.Model):
#    element_id = models.AutoField()
#    element_name = models.TextField(blank=True, null=True)
#    element_label = models.TextField(blank=True, null=True)
#    data_type_complex = models.TextField(blank=True, null=True)
#    data_type_simple = models.TextField(blank=True, null=True)
#    period_type = models.TextField(blank=True, null=True)
#    abstract = models.TextField(blank=True, null=True)
#    value_source_type = models.TextField(blank=True, null=True)
#    nillable = models.TextField(blank=True, null=True)
#    required_context = models.TextField(blank=True, null=True)
#    context_aspect_set_id = models.IntegerField(blank=True, null=True)
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'fct_element'
#
#
# class FctElementCalcRelationship(models.Model):
#    calc_relationship_id = models.AutoField()
#    parent_element_id = models.IntegerField(blank=True, null=True)
#    child_element_id = models.IntegerField(blank=True, null=True)
#    weight = models.TextField(blank=True, null=True)
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'fct_element_calc_relationship'
#
#
# class FctElementCompositionRelationship(models.Model):
#    composition_relationship_id = models.AutoField()
#    parent_element_id = models.IntegerField(blank=True, null=True)
#    child_element_id = models.IntegerField(blank=True, null=True)
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'fct_element_composition_relationship'
#
#
# class FctFact(models.Model):
#    fact_id = models.AutoField()
#    element_id = models.IntegerField(blank=True, null=True)
#    package_id = models.IntegerField(blank=True, null=True)
#    entity_owner = models.TextField(blank=True, null=True)
#    period_start = models.DateField(blank=True, null=True)
#    period_end = models.DateField(blank=True, null=True)
#    unit = models.TextField(blank=True, null=True)
#    lang = models.TextField(blank=True, null=True)
#    value = models.TextField(blank=True, null=True)
#    normalized_string_value = models.TextField(blank=True, null=True)
#    is_nil = models.NullBooleanField()
#    context_aspect_value_set_id = models.IntegerField(blank=True, null=True)
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'fct_fact'
#
#
# class FctOwner(models.Model):
#    owner_id = models.IntegerField(blank=True, null=True)
#    entity_owner = models.TextField(blank=True, null=True)
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'fct_owner'
#
#
# class FctPackage(models.Model):
#    package_id = models.AutoField()
#    submission_id = models.IntegerField(blank=True, null=True)
#    package_name = models.TextField(blank=True, null=True)
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'fct_package'
#
#
# class FctSubmission(models.Model):
#    submission_id = models.AutoField()
#    owner_id = models.IntegerField(blank=True, null=True)
#    accept_date = models.DateTimeField(blank=True, null=True)
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'fct_submission'

# AJ 09/07/2016...this tables is not needed
# class FinancialAccounts(models.Model):
#    financial_accounts_id = models.AutoField()
#    username = models.TextField(blank=True, null=True)
#    agencyidentifier = models.TextField(db_column='AgencyIdentifier', blank=True, null=True)  # Field name made lowercase.
#    mainaccountcode = models.TextField(db_column='MainAccountCode', blank=True, null=True)  # Field name made lowercase.
#    obligationsincurredtotalbytas_cpe = models.TextField(db_column='ObligationsIncurredTotalByTAS_CPE', blank=True, null=True)  # Field name made lowercase.
#    grossoutlayamountbytas_cpe = models.TextField(db_column='GrossOutlayAmountByTAS_CPE', blank=True, null=True)  # Field name made lowercase.
#    deobligationsrecoveriesrefundsbytas_cpe = models.TextField(db_column='DeobligationsRecoveriesRefundsByTAS_CPE', blank=True, null=True)  # Field name made lowercase.
#    borrowingauthorityamounttotal_cpe = models.TextField(db_column='BorrowingAuthorityAmountTotal_CPE', blank=True, null=True)  # Field name made lowercase.
#    statusofbudgetaryresourcestotal_cpe = models.TextField(db_column='StatusOfBudgetaryResourcesTotal_CPE', blank=True, null=True)  # Field name made lowercase.
#    unobligatedbalance_cpe = models.TextField(db_column='UnobligatedBalance_CPE', blank=True, null=True)  # Field name made lowercase.
#    beginningperiodofavailability = models.TextField(db_column='BeginningPeriodOfAvailability', blank=True, null=True)  # Field name made lowercase.
#    create_date = models.DateTimeField(db_column='create date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#    update_date = models.DateTimeField(db_column='update date', blank=True, null=True)  # Field renamed to remove unsuitable characters.
#
#    class Meta:
#        managed = False
#        db_table = 'financial_accounts'

# AJ...program_activity_code & TAS have to be a combo key
class FinancialAccountsByAwards(models.Model):
    financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    appropriation_account_balances = models.ForeignKey(AppropriationAccountBalances, models.DO_NOTHING)
    program_activity_name = models.CharField(max_length=164, blank=True, null=True)
    program_activity_code = models.ForeignKey('RefProgramActivity', models.DO_NOTHING, db_column='program_activity_code', blank=True, null=True)
    object_class = models.ForeignKey('RefObjectClassCode', models.DO_NOTHING, db_column='object_class')
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

# AJ 09/07/2016...changed managed to TRUE
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

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards_transaction_obligations'


# AJ...program_activity_code & TAS have to be a combo key
class FinancialAccountsByProgramActivityObjectClass(models.Model):
    financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    program_activity_name = models.CharField(max_length=164)
    program_activity_code = models.ForeignKey('RefProgramActivity', models.DO_NOTHING, db_column='program_activity_code')
    object_class = models.ForeignKey('RefObjectClassCode', models.DO_NOTHING, db_column='object_class')
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

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'financial_accounts_by_program_activity_object_class'

# Moved to awards app
class FinancialAssistanceAward(models.Model):
    financial_assistance_award_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.DO_NOTHING)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    cfda_number = models.CharField(max_length=7, blank=True, null=True)
    cfda_title = models.CharField(max_length=100, blank=True, null=True)
    business_funds_indicator = models.CharField(max_length=3)
    non_federal_funding_amount = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    total_funding_amount = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    federal_funding_amount = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    assistance_type = models.CharField(max_length=2)
    record_type = models.IntegerField()
    correction_late_delete_ind = models.CharField(max_length=1, blank=True, null=True)
    fiscal_year_and_quarter_co = models.CharField(max_length=5, blank=True, null=True)
    sai_number = models.CharField(max_length=50, blank=True, null=True)
    drv_awd_fin_assist_type_label = models.CharField(max_length=50, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'financial_assistance_award'


class HighlyCompensatedOfficer(models.Model):
    highly_compensated_officer_id = models.AutoField(primary_key=True)
    legal_entity = models.ForeignKey('LegalEntity', models.DO_NOTHING)
    high_comp_officer_prefix = models.CharField(max_length=10, blank=True, null=True)
    high_comp_officer_first_na = models.CharField(max_length=35, blank=True, null=True)
    high_comp_officer_middle_i = models.CharField(max_length=1, blank=True, null=True)
    high_comp_officer_last_nam = models.CharField(max_length=35, blank=True, null=True)
    high_comp_officer_sufix = models.CharField(max_length=10, blank=True, null=True)
    high_comp_officer_amount = models.DecimalField(max_digits=38, decimal_places=0, blank=True, null=True)
    high_comp_officer_full_nam = models.CharField(max_length=255, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'highly_compensated_officer'


class Job(models.Model):
    job_id = models.AutoField(primary_key=True)
    submission = models.ForeignKey('SubmissionAttributes', models.DO_NOTHING, blank=True, null=True)
    job_status = models.ForeignKey('JobStatus', models.DO_NOTHING, blank=True, null=True)
    job_type = models.ForeignKey('JobType', models.DO_NOTHING, blank=True, null=True)
    job_name = models.CharField(max_length=100, blank=True, null=True)
    job_description = models.CharField(max_length=500, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'job'


class JobStatus(models.Model):
    job_status_id = models.AutoField(primary_key=True)
    job_status_name = models.CharField(max_length=100, blank=True, null=True)
    job_description = models.CharField(max_length=500, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'job_status'


class JobType(models.Model):
    job_type_id = models.AutoField(primary_key=True)
    job_type_name = models.CharField(max_length=100, blank=True, null=True)
    job_type_description = models.CharField(max_length=500, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'job_type'

# Moved to reference app, for now
class LegalEntity(models.Model):
    legal_entity_id = models.AutoField(primary_key=True)
    location = models.ForeignKey('Location', models.DO_NOTHING)
    ultimate_parent_legal_entity_id = models.IntegerField()
    awardee_or_recipient_legal = models.CharField(max_length=120, blank=True, null=True)
    vendor_doing_as_business_n = models.CharField(max_length=400, blank=True, null=True)
    vendor_phone_number = models.CharField(max_length=30, blank=True, null=True)
    vendor_fax_number = models.CharField(max_length=30, blank=True, null=True)
    business_types = models.CharField(max_length=3, blank=True, null=True)
    awardee_or_recipient_uniqu = models.CharField(max_length=9, blank=True, null=True)
    limited_liability_corporat = models.CharField(max_length=1, blank=True, null=True)
    sole_proprietorship = models.CharField(max_length=1, blank=True, null=True)
    partnership_or_limited_lia = models.CharField(max_length=1, blank=True, null=True)
    subchapter_s_corporation = models.CharField(max_length=1, blank=True, null=True)
    foundation = models.CharField(max_length=1, blank=True, null=True)
    for_profit_organization = models.CharField(max_length=1, blank=True, null=True)
    nonprofit_organization = models.CharField(max_length=1, blank=True, null=True)
    corporate_entity_tax_exemp = models.CharField(max_length=1, blank=True, null=True)
    corporate_entity_not_tax_e = models.CharField(max_length=1, blank=True, null=True)
    other_not_for_profit_organ = models.CharField(max_length=1, blank=True, null=True)
    sam_exception = models.CharField(max_length=1, blank=True, null=True)
    city_local_government = models.CharField(max_length=1, blank=True, null=True)
    county_local_government = models.CharField(max_length=1, blank=True, null=True)
    inter_municipal_local_gove = models.CharField(max_length=1, blank=True, null=True)
    local_government_owned = models.CharField(max_length=1, blank=True, null=True)
    municipality_local_governm = models.CharField(max_length=1, blank=True, null=True)
    school_district_local_gove = models.CharField(max_length=1, blank=True, null=True)
    township_local_government = models.CharField(max_length=1, blank=True, null=True)
    us_state_government = models.CharField(max_length=1, blank=True, null=True)
    us_federal_government = models.CharField(max_length=1, blank=True, null=True)
    federal_agency = models.CharField(max_length=1, blank=True, null=True)
    federally_funded_research = models.CharField(max_length=1, blank=True, null=True)
    us_tribal_government = models.CharField(max_length=1, blank=True, null=True)
    foreign_government = models.CharField(max_length=1, blank=True, null=True)
    community_developed_corpor = models.CharField(max_length=1, blank=True, null=True)
    labor_surplus_area_firm = models.CharField(max_length=1, blank=True, null=True)
    small_agricultural_coopera = models.CharField(max_length=1, blank=True, null=True)
    international_organization = models.CharField(max_length=1, blank=True, null=True)
    us_government_entity = models.CharField(max_length=1, blank=True, null=True)
    emerging_small_business = models.CharField(max_length=1, blank=True, null=True)
    number_8a_program_participant = models.CharField(db_column='8a_program_participant', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    sba_certified_8_a_joint_ve = models.CharField(max_length=1, blank=True, null=True)
    dot_certified_disadvantage = models.CharField(max_length=1, blank=True, null=True)
    self_certified_small_disad = models.CharField(max_length=1, blank=True, null=True)
    historically_underutilized = models.CharField(max_length=1, blank=True, null=True)
    small_disadvantaged_busine = models.CharField(max_length=1, blank=True, null=True)
    the_ability_one_program = models.CharField(max_length=1, blank=True, null=True)
    historically_black_college = models.CharField(max_length=1, blank=True, null=True)
    number_1862_land_grant_college = models.CharField(db_column='1862_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    number_1890_land_grant_college = models.CharField(db_column='1890_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    number_1994_land_grant_college = models.CharField(db_column='1994_land_grant_college', max_length=1, blank=True, null=True)  # Field renamed because it wasn't a valid Python identifier.
    minority_institution = models.CharField(max_length=1, blank=True, null=True)
    private_university_or_coll = models.CharField(max_length=1, blank=True, null=True)
    school_of_forestry = models.CharField(max_length=1, blank=True, null=True)
    state_controlled_instituti = models.CharField(max_length=1, blank=True, null=True)
    tribal_college = models.CharField(max_length=1, blank=True, null=True)
    veterinary_college = models.CharField(max_length=1, blank=True, null=True)
    educational_institution = models.CharField(max_length=1, blank=True, null=True)
    alaskan_native_servicing_i = models.CharField(max_length=1, blank=True, null=True)
    community_development_corp = models.CharField(max_length=1, blank=True, null=True)
    native_hawaiian_servicing = models.CharField(max_length=1, blank=True, null=True)
    domestic_shelter = models.CharField(max_length=1, blank=True, null=True)
    manufacturer_of_goods = models.CharField(max_length=1, blank=True, null=True)
    hospital_flag = models.CharField(max_length=1, blank=True, null=True)
    veterinary_hospital = models.CharField(max_length=1, blank=True, null=True)
    hispanic_servicing_institu = models.CharField(max_length=1, blank=True, null=True)
    woman_owned_business = models.CharField(max_length=1, blank=True, null=True)
    minority_owned_business = models.CharField(max_length=1, blank=True, null=True)
    women_owned_small_business = models.CharField(max_length=1, blank=True, null=True)
    economically_disadvantaged = models.CharField(max_length=1, blank=True, null=True)
    joint_venture_women_owned = models.CharField(max_length=1, blank=True, null=True)
    joint_venture_economically = models.CharField(max_length=1, blank=True, null=True)
    veteran_owned_business = models.CharField(max_length=1, blank=True, null=True)
    service_disabled_veteran_o = models.CharField(max_length=1, blank=True, null=True)
    contracts = models.CharField(max_length=1, blank=True, null=True)
    grants = models.CharField(max_length=1, blank=True, null=True)
    receives_contracts_and_gra = models.CharField(max_length=1, blank=True, null=True)
    airport_authority = models.CharField(max_length=1, blank=True, null=True)
    council_of_governments = models.CharField(max_length=1, blank=True, null=True)
    housing_authorities_public = models.CharField(max_length=1, blank=True, null=True)
    interstate_entity = models.CharField(max_length=1, blank=True, null=True)
    planning_commission = models.CharField(max_length=1, blank=True, null=True)
    port_authority = models.CharField(max_length=1, blank=True, null=True)
    transit_authority = models.CharField(max_length=1, blank=True, null=True)
    foreign_owned_and_located = models.CharField(max_length=1, blank=True, null=True)
    american_indian_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    alaskan_native_owned_corpo = models.CharField(max_length=1, blank=True, null=True)
    indian_tribe_federally_rec = models.CharField(max_length=1, blank=True, null=True)
    native_hawaiian_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    tribally_owned_business = models.CharField(max_length=1, blank=True, null=True)
    asian_pacific_american_own = models.CharField(max_length=1, blank=True, null=True)
    black_american_owned_busin = models.CharField(max_length=1, blank=True, null=True)
    hispanic_american_owned_bu = models.CharField(max_length=1, blank=True, null=True)
    native_american_owned_busi = models.CharField(max_length=1, blank=True, null=True)
    subcontinent_asian_asian_i = models.CharField(max_length=1, blank=True, null=True)
    other_minority_owned_busin = models.CharField(max_length=1, blank=True, null=True)
    us_local_government = models.CharField(max_length=1, blank=True, null=True)
    undefinitized_action = models.CharField(max_length=1, blank=True, null=True)
    domestic_or_foreign_entity = models.CharField(max_length=1, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'legal_entity'


class LinkAward(models.Model):
    link_award_id = models.AutoField(primary_key=True)
    financial_accounts_by_awards = models.ForeignKey(FinancialAccountsByAwards, models.DO_NOTHING)
    award = models.ForeignKey(Award, models.DO_NOTHING)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'link_award'

# moved to reference app`
class Location(models.Model):
    location_id = models.AutoField(primary_key=True)
    location_country_code = models.ForeignKey('RefCountryCode', models.DO_NOTHING, db_column='location_country_code', blank=True, null=True)
    location_country_name = models.CharField(max_length=100, blank=True, null=True)
    location_state_code = models.CharField(max_length=2, blank=True, null=True)
    location_state_name = models.CharField(max_length=50, blank=True, null=True)
    location_state_text = models.CharField(max_length=100, blank=True, null=True)
    location_city_name = models.CharField(max_length=40, blank=True, null=True)
    location_city_code = models.CharField(max_length=5, blank=True, null=True)
    location_county_name = models.CharField(max_length=40, blank=True, null=True)
    location_county_code = models.CharField(max_length=3, blank=True, null=True)
    location_address_line1 = models.CharField(max_length=150, blank=True, null=True)
    location_address_line2 = models.CharField(max_length=150, blank=True, null=True)
    location_address_line3 = models.CharField(max_length=55, blank=True, null=True)
    location_foreign_location_description = models.CharField(max_length=100, blank=True, null=True)
    location_zip4 = models.CharField(max_length=10, blank=True, null=True)
    location_congressional_code = models.CharField(max_length=2, blank=True, null=True)
    location_performance_code = models.CharField(max_length=9, blank=True, null=True)
    location_zip_last4 = models.CharField(max_length=4, blank=True, null=True)
    location_zip5 = models.CharField(max_length=5, blank=True, null=True)
    location_foreign_postal_code = models.CharField(max_length=50, blank=True, null=True)
    location_foreign_province = models.CharField(max_length=25, blank=True, null=True)
    location_foreign_city_name = models.CharField(max_length=40, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'location'


class Permission(models.Model):
    permission_id = models.AutoField(primary_key=True)
    user = models.ForeignKey('Users', models.DO_NOTHING, blank=True, null=True)
    permission_name = models.CharField(max_length=50, blank=True, null=True)
    permission_description = models.CharField(max_length=150, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'permission'


class PlaceOfPerformanceRelationship(models.Model):
    place_of_performance_relationship_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.DO_NOTHING)
    sub_award = models.ForeignKey('SubAward', models.DO_NOTHING)
    location = models.ForeignKey(Location, models.DO_NOTHING)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'place_of_performance_relationship'

# moved to awards app
    class Procurement(models.Model):
    procurement_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.DO_NOTHING)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True)
    cost_or_pricing_data = models.CharField(max_length=1, blank=True, null=True)
    type_of_contract_pricing = models.CharField(max_length=2, blank=True, null=True)
    contract_award_type = models.CharField(max_length=1, blank=True, null=True)
    naics = models.CharField(max_length=6, blank=True, null=True)
    naics_description = models.CharField(max_length=150, blank=True, null=True)
    period_of_perf_potential_e = models.CharField(max_length=8, blank=True, null=True)
    ordering_period_end_date = models.CharField(max_length=8, blank=True, null=True)
    current_total_value_award = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    potential_total_value_awar = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    referenced_idv_agency_iden = models.CharField(max_length=4, blank=True, null=True)
    idv_type = models.CharField(max_length=1, blank=True, null=True)
    multiple_or_single_award_i = models.CharField(max_length=1, blank=True, null=True)
    type_of_idc = models.CharField(max_length=1, blank=True, null=True)
    a_76_fair_act_action = models.CharField(max_length=1, blank=True, null=True)
    dod_claimant_program_code = models.CharField(max_length=3, blank=True, null=True)
    clinger_cohen_act_planning = models.CharField(max_length=1, blank=True, null=True)
    commercial_item_acquisitio = models.CharField(max_length=1, blank=True, null=True)
    commercial_item_test_progr = models.CharField(max_length=1, blank=True, null=True)
    consolidated_contract = models.CharField(max_length=1, blank=True, null=True)
    contingency_humanitarian_o = models.CharField(max_length=1, blank=True, null=True)
    contract_bundling = models.CharField(max_length=1, blank=True, null=True)
    contract_financing = models.CharField(max_length=1, blank=True, null=True)
    contracting_officers_deter = models.CharField(max_length=1, blank=True, null=True)
    cost_accounting_standards = models.CharField(max_length=1, blank=True, null=True)
    country_of_product_or_serv = models.CharField(max_length=3, blank=True, null=True)
    davis_bacon_act = models.CharField(max_length=1, blank=True, null=True)
    evaluated_preference = models.CharField(max_length=6, blank=True, null=True)
    extent_competed = models.CharField(max_length=3, blank=True, null=True)
    fed_biz_opps = models.CharField(max_length=1, blank=True, null=True)
    foreign_funding = models.CharField(max_length=1, blank=True, null=True)
    government_furnished_equip = models.CharField(max_length=1, blank=True, null=True)
    information_technology_com = models.CharField(max_length=1, blank=True, null=True)
    interagency_contracting_au = models.CharField(max_length=1, blank=True, null=True)
    local_area_set_aside = models.CharField(max_length=1, blank=True, null=True)
    major_program = models.CharField(max_length=100, blank=True, null=True)
    purchase_card_as_payment_m = models.CharField(max_length=1, blank=True, null=True)
    multi_year_contract = models.CharField(max_length=1, blank=True, null=True)
    national_interest_action = models.CharField(max_length=4, blank=True, null=True)
    number_of_actions = models.CharField(max_length=3, blank=True, null=True)
    number_of_offers_received = models.CharField(max_length=3, blank=True, null=True)
    other_statutory_authority = models.CharField(max_length=1, blank=True, null=True)
    performance_based_service = models.CharField(max_length=1, blank=True, null=True)
    place_of_manufacture = models.CharField(max_length=1, blank=True, null=True)
    price_evaluation_adjustmen = models.CharField(max_length=2, blank=True, null=True)
    product_or_service_code = models.CharField(max_length=4, blank=True, null=True)
    program_acronym = models.CharField(max_length=25, blank=True, null=True)
    other_than_full_and_open_c = models.CharField(max_length=3, blank=True, null=True)
    recovered_materials_sustai = models.CharField(max_length=1, blank=True, null=True)
    research = models.CharField(max_length=3, blank=True, null=True)
    sea_transportation = models.CharField(max_length=1, blank=True, null=True)
    service_contract_act = models.CharField(max_length=1, blank=True, null=True)
    small_business_competitive = models.CharField(max_length=1, blank=True, null=True)
    solicitation_identifier = models.CharField(max_length=25, blank=True, null=True)
    solicitation_procedures = models.CharField(max_length=5, blank=True, null=True)
    fair_opportunity_limited_s = models.CharField(max_length=50, blank=True, null=True)
    subcontracting_plan = models.CharField(max_length=1, blank=True, null=True)
    program_system_or_equipmen = models.CharField(max_length=4, blank=True, null=True)
    type_set_aside = models.CharField(max_length=10, blank=True, null=True)
    epa_designated_product = models.CharField(max_length=1, blank=True, null=True)
    walsh_healey_act = models.CharField(max_length=1, blank=True, null=True)
    transaction_number = models.CharField(max_length=6, blank=True, null=True)
    referenced_idv_modificatio = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'procurement'

# moved to reference app
class RefCgacCode(models.Model):
    agency_code_cgac = models.CharField(primary_key=True, max_length=3)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'ref_cgac_code'

# moved to reference app
class RefCityCountyCode(models.Model):
    city_county_code_id = models.AutoField(primary_key=True)
    state_code = models.CharField(max_length=2, blank=True, null=True)
    city_name = models.CharField(max_length=50, blank=True, null=True)
    city_code = models.CharField(max_length=5, blank=True, null=True)
    county_code = models.CharField(max_length=3, blank=True, null=True)
    county_name = models.CharField(max_length=100, blank=True, null=True)
    type_of_area = models.CharField(max_length=20, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'ref_city_county_code'


class RefCountryCode(models.Model):
    country_code = models.CharField(primary_key=True, max_length=3)
    country_name = models.CharField(max_length=100, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'ref_country_code'


class RefDomainEnumerationType(models.Model):
    ref_domain_enumeration_type_id = models.AutoField(primary_key=True)
    domain_name = models.CharField(max_length=40, blank=True, null=True)
    enumeration_data_type = models.CharField(max_length=50, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    label = models.CharField(max_length=800, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'ref_domain_enumeration_type'


class RefObjectClassCode(models.Model):
    object_class = models.CharField(primary_key=True, max_length=4)
    max_object_class_name = models.CharField(max_length=60, blank=True, null=True)
    direct_or_reimbursable = models.CharField(max_length=25, blank=True, null=True)
    label = models.CharField(max_length=100, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'ref_object_class_code'


class RefProgramActivity(models.Model):
    program_activity_code = models.CharField(max_length=4)
    program_activity_name = models.CharField(max_length=164)
    budget_year = models.CharField(max_length=4, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'ref_program_activity'
        unique_together = (('program_activity_code', 'program_activity_name'),)


class RefStateCode(models.Model):
    state_code = models.CharField(primary_key=True, max_length=2)
    state_name = models.CharField(max_length=50, blank=True, null=True)
    state_fips_code = models.CharField(max_length=2, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'ref_state_code'


class RefSubtierAgencyOfficeCode(models.Model):
    department_id = models.CharField(max_length=10, blank=True, null=True)
    department_name = models.CharField(max_length=150, blank=True, null=True)
    agency_code = models.CharField(max_length=10, blank=True, null=True)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    contracting_office_code = models.CharField(max_length=6, blank=True, null=True)
    contracting_office_name = models.CharField(max_length=150, blank=True, null=True)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    address_line_1 = models.CharField(max_length=150, blank=True, null=True)
    address_line_2 = models.CharField(max_length=150, blank=True, null=True)
    address_line_3 = models.CharField(max_length=55, blank=True, null=True)
    address_city = models.CharField(max_length=40, blank=True, null=True)
    address_state = models.CharField(max_length=2, blank=True, null=True)
    zip_code = models.CharField(max_length=10, blank=True, null=True)
    country_code = models.CharField(max_length=3, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'ref_subtier_agency_office_code'


class StagingRefCgacCode(models.Model):
    agency_code_cgac = models.CharField(primary_key=True, max_length=3)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_ref_cgac_code'


class StagingRefCityCountyCode(models.Model):
    city_county_code_id = models.AutoField(primary_key=True)
    state_code = models.CharField(max_length=2, blank=True, null=True)
    city_name = models.CharField(max_length=50, blank=True, null=True)
    city_code = models.CharField(max_length=5, blank=True, null=True)
    county_code = models.CharField(max_length=3, blank=True, null=True)
    county_name = models.CharField(max_length=100, blank=True, null=True)
    type_of_area = models.CharField(max_length=20, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_ref_city_county_code'


class StagingRefCountryCode(models.Model):
    country_code = models.CharField(primary_key=True, max_length=3)
    country_name = models.CharField(max_length=100, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_ref_country_code'


class StagingRefDomainEnumerationType(models.Model):
    ref_domain_enumeration_type_id = models.AutoField(primary_key=True)
    domain_name = models.CharField(max_length=40, blank=True, null=True)
    enumeration_data_type = models.CharField(max_length=50, blank=True, null=True)
    code = models.CharField(max_length=20, blank=True, null=True)
    label = models.CharField(max_length=800, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_ref_domain_enumeration_type'


class StagingRefObjectClassCode(models.Model):
    object_class = models.CharField(primary_key=True, max_length=4)
    max_object_class_name = models.CharField(max_length=60, blank=True, null=True)
    direct_or_reimbursable = models.CharField(max_length=25, blank=True, null=True)
    label = models.CharField(max_length=100, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_ref_object_class_code'


class StagingRefProgramActivity(models.Model):
    program_activity_code = models.CharField(max_length=4)
    program_activity_name = models.CharField(max_length=164)
    budget_year = models.CharField(max_length=4, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_ref_program_activity'
        unique_together = (('program_activity_code', 'program_activity_name'),)


class StagingRefStateCode(models.Model):
    state_code = models.CharField(primary_key=True, max_length=2)
    state_name = models.CharField(max_length=50, blank=True, null=True)
    state_fips_code = models.CharField(max_length=2, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_ref_state_code'


class StagingRefSubtierAgencyOfficeCode(models.Model):
    department_id = models.CharField(max_length=10, blank=True, null=True)
    department_name = models.CharField(max_length=150, blank=True, null=True)
    agency_code = models.CharField(max_length=10, blank=True, null=True)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    contracting_office_code = models.CharField(max_length=6, blank=True, null=True)
    contracting_office_name = models.CharField(max_length=150, blank=True, null=True)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    address_line_1 = models.CharField(max_length=150, blank=True, null=True)
    address_line_2 = models.CharField(max_length=150, blank=True, null=True)
    address_line_3 = models.CharField(max_length=55, blank=True, null=True)
    address_city = models.CharField(max_length=40, blank=True, null=True)
    address_state = models.CharField(max_length=2, blank=True, null=True)
    zip_code = models.CharField(max_length=10, blank=True, null=True)
    country_code = models.CharField(max_length=3, blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.CharField(max_length=1, blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_ref_subtier_agency_office_code'


class StagingTreasuryAppropriationAccount(models.Model):
    treasury_account_identifier = models.IntegerField(primary_key=True)
    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
    responsible_agency_id = models.CharField(max_length=3)
    allocation_transfer_agency_id = models.CharField(max_length=3, blank=True, null=True)
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

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'staging_treasury_appropriation_account'


class SubAward(models.Model):
    sub_award_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.DO_NOTHING)
    place_of_performance_relationship = models.ForeignKey(PlaceOfPerformanceRelationship, models.DO_NOTHING)
    legal_entity = models.ForeignKey(LegalEntity, models.DO_NOTHING)
    sub_awardee_or_recipient_u = models.CharField(max_length=9, blank=True, null=True)
    sub_awardee_ultimate_pa_id = models.CharField(max_length=9, blank=True, null=True)
    sub_awardee_ultimate_paren = models.CharField(max_length=120, blank=True, null=True)
    subawardee_business_type = models.CharField(max_length=255, blank=True, null=True)
    sub_awardee_or_recipient_l = models.CharField(max_length=120, blank=True, null=True)
    subcontract_award_amount = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    cfda_number_and_title = models.CharField(max_length=255, blank=True, null=True)
    prime_award_report_id = models.CharField(max_length=40, blank=True, null=True)
    award_report_month = models.CharField(max_length=25, blank=True, null=True)
    award_report_year = models.CharField(max_length=4, blank=True, null=True)
    rec_model_question1 = models.CharField(max_length=1, blank=True, null=True)
    rec_model_question2 = models.CharField(max_length=1, blank=True, null=True)
    subaward_number = models.CharField(max_length=32, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'sub_award'


class SubmissionAttributes(models.Model):
    submission_id = models.AutoField(primary_key=True)
    user_id = models.IntegerField()
    cgac_code = models.CharField(max_length=3, blank=True, null=True)
    submitting_agency = models.CharField(max_length=150, blank=True, null=True)
    submitter_name = models.CharField(max_length=200, blank=True, null=True)
    submission_modification = models.NullBooleanField()
    version_number = models.IntegerField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'submission_attributes'


class SubmissionProcess(models.Model):
    submission_process_id = models.AutoField(primary_key=True)
    submission = models.ForeignKey(SubmissionAttributes, models.DO_NOTHING)
    status = models.CharField(max_length=50, blank=True, null=True)
    file_a_submission = models.NullBooleanField()
    file_b_submission = models.NullBooleanField()
    file_c_submission = models.NullBooleanField()
    file_d1_submission = models.NullBooleanField()
    file_d2_submission = models.NullBooleanField()
    file_e_submission = models.NullBooleanField()
    file_f_submission = models.NullBooleanField()
    create_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'submission_process'


# AJ 09/07/2016...these Test tables are not needed
# class TestEmp(models.Model):
#    test_emp_id = models.AutoField(primary_key=True)
#    empname = models.TextField()
#    salary = models.IntegerField(blank=True, null=True)
#
#    class Meta:
#        managed = False
#        db_table = 'test_emp'
#
#
# class TestEmpAudit(models.Model):
#    adt_test_emp_id = models.AutoField()
#    test_emp_id = models.IntegerField(blank=True, null=True)
#    operation = models.CharField(max_length=1)
#    stamp = models.DateTimeField()
#    userid = models.TextField()
#    empname = models.TextField()
#    salary = models.IntegerField(blank=True, null=True)
#
#    class Meta:
#        managed = False
#        db_table = 'test_emp_audit'
#
#
# class TestStateCountyCityCodes(models.Model):
#    state_code = models.CharField(max_length=2, blank=True, null=True)
#    county_code = models.CharField(max_length=3, blank=True, null=True)
#    county_name = models.CharField(max_length=40, blank=True, null=True)
#    city_code = models.CharField(max_length=5, blank=True, null=True)
#    city_name = models.CharField(max_length=50, blank=True, null=True)
#
#    class Meta:
#        managed = False
#        db_table = 'test_state_county_city_codes'
#
#
# class TestTreasuryAppropriationAccount1(models.Model):
#    treasury_account_identifier = models.IntegerField(primary_key=True)
#    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
#    responsible_agency_id = models.CharField(max_length=3)
#    responsible_agency_id_nvl = models.CharField(max_length=3)
#    allocation_transfer_agency_id = models.CharField(max_length=3, blank=True, null=True)
#    allocation_transfer_agency_id_nvl = models.CharField(max_length=3, blank=True, null=True)
#    beginning_period_of_availa = models.CharField(max_length=4, blank=True, null=True)
#    beginning_period_of_availa_nvl = models.CharField(max_length=4, blank=True, null=True)
#    ending_period_of_availabil = models.CharField(max_length=4, blank=True, null=True)
#    ending_period_of_availabil_nvl = models.CharField(max_length=4, blank=True, null=True)
#    availability_type_code = models.CharField(max_length=1, blank=True, null=True)
#    availability_type_code_nvl = models.CharField(max_length=1, blank=True, null=True)
#    main_account_code = models.CharField(max_length=4)
#    main_account_code_nvl = models.CharField(max_length=4)
#    sub_account_code = models.CharField(max_length=3)
#    sub_account_code_nvl = models.CharField(max_length=3)
#    drv_approp_avail_pd_start_date = models.DateField(blank=True, null=True)
#    drv_approp_avail_pd_end_date = models.DateField(blank=True, null=True)
#    drv_approp_account_exp_status = models.CharField(max_length=10, blank=True, null=True)
#    create_date = models.DateTimeField(blank=True, null=True)
#    update_date = models.DateTimeField(blank=True, null=True)
#    create_user_id = models.CharField(max_length=50, blank=True, null=True)
#    update_user_id = models.CharField(max_length=50, blank=True, null=True)
#
#    class Meta:
#        managed = False
#        db_table = 'test_treasury_appropriation_account_1'
#
#
# class TestTreasuryAppropriationAccount2(models.Model):
#    treasury_account_identifier = models.IntegerField(primary_key=True)
#    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
#    responsible_agency_id = models.CharField(max_length=3)
#    responsible_agency_id_nvl = models.CharField(max_length=3)
#    allocation_transfer_agency_id = models.CharField(max_length=3, blank=True, null=True)
#    allocation_transfer_agency_id_nvl = models.CharField(max_length=3, blank=True, null=True)
#    beginning_period_of_availa = models.CharField(max_length=4, blank=True, null=True)
#    beginning_period_of_availa_nvl = models.CharField(max_length=4, blank=True, null=True)
#    ending_period_of_availabil = models.CharField(max_length=4, blank=True, null=True)
#    ending_period_of_availabil_nvl = models.CharField(max_length=4, blank=True, null=True)
#    availability_type_code = models.CharField(max_length=1, blank=True, null=True)
#    availability_type_code_nvl = models.CharField(max_length=1, blank=True, null=True)
#    main_account_code = models.CharField(max_length=4)
#    main_account_code_nvl = models.CharField(max_length=4)
#    sub_account_code = models.CharField(max_length=3)
#    sub_account_code_nvl = models.CharField(max_length=3)
#    drv_approp_avail_pd_start_date = models.DateField(blank=True, null=True)
#    drv_approp_avail_pd_end_date = models.DateField(blank=True, null=True)
#    drv_approp_account_exp_status = models.CharField(max_length=10, blank=True, null=True)
#    create_date = models.DateTimeField(blank=True, null=True)
#    update_date = models.DateTimeField(blank=True, null=True)
#    create_user_id = models.CharField(max_length=50, blank=True, null=True)
#    update_user_id = models.CharField(max_length=50, blank=True, null=True)
#
#    class Meta:
#        managed = False
#        db_table = 'test_treasury_appropriation_account_2'

# AJ 09/07/2016...For treasury_account_identifier, changed AutoField to IntegerField
class TreasuryAppropriationAccount(models.Model):
    treasury_account_identifier = models.IntegerField(primary_key=True)
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

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'treasury_appropriation_account'


class Users(models.Model):
    user_id = models.AutoField(primary_key=True)
    title = models.CharField(max_length=20, blank=True, null=True)
    last_name = models.CharField(max_length=50, blank=True, null=True)
    first_name = models.CharField(max_length=50, blank=True, null=True)
    middle_initial = models.CharField(max_length=1, blank=True, null=True)
    agency_name = models.CharField(max_length=150, blank=True, null=True)
    cgac_code = models.CharField(max_length=3, blank=True, null=True)
    process = models.CharField(max_length=50, blank=True, null=True)
    password = models.CharField(max_length=50, blank=True, null=True)
    salt = models.CharField(max_length=50, blank=True, null=True)
    login_name = models.CharField(max_length=100, blank=True, null=True)
    last_login_date = models.DateTimeField(blank=True, null=True)
    user_active = models.NullBooleanField()
    incorrect_password_attempts = models.IntegerField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

# AJ 09/07/2016...changed managed to TRUE
    class Meta:
        managed = True
        db_table = 'users'
