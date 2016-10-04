from django.db import models
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.models import RefProgramActivity, RefObjectClassCode, Agency, Location, LegalEntity
from django.db.models import F, Sum


# Model Objects
class FinancialAccountsByAwards(models.Model):
    financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    appropriation_account_balances = models.ForeignKey(AppropriationAccountBalances, models.DO_NOTHING)
    program_activity_name = models.CharField(max_length=164, blank=True, null=True)
    program_activity_code = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, db_column='program_activity_code',blank=True, null=True)
    object_class = models.ForeignKey(RefObjectClassCode, models.DO_NOTHING, db_column='object_class')
    by_direct_reimbursable_funding_source = models.CharField(max_length=1, blank=True, null=True)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    award_type = models.CharField(max_length=30, blank=True, null=True)
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl490100_delivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl490100_delivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl490200_delivered_orders_obligations_paid_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True,null=True)
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    obligations_undelivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True,null=True)
    obligations_delivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True,null=True)
    obligations_delivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True,null=True)
    gross_outlays_undelivered_orders_prepaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    gross_outlays_undelivered_orders_prepaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    gross_outlays_delivered_orders_paid_total_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True,null=True)
    gross_outlay_amount_by_award_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    gross_outlay_amount_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    obligations_incurred_total_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True,null=True)
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    deobligations_recoveries_refunds_of_prior_year_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=0,blank=True, null=True)
    obligations_undelivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=0, blank=True,null=True)
    gross_outlays_delivered_orders_paid_total_cpe = models.DecimalField(max_digits=21, decimal_places=0, blank=True,null=True)
    drv_award_id_field_type = models.CharField(max_length=10, blank=True, null=True)
    drv_oblig_incur_total_by_award = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards'


class FinancialAccountsByAwardsTransactionObligations(models.Model):
    financial_accounts_by_awards_transaction_obligations_id = models.AutoField(primary_key=True)
    financial_accounts_by_awards = models.ForeignKey('FinancialAccountsByAwards', models.CASCADE)
    transaction_obligated_amount = models.DecimalField(max_digits=21, decimal_places=0, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards_transaction_obligations'


class Award(models.Model):

    AWARD_TYPES = (
        ('2', 'Block Grant'),
        ('3', 'Formula Grant'),
        ('4', 'Project Grant'),
        ('5', 'Cooperative Agreement'),
        ('6', 'Direct Payment for Specified Use'),
        ('7', 'Direct Loan'),
        ('8', 'Guaranteed/Insured Loan'),
        ('9', 'Insurance'),
        ('10', 'Direct Payment unrestricted'),
        ('11', 'Other'),
        ('C', 'Contract'),
        ('G', 'Grant'),
        ('DP', 'Direct Payment'),
        ('L', 'Loan'),
    )

    type = models.CharField(max_length=5, choices=AWARD_TYPES)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    # dollarsobligated
    # This is a sum that should get updated when a transaction is entered
    total_obligation = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    total_outlay = models.DecimalField(max_digits=15, decimal_places=2, null=True)

    # maj_agency_cat
    awarding_agency = models.ForeignKey(Agency, related_name='+', null=True)
    funding_agency = models.ForeignKey(Agency, related_name='+', null=True)

    # signeddate
    date_signed = models.DateField(null=True)
    # vendorname
    recipient = models.ForeignKey(LegalEntity, null=True)
    description = models.CharField(max_length=255, null=True)
    period_of_performance_start_date = models.DateField(null=True)
    period_of_performance_current_end_date = models.DateField(null=True)
    place_of_performance = models.ForeignKey(Location, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    # this is a pointer to the latest mod, which should include most up
    # to date info on the location, etc.

    # Can use award.actions to get reverse reference to all actions

    latest_submission = models.ForeignKey(SubmissionAttributes, null=True)
    # recipient_name = models.CharField(max_length=250, null=True)
    # recipient_address_line1 = models.CharField(max_length=100, null=True)

    def __str__(self):
        # define a string representation of an award object
        return '%s #%s' % (self.get_type_display(), self.award_identifier)

    def __get_latest_submission(self):
        return self.actions.all().order_by('-action_date').first()

    def update_from_mod(self, mod):
        if self.type == 'C':
            # only contract loading/summing supported right now
            self.total_obligation = Procurement.objects.filter(piid=self.piid)\
                                .aggregate(total_obs=Sum(F('federal_action_obligation')))['total_obs']
            self.save()

    latest_award_transaction = property(__get_latest_submission)  # models.ForeignKey('AwardAction')

    class Meta:
        db_table = 'awards'


class AwardAction(models.Model):
    award = models.ForeignKey(Award, related_name="actions")
    action_date = models.CharField(max_length=10)
    action_type = models.CharField(max_length=1, blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    award_modification_amendme = models.CharField(max_length=50, blank=True, null=True)
    awarding_agency = models.ForeignKey(Agency, null=True)
    recipient = models.ForeignKey(LegalEntity, null=True)
    description = models.CharField(max_length=255, null=True)
    award_transaction_usaspend = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    current_total_value_award = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    potential_total_value_adju = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        abstract = True


# BD 09/26/2016 Added rec_flag data, parent_award_awarding_agency_code, current_aggregated_total_v, current_total_value_adjust,potential_idv_total_est, potential_aggregated_idv_t, potential_aggregated_total, and potential_total_value_adju data elements to the procurement table
class Procurement(AwardAction):
    procurement_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.DO_NOTHING)
    piid = models.CharField(max_length=50, blank=True)
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
    rec_flag = models.CharField(max_length=1, blank=True, null=True)
    parent_award_awarding_agency_code = models.CharField(max_length=4, blank=True, null=True)
    current_aggregated_total_v = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    current_total_value_adjust = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    potential_idv_total_est = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    potential_aggregated_idv_t = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    potential_aggregated_total = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    potential_total_value_adju = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)


class FinancialAssistanceAward(AwardAction):
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
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_assistance_award'
