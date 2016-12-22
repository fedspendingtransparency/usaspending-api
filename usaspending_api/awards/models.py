from django.db import models
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.models import RefProgramActivity, RefObjectClassCode, Agency, Location, LegalEntity
from usaspending_api.common.models import DataSourceTrackedModel
from django.db.models import F, Sum


# Model Objects added white spaces
class FinancialAccountsByAwards(DataSourceTrackedModel):
    financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    appropriation_account_balances = models.ForeignKey(AppropriationAccountBalances, models.CASCADE)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    award = models.ForeignKey('awards.Award', models.CASCADE, null=True, related_name="financial_set")
    program_activity_name = models.CharField(max_length=164, blank=True, null=True)
    program_activity_code = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, db_column='program_activity_code', blank=True, null=True)
    object_class = models.ForeignKey(RefObjectClassCode, models.DO_NOTHING, null=True, db_column='object_class')
    by_direct_reimbursable_funding_source = models.CharField(max_length=1, blank=True, null=True)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    award_type = models.CharField(max_length=30, blank=True, null=True)
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl490100_delivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl490100_delivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl490200_delivered_orders_obligations_paid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    obligations_undelivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    obligations_delivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    obligations_delivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    gross_outlays_undelivered_orders_prepaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    gross_outlays_undelivered_orders_prepaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    gross_outlays_delivered_orders_paid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    gross_outlay_amount_by_award_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    gross_outlay_amount_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    obligations_incurred_total_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    deobligations_recoveries_refunds_of_prior_year_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    obligations_undelivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    gross_outlays_delivered_orders_paid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    drv_award_id_field_type = models.CharField(max_length=10, blank=True, null=True)
    drv_obligations_incurred_total_by_award = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards'


class FinancialAccountsByAwardsTransactionObligations(DataSourceTrackedModel):
    financial_accounts_by_awards_transaction_obligations_id = models.AutoField(primary_key=True)
    financial_accounts_by_awards = models.ForeignKey('FinancialAccountsByAwards', models.CASCADE)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    transaction_obligated_amount = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards_transaction_obligations'


class Award(DataSourceTrackedModel):

    AWARD_TYPES = (
        ('U', 'Unknown Type'),
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

    type = models.CharField(max_length=5, choices=AWARD_TYPES, verbose_name="Award Type", default='U', null=True)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award = models.ForeignKey('awards.Award', related_name='child_award', null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    # dollarsobligated
    # This is a sum that should get updated when a transaction is entered
    total_obligation = models.DecimalField(max_digits=15, decimal_places=2, null=True, verbose_name="Total Obligated")
    total_outlay = models.DecimalField(max_digits=15, decimal_places=2, null=True)

    # maj_agency_cat
    awarding_agency = models.ForeignKey(Agency, related_name='+', null=True)
    funding_agency = models.ForeignKey(Agency, related_name='+', null=True)

    # signeddate
    date_signed = models.DateField(null=True, verbose_name="Award Date")
    # vendorname
    recipient = models.ForeignKey(LegalEntity, null=True)
    # Changed by KPJ to 4000 from 255, on 20161013
    description = models.CharField(max_length=4000, null=True, verbose_name="Award Description")
    period_of_performance_start_date = models.DateField(null=True, verbose_name="Start Date")
    period_of_performance_current_end_date = models.DateField(null=True, verbose_name="End Date")
    place_of_performance = models.ForeignKey(Location, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    latest_submission = models.ForeignKey(SubmissionAttributes, null=True)
    # recipient_name = models.CharField(max_length=250, null=True)
    # recipient_address_line1 = models.CharField(max_length=100, null=True)

    def __str__(self):
        # define a string representation of an award object
        return '%s piid: %s fain: %s uri: %s' % (self.get_type_display(), self.piid, self.fain, self.uri)

    def __get_latest_transaction(self):
        return self.__get_transaction_set().latest("action_date")

    # We should only have either procurements or financial assistance awards
    def __get_transaction_set(self):
        # Do we have procurements or financial assistance awards?
        transaction_set = self.procurement_set
        if transaction_set.count() == 0:
            transaction_set = self.financialassistanceaward_set
        return transaction_set

    def __get_type_description(self):
        return [item for item in self.AWARD_TYPES if item == self.type][0][1]

    def update_from_mod(self, mod):
        transaction_set = self.__get_transaction_set()
        self.date_signed = transaction_set.earliest("action_date").action_date
        self.period_of_performance_start_date = transaction_set.earliest("action_date").action_date
        self.period_of_perfoormance_current_end_date = transaction_set.latest("action_date").action_date
        self.total_obligation = transaction_set.aggregate(total_obs=Sum(F('federal_action_obligation')))['total_obs']
        self.save()

    @staticmethod
    def get_default_fields():
        default_fields = ['type', 'piid', 'fain', 'procurement_set']
        return default_fields

    latest_award_transaction = property(__get_latest_transaction)  # models.ForeignKey('AwardAction')
    type_description = property(__get_type_description)

    class Meta:
        db_table = 'awards'


class AwardAction(DataSourceTrackedModel):
    award = models.ForeignKey(Award, models.CASCADE, related_name="actions")
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    action_date = models.DateField(max_length=10, verbose_name="Transaction Date")
    action_type = models.CharField(max_length=1, blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    modification_number = models.CharField(max_length=50, blank=True, null=True, verbose_name="Modification Number")
    awarding_agency = models.ForeignKey(Agency, null=True)
    recipient = models.ForeignKey(LegalEntity, null=True)
    # Changed by KPJ to 4000 from 255, on 20161013
    award_description = models.CharField(max_length=4000, null=True)
    drv_award_transaction_usaspend = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_current_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    # Override the save method so that after saving we always call update_from_mod on our Award
    def save(self, *args, **kwargs):
        super(AwardAction, self).save(*args, **kwargs)
        self.award.update_from_mod(self)

    class Meta:
        abstract = True


# BD 09/26/2016 Added rec_flag data, parent_award_awarding_agency_code, current_aggregated_total_v, current_total_value_adjust,potential_idv_total_est, potential_aggregated_idv_t, potential_aggregated_total, and potential_total_value_adju data elements to the procurement table
class Procurement(AwardAction):
    procurement_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.CASCADE)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    piid = models.CharField(max_length=50, blank=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Parent Award ID")
    cost_or_pricing_data = models.CharField(max_length=1, blank=True, null=True)
    type_of_contract_pricing = models.CharField(max_length=2, blank=True, null=True, verbose_name="Type of Contract Pricing")
    contract_award_type = models.CharField(max_length=1, blank=True, null=True, verbose_name="Contract Award Type")
    naics = models.CharField(max_length=6, blank=True, null=True, verbose_name="NAICS")
    naics_description = models.CharField(max_length=150, blank=True, null=True, verbose_name="NAICS Description")
    period_of_performance_potential_end_date = models.CharField(max_length=8, blank=True, null=True, verbose_name="Period of Performance Potential End Date")
    ordering_period_end_date = models.CharField(max_length=8, blank=True, null=True)
    current_total_value_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    potential_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="Potential Total Value of Award")
    referenced_idv_agency_identifier = models.CharField(max_length=4, blank=True, null=True)
    idv_type = models.CharField(max_length=1, blank=True, null=True, verbose_name="IDV Type")
    multiple_or_single_award_idv = models.CharField(max_length=1, blank=True, null=True)
    type_of_idc = models.CharField(max_length=1, blank=True, null=True, verbose_name="Type of IDC")
    a76_fair_act_action = models.CharField(max_length=1, blank=True, null=True, verbose_name="A-76 FAIR Act Action")
    dod_claimant_program_code = models.CharField(max_length=3, blank=True, null=True)
    clinger_cohen_act_planning = models.CharField(max_length=1, blank=True, null=True)
    commercial_item_acquisition_procedures = models.CharField(max_length=1, blank=True, null=True)
    commercial_item_test_program = models.CharField(max_length=1, blank=True, null=True)
    consolidated_contract = models.CharField(max_length=1, blank=True, null=True)
    contingency_humanitarian_or_peacekeeping_operation = models.CharField(max_length=1, blank=True, null=True)
    contract_bundling = models.CharField(max_length=1, blank=True, null=True)
    contract_financing = models.CharField(max_length=1, blank=True, null=True)
    contracting_officers_determination_of_business_size = models.CharField(max_length=1, blank=True, null=True)
    cost_accounting_standards = models.CharField(max_length=1, blank=True, null=True)
    country_of_product_or_service_origin = models.CharField(max_length=3, blank=True, null=True)
    davis_bacon_act = models.CharField(max_length=1, blank=True, null=True)
    evaluated_preference = models.CharField(max_length=6, blank=True, null=True)
    extent_competed = models.CharField(max_length=3, blank=True, null=True)
    fed_biz_opps = models.CharField(max_length=1, blank=True, null=True)
    foreign_funding = models.CharField(max_length=1, blank=True, null=True)
    gfe_gfp = models.CharField(max_length=1, blank=True, null=True)
    information_technology_commercial_item_category = models.CharField(max_length=1, blank=True, null=True)
    interagency_contracting_authority = models.CharField(max_length=1, blank=True, null=True)
    local_area_set_aside = models.CharField(max_length=1, blank=True, null=True)
    major_program = models.CharField(max_length=100, blank=True, null=True)
    purchase_card_as_payment_method = models.CharField(max_length=1, blank=True, null=True)
    multi_year_contract = models.CharField(max_length=1, blank=True, null=True)
    national_interest_action = models.CharField(max_length=4, blank=True, null=True)
    number_of_actions = models.CharField(max_length=3, blank=True, null=True)
    number_of_offers_received = models.CharField(max_length=3, blank=True, null=True)
    other_statutory_authority = models.CharField(max_length=1, blank=True, null=True)
    performance_based_service_acquisition = models.CharField(max_length=1, blank=True, null=True)
    place_of_manufacture = models.CharField(max_length=1, blank=True, null=True)
    price_evaluation_adjustment_preference_percent_difference = models.DecimalField(max_digits=2, decimal_places=2, blank=True, null=True)
    product_or_service_code = models.CharField(max_length=4, blank=True, null=True)
    program_acronym = models.CharField(max_length=25, blank=True, null=True)
    other_than_full_and_open_competition = models.CharField(max_length=3, blank=True, null=True)
    recovered_materials_sustainability = models.CharField(max_length=1, blank=True, null=True)
    research = models.CharField(max_length=3, blank=True, null=True)
    sea_transportation = models.CharField(max_length=1, blank=True, null=True)
    service_contract_act = models.CharField(max_length=1, blank=True, null=True)
    small_business_competitiveness_demonstration_program = models.CharField(max_length=1, blank=True, null=True)
    solicitation_identifier = models.CharField(max_length=25, blank=True, null=True, verbose_name="Solicitation ID")
    solicitation_procedures = models.CharField(max_length=5, blank=True, null=True)
    fair_opportunity_limited_sources = models.CharField(max_length=50, blank=True, null=True)
    subcontracting_plan = models.CharField(max_length=1, blank=True, null=True)
    program_system_or_equipment_code = models.CharField(max_length=4, blank=True, null=True)
    type_set_aside = models.CharField(max_length=10, blank=True, null=True, verbose_name="Type Set Aside")
    epa_designated_product = models.CharField(max_length=1, blank=True, null=True)
    walsh_healey_act = models.CharField(max_length=1, blank=True, null=True)
    transaction_number = models.CharField(max_length=6, blank=True, null=True)
    # Changed by KPJ to 25 from 1, on 20161013
    referenced_idv_modification_number = models.CharField(max_length=25, blank=True, null=True)
    rec_flag = models.CharField(max_length=1, blank=True, null=True)
    drv_parent_award_awarding_agency_code = models.CharField(max_length=4, blank=True, null=True)
    drv_current_aggregated_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_current_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_award_idv_amount_total_estimate = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_aggregated_award_idv_amount_total_estimate = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_aggregated_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)

    @staticmethod
    def get_default_fields():
        default_fields = ['procurement_id']
        return default_fields


class FinancialAssistanceAward(AwardAction):
    financial_assistance_award_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.CASCADE)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    cfda_number = models.CharField(max_length=7, blank=True, null=True, verbose_name="CFDA Number")
    cfda_title = models.CharField(max_length=250, blank=True, null=True, verbose_name="CFDA Title")
    business_funds_indicator = models.CharField(max_length=3)
    non_federal_funding_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    total_funding_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    assistance_type = models.CharField(max_length=2, verbose_name="Assistance Type")
    record_type = models.IntegerField()
    correction_late_delete_indicator = models.CharField(max_length=1, blank=True, null=True)
    fiscal_year_and_quarter_correction = models.CharField(max_length=5, blank=True, null=True)
    sai_number = models.CharField(max_length=50, blank=True, null=True, verbose_name="SAI Number")
    drv_federal_funding_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_award_finance_assistance_type_label = models.CharField(max_length=50, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    submitted_type = models.CharField(max_length=1, blank=True, null=True, verbose_name="Submitted Type")
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'financial_assistance_award'


class SubAward(DataSourceTrackedModel):
    sub_award_id = models.AutoField(primary_key=True, verbose_name="Sub-Award ID")
    award = models.ForeignKey(Award, models.CASCADE)
    legal_entity = models.ForeignKey(LegalEntity, models.CASCADE)
    sub_recipient_unique_id = models.CharField(max_length=9, blank=True, null=True)
    sub_recipient_ultimate_parent_unique_id = models.CharField(max_length=9, blank=True, null=True)
    sub_recipient_ultimate_parent_name = models.CharField(max_length=120, blank=True, null=True)
    subawardee_business_type = models.CharField(max_length=255, blank=True, null=True)
    sub_recipient_name = models.CharField(max_length=120, blank=True, null=True)
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
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'sub_award'
