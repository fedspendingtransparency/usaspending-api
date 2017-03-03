from django.db import models
from django.db.models import F, Q, Sum

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.models import RefProgramActivity, RefObjectClassCode, Agency, Location, LegalEntity
from usaspending_api.common.models import DataSourceTrackedModel

AWARD_TYPES = (
    ('U', 'Unknown Type'),
    ('02', 'Block Grant'),
    ('03', 'Formula Grant'),
    ('04', 'Project Grant'),
    ('05', 'Cooperative Agreement'),
    ('06', 'Direct Payment for Specified Use'),
    ('07', 'Direct Loan'),
    ('08', 'Guaranteed/Insured Loan'),
    ('09', 'Insurance'),
    ('10', 'Direct Payment unrestricted'),
    ('11', 'Other'),
    ('A', 'BPA Call'),
    ('B', 'Purchase Order'),
    ('C', 'Delivery Order'),
    ('D', 'Definitive Contract')
)

CONTRACT_PRICING_TYPES = (
    ('A', 'Fixed Price Redetermination'),
    ('B', 'Fixed Price Level of Effort'),
    ('J', 'Firm Fixed Price'),
    ('K', 'Fixed Price with Economic Price Adjustment'),
    ('L', 'Fixed Price Incentive'),
    ('M', 'Fixed Price Award Fee'),
    ('R', 'Cost Plus Award Fee'),
    ('S', 'Cost No Fee'),
    ('T', 'Cost Sharing'),
    ('U', 'Cost Plus Fixed Fee'),
    ('V', 'Cost Plus Incentive Fee'),
    ('Y', 'Time and Materials'),
    ('Z', 'Labor Hours'),
    ('UN', 'Unknown Type')
)


class FinancialAccountsByAwards(DataSourceTrackedModel):
    financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    treasury_account = models.ForeignKey(TreasuryAppropriationAccount, models.CASCADE, null=True)
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

    @staticmethod
    def get_default_fields(path=None):
        return [
            "financial_accounts_by_awards_id",
            "treasury_account",
            "transaction_obligations",
            "object_class",
            "program_activity_code",
            "program_activity_name",
            "piid",
            "fain",
            "uri",
            "gross_outlay_amount_by_award_cpe",
            "gross_outlay_amount_by_award_fyb",
            "certified_date",
            "last_modified_date"
        ]

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards'


class FinancialAccountsByAwardsTransactionObligations(DataSourceTrackedModel):
    financial_accounts_by_awards_transaction_obligations_id = models.AutoField(primary_key=True)
    financial_accounts_by_awards = models.ForeignKey('FinancialAccountsByAwards', models.CASCADE, related_name="transaction_obligations")
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    transaction_obligated_amount = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    @staticmethod
    def get_default_fields(path=None):
        return [
            "transaction_obligated_amount"
        ]

    class Meta:
        managed = True
        db_table = 'financial_accounts_by_awards_transaction_obligations'


class AwardManager(models.Manager):
    def get_queryset(self):
        '''
        A generated award will have these set to null, but will also receive no
        transactions. Thus, these will remain null. This finds those awards and
        throws them out. As soon as one of those awards gets a transaction
        (i.e. it is no longer empty), these will be updated via update_from_transaction
        and the award will no longer match these criteria
        '''
        q_kwargs = {
            "type": "U",
            "total_obligation__isnull": True,
            "date_signed__isnull": True,
            "recipient__isnull": True
        }
        return super(AwardManager, self).get_queryset().filter(~Q(**q_kwargs))


class Award(DataSourceTrackedModel):

    type = models.CharField(max_length=5, choices=AWARD_TYPES, verbose_name="Award Type", default='U', null=True)
    type_description = models.CharField(max_length=50, verbose_name="Award Type Description", default="Unknown Type", null=True)
    piid = models.CharField(max_length=50, blank=True, null=True)
    parent_award = models.ForeignKey('awards.Award', related_name='child_award', null=True)
    fain = models.CharField(max_length=30, blank=True, null=True)
    uri = models.CharField(max_length=70, blank=True, null=True)
    total_obligation = models.DecimalField(max_digits=15, decimal_places=2, null=True, verbose_name="Total Obligated")
    total_outlay = models.DecimalField(max_digits=15, decimal_places=2, null=True)
    awarding_agency = models.ForeignKey(Agency, related_name='+', null=True, db_index=True)
    funding_agency = models.ForeignKey(Agency, related_name='+', null=True, db_index=True)
    date_signed = models.DateField(null=True, verbose_name="Award Date")
    recipient = models.ForeignKey(LegalEntity, null=True, db_index=True)
    description = models.CharField(max_length=4000, null=True, verbose_name="Award Description")
    period_of_performance_start_date = models.DateField(null=True, verbose_name="Start Date")
    period_of_performance_current_end_date = models.DateField(null=True, verbose_name="End Date")
    place_of_performance = models.ForeignKey(Location, null=True, db_index=True)
    potential_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="Potential Total Value of Award")
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    latest_submission = models.ForeignKey(SubmissionAttributes, null=True)

    objects = models.Manager()
    nonempty = AwardManager()

    @staticmethod
    def get_default_fields(path=None):
        return [
            "id",
            "type",
            "type_description",
            "total_obligation",
            "total_outlay",
            "date_signed",
            "description",
            "piid",
            "fain",
            "uri",
            "period_of_performance_start_date",
            "period_of_performance_current_end_date",
            "potential_total_value_of_award",
            "place_of_performance",
            "awarding_agency",
            "funding_agency",
            "recipient",
            "date_signed__fy",
        ]

    def __str__(self):
        return '%s piid: %s fain: %s uri: %s' % (self.get_type_display(), self.piid, self.fain, self.uri)

    # note: the next 3 functions are deprecated. they will stay here as we
    # transition from the AwardAction series of transaction models to those
    # based on Transaction/TransactionContract/TransactionAssistance and
    # should be removed when that work is done
    def __get_latest_transaction(self):
        return self.__get_transaction_set().latest("action_date")

    # We should only have either procurements or financial assistance awards
    def __get_transaction_set(self):
        # Do we have procurements or financial assistance awards?
        transaction_set = self.procurement_set
        if transaction_set.count() == 0:
            transaction_set = self.financialassistanceaward_set
        return transaction_set

    def update_from_mod(self, mod):
        transaction_set = self.__get_transaction_set()
        transaction_latest = transaction_set.latest("action_date")
        transaction_earliest = transaction_set.earliest("action_date")
        self.awarding_agency = transaction_latest.awarding_agency
        self.certified_date = transaction_latest.certified_date
        self.data_source = transaction_latest.data_source
        self.date_signed = transaction_earliest.action_date
        self.description = transaction_latest.description
        self.funding_agency = transaction_latest.funding_agency
        self.last_modified_date = transaction_latest.last_modified_date
        self.latest_submission = transaction_latest.submission
        self.period_of_performance_start_date = transaction_earliest.period_of_performance_start_date
        self.period_of_performance_current_end_date = transaction_latest.period_of_performance_current_end_date
        self.place_of_performance = transaction_latest.place_of_performance
        self.recipient = transaction_latest.recipient
        self.total_obligation = transaction_set.aggregate(total_obs=Sum(F('federal_action_obligation')))['total_obs']
        self.type = transaction_latest.type
        self.type_description = transaction_latest.type_description
        if hasattr(transaction_latest, "potential_total_value_of_award"):
            self.potential_total_value_of_award = transaction_latest.potential_total_value_of_award
        self.save()

    @staticmethod
    def get_or_create_summary_award(piid=None, fain=None, uri=None, awarding_agency=None, parent_award_id=None):
        # If an award transaction's ID is a piid, it's contract data
        # If the ID is fain or a uri, it's financial assistance. If the award transaction
        # has both a fain and a uri, fain takes precedence.
        q_kwargs = {}
        for i in [(piid, "piid"), (fain, "fain"), (uri, "uri")]:
            if i[0]:
                q_kwargs[i[1]] = i[0]
                if parent_award_id:
                    q_kwargs["parent_award__" + i[1]] = parent_award_id
                else:
                    q_kwargs["parent_award"] = None

                # Now search for it
                # Do we want to log something if the the query below turns up
                # more than one award record?
                summary_award = Award.objects.all().filter(Q(**q_kwargs)).filter(awarding_agency=awarding_agency).first()
                if summary_award:
                    return summary_award
                else:
                    parent_award = None
                    if parent_award_id:
                        # If we have a parent award id, recursively get/create the award for it
                        parent_award = Award.get_or_create_summary_award(**{i[1]: parent_award_id, 'awarding_agency': awarding_agency})
                    # Now create the award record for this award transaction
                    summary_award = Award(**{i[1]: i[0], "parent_award": parent_award, "awarding_agency": awarding_agency})
                    summary_award.save()
                    return summary_award

        raise ValueError(
            'Unable to find or create an award with the provided information: piid={}, fain={}, uri={}, parent_id={}'.format(
                piid, fain, uri, parent_award_id))

    class Meta:
        db_table = 'awards'


class Transaction(DataSourceTrackedModel):
    award = models.ForeignKey(Award, models.CASCADE)
    usaspending_unique_transaction_id = models.CharField(max_length=256, blank=True, null=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    type = models.CharField(max_length=5, choices=AWARD_TYPES, verbose_name="Action Type", default='U', null=True)
    type_description = models.CharField(max_length=50, verbose_name="Action Type Description", default="Unknown Type", null=True)
    period_of_performance_start_date = models.DateField(max_length=10, verbose_name="Period of Performance Start Date", null=True)
    period_of_performance_current_end_date = models.DateField(max_length=10, verbose_name="Period of Performance Current End Date", null=True)
    action_date = models.DateField(max_length=10, verbose_name="Transaction Date")
    action_type = models.CharField(max_length=1, blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    modification_number = models.CharField(max_length=50, blank=True, null=True, verbose_name="Modification Number")
    awarding_agency = models.ForeignKey(Agency, related_name='%(app_label)s_%(class)s_awarding_agency', null=True, db_index=True)
    funding_agency = models.ForeignKey(Agency, related_name='%(app_label)s_%(class)s_funding_agency', null=True, db_index=True)
    recipient = models.ForeignKey(LegalEntity, null=True, db_index=True)
    description = models.CharField(max_length=4000, null=True)
    place_of_performance = models.ForeignKey(Location, null=True, db_index=True)
    drv_award_transaction_usaspend = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_current_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    def __str__(self):
        return '%s award: %s' % (self.get_type_description(), self.award)

    @staticmethod
    def get_default_fields(path=None):
        return [
            "id",
            "type",
            "type_description",
            "period_of_performance_start_date",
            "period_of_performance_current_end_date",
            "action_date",
            "action_type",
            "action_date__fy",
            "federal_action_obligation",
            "modification_number",
            "awarding_agency",
            "funding_agency",
            "recipient",
            "description",
            "place_of_performance",
            "contract_data",  # must match related_name in TransactionContract
            "assistance_data"  # must match related_name in TransactionAssistance
        ]

    def get_type_description(self):
        description = [item for item in AWARD_TYPES if item[0] == self.type]
        if len(description) == 0:
            return "Unknown Type"
        else:
            return description[0][1]

    class Meta:
        db_table = 'transaction'
        index_together = ['award', 'action_date']


class TransactionContract(DataSourceTrackedModel):
    transaction = models.OneToOneField(
        Transaction, on_delete=models.CASCADE,
        primary_key=True, related_name='contract_data')
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    piid = models.CharField(max_length=50, blank=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Parent Award ID")
    cost_or_pricing_data = models.CharField(max_length=1, blank=True, null=True)
    type_of_contract_pricing = models.CharField(max_length=2, default="UN", blank=True, null=True, choices=CONTRACT_PRICING_TYPES, verbose_name="Type of Contract Pricing")
    type_of_contract_pricing_description = models.CharField(max_length=150, blank=True, null=True, verbose_name="Type of Contract Pricing Description")
    naics = models.CharField(max_length=6, blank=True, null=True, verbose_name="NAICS")
    naics_description = models.CharField(max_length=150, blank=True, null=True, verbose_name="NAICS Description")
    period_of_performance_potential_end_date = models.DateField(max_length=10, verbose_name="Period of Performance Potential End Date", null=True)
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
    number_of_actions = models.CharField(max_length=6, blank=True, null=True)
    number_of_offers_received = models.CharField(max_length=3, blank=True, null=True)
    other_statutory_authority = models.CharField(max_length=1000, blank=True, null=True)
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
    def get_default_fields(path=None):
        return [
            "piid",
            "parent_award_id",
            "type",
            "type_description",
            "cost_or_pricing_data",
            "type_of_contract_pricing",
            "type_of_contract_pricing_description",
            "naics",
            "naics_description",
            "product_or_service_code"
        ]

    class Meta:
        db_table = 'transaction_contract'


class TransactionAssistance(DataSourceTrackedModel):
    transaction = models.OneToOneField(
        Transaction, on_delete=models.CASCADE,
        primary_key=True, related_name='assistance_data')
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
    period_of_performance_start_date = models.DateField(blank=True, null=True)
    period_of_performance_current_end_date = models.DateField(blank=True, null=True)

    @staticmethod
    def get_default_fields(path=None):
        return [
            "fain",
            "uri",
            "cfda_number",
            "cfda_title",
            "face_value_loan_guarantee",
            "original_loan_subsidy_cost",
            "type"
        ]

    class Meta:
        db_table = 'transaction_assistance'


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


# To be removed: older transaction-related models here temporarily
# for interim compatibility
class AwardAction(DataSourceTrackedModel):
    award = models.ForeignKey(Award, models.CASCADE, related_name="actions")
    usaspending_unique_transaction_id = models.CharField(max_length=256, blank=True, null=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    type = models.CharField(max_length=5, choices=AWARD_TYPES, verbose_name="Action Type", default='U', null=True)
    type_description = models.CharField(max_length=50, verbose_name="Action Type Description", default="Unknown Type", null=True)
    period_of_performance_start_date = models.DateField(max_length=10, verbose_name="Period of Performance Start Date", null=True)
    period_of_performance_current_end_date = models.DateField(max_length=10, verbose_name="Period of Performance Current End Date", null=True)
    action_date = models.DateField(max_length=10, verbose_name="Transaction Date")
    action_type = models.CharField(max_length=1, blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    modification_number = models.CharField(max_length=50, blank=True, null=True, verbose_name="Modification Number")
    awarding_agency = models.ForeignKey(Agency, related_name='%(app_label)s_%(class)s_awarding_agency', null=True)
    funding_agency = models.ForeignKey(Agency, related_name='%(app_label)s_%(class)s_funding_agency', null=True)
    recipient = models.ForeignKey(LegalEntity, null=True)
    description = models.CharField(max_length=4000, null=True)
    place_of_performance = models.ForeignKey(Location, null=True)
    drv_award_transaction_usaspend = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_current_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    @staticmethod
    def get_default_fields(path=None):
        return [
            "modification_number",
            "federal_action_obligation",
            "action_date",
            "description",
            "update_date"
        ]

    def get_type_description(self):
        description = [item for item in AWARD_TYPES if item[0] == self.type]
        if len(description) == 0:
            return "Unknown Type"
        else:
            return description[0][1]

    # Override the save method so that after saving we always call update_from_mod on our Award
    def save(self, *args, **kwargs):
        self.type_description = self.get_type_description()
        super(AwardAction, self).save(*args, **kwargs)
        self.award.update_from_mod(self)

    class Meta:
        abstract = True


class Procurement(AwardAction):
    procurement_id = models.AutoField(primary_key=True)
    award = models.ForeignKey(Award, models.CASCADE)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    piid = models.CharField(max_length=50, blank=True)
    parent_award_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Parent Award ID")
    cost_or_pricing_data = models.CharField(max_length=1, blank=True, null=True)
    type_of_contract_pricing = models.CharField(max_length=2, default="UN", blank=True, null=True, choices=CONTRACT_PRICING_TYPES, verbose_name="Type of Contract Pricing")
    type_of_contract_pricing_description = models.CharField(max_length=150, blank=True, null=True, verbose_name="Type of Contract Pricing Description")
    naics = models.CharField(max_length=6, blank=True, null=True, verbose_name="NAICS")
    naics_description = models.CharField(max_length=150, blank=True, null=True, verbose_name="NAICS Description")
    period_of_performance_potential_end_date = models.DateField(max_length=10, verbose_name="Period of Performance Potential End Date", null=True)
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
    number_of_actions = models.CharField(max_length=6, blank=True, null=True)
    number_of_offers_received = models.CharField(max_length=3, blank=True, null=True)
    other_statutory_authority = models.CharField(max_length=1000, blank=True, null=True)
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
    def get_default_fields(path=None):
        default_fields = AwardAction.get_default_fields()
        return default_fields + [
            "type",
            "type_description",
            "cost_or_pricing_data",
            "type_of_contract_pricing",
            "type_of_contract_pricing_description",
            "naics",
            "naics_description",
            "product_or_service_code"
        ]

    def get_pricing_type_description(self):
        description = [item for item in CONTRACT_PRICING_TYPES if item[0] == self.type_of_contract_pricing]
        if len(description) == 0:
            return "Unknown Type"
        else:
            return description[0][1]

    # Override the save method so that after saving we always update our type description
    def save(self, *args, **kwargs):
        self.type_of_contract_pricing_description = self.get_pricing_type_description()
        super(Procurement, self).save(*args, **kwargs)


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
    period_of_performance_start_date = models.DateField(blank=True, null=True)
    period_of_performance_current_end_date = models.DateField(blank=True, null=True)

    @staticmethod
    def get_default_fields(path=None):
        default_fields = AwardAction.get_default_fields()
        return default_fields + [
            "cfda_number",
            "cfda_title",
            "face_value_loan_guarantee",
            "original_loan_subsidy_cost",
            "type"
        ]

    class Meta:
        managed = True
        db_table = 'financial_assistance_award'
