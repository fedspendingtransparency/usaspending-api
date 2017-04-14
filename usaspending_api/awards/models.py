import warnings

from django.db import models
from django.db.models import F, Q, Sum

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.models import (
    Agency, CFDAProgram, LegalEntity, Location, ObjectClass, RefProgramActivity)
from usaspending_api.common.models import DataSourceTrackedModel
from django.core.cache import caches, CacheKeyWarning

warnings.simplefilter("ignore", CacheKeyWarning)


class FinancialAccountsByAwards(DataSourceTrackedModel):
    financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    treasury_account = models.ForeignKey(TreasuryAppropriationAccount, models.CASCADE, null=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    award = models.ForeignKey('awards.Award', models.CASCADE, null=True, related_name="financial_set")
    program_activity = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, null=True, db_index=True)
    object_class = models.ForeignKey(ObjectClass, models.DO_NOTHING, null=True, db_index=True)
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
            "financial_accounts_by_awards_id",
            "award",
            "treasury_account",
            "transaction_obligated_amount",
            "object_class",
            "program_activity",
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
            "latest_transaction__isnull": True,
            "date_signed__isnull": True,
            "total_obligation__isnull": True
        }

        return super(AwardManager, self).get_queryset().filter(~Q(**q_kwargs))

awards_cache = caches['awards']


class Award(DataSourceTrackedModel):
    """
    Model that provides a high-level award that individual transaction
    data can be mapped to. Transactions (i.e., contract and financial assistance
    data from the old USAspending site and D1/D2 files from the broker) represent
    specific actions against an award, though the award records themselves aren't
    assigned on the incoming data. To rectify that and to make the transactional
    data easier for people to understand, we create Award objects and map
    transactions to them.

    Much of the award record data (for example, awarding_agency, funding_agency,
    type) is automatically populated from info in the award's child transactions.
    These updates happen in our USAspending and data act broker load processes:
    see ETL\award_helpers.py for details.
    """

    type = models.CharField(max_length=5, db_index=True, verbose_name="Award Type", null=True, help_text="	The mechanism used to distribute funding. The federal government can distribute funding in several forms. These award types include contracts, grants, loans, and direct payments.")
    type_description = models.TextField(verbose_name="Award Type Description", blank=True, null=True, help_text="The plain text description of the type of the award")
    piid = models.CharField(max_length=50, db_index=True, blank=True, null=True, help_text="Procurement Instrument Identifier - A unique identifier assigned to a federal contract, purchase order, basic ordering agreement, basic agreement, and blanket purchase agreement. It is used to track the contract, and any modifications or transactions related to it. After October 2017, it is between 13 and 17 digits, both letters and numbers.")
    parent_award = models.ForeignKey('awards.Award', related_name='child_award', null=True, help_text="The parent award, if applicable")
    fain = models.CharField(max_length=30, db_index=True, blank=True, null=True, help_text="An identification code assigned to each financial assistance award tracking purposes. The FAIN is tied to that award (and all future modifications to that award) throughout the award’s life. Each FAIN is assigned by an agency. Within an agency, FAIN are unique: each new award must be issued a new FAIN. FAIN stands for Federal Award Identification Number, though the digits are letters, not numbers.")
    uri = models.CharField(max_length=70, db_index=True, blank=True, null=True, help_text="The uri of the award")
    total_obligation = models.DecimalField(max_digits=15, db_index=True, decimal_places=2, null=True, verbose_name="Total Obligated", help_text="The amount of money the government is obligated to pay for the award")
    total_outlay = models.DecimalField(max_digits=15, db_index=True, decimal_places=2, null=True, help_text="The total amount of money paid out for this award")
    awarding_agency = models.ForeignKey(Agency, related_name='+', null=True, help_text="The awarding agency for the award")
    funding_agency = models.ForeignKey(Agency, related_name='+', null=True, help_text="The funding agency for the award")
    date_signed = models.DateField(null=True, db_index=True, verbose_name="Award Date", help_text="The date the award was signed")
    recipient = models.ForeignKey(LegalEntity, null=True, help_text="The recipient of the award")
    description = models.CharField(max_length=4000, null=True, verbose_name="Award Description", help_text="A description of the award")
    period_of_performance_start_date = models.DateField(null=True, db_index=True, verbose_name="Start Date", help_text="The start date for the period of performance")
    period_of_performance_current_end_date = models.DateField(null=True, db_index=True, verbose_name="End Date", help_text="The current, not original, period of performance end date")
    place_of_performance = models.ForeignKey(Location, null=True, help_text="The principal place of business, where the majority of the work is performed. For example, in a manufacturing contract, this would be the main plant where items are produced.")
    potential_total_value_of_award = models.DecimalField(max_digits=20, db_index=True, decimal_places=2, blank=True, null=True, verbose_name="Potential Total Value of Award", help_text="The sum of the potential_value_of_award from associated transactions")
    last_modified_date = models.DateField(blank=True, null=True, help_text="The date this award was last modified")
    certified_date = models.DateField(blank=True, null=True, help_text="The date this record was certified")
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True, help_text="The date this record was created in the API")
    update_date = models.DateTimeField(auto_now=True, null=True, help_text="The last time this record was updated in the API")
    latest_submission = models.ForeignKey(SubmissionAttributes, models.DO_NOTHING, null=True, help_text="The submission attribute object that created this award")
    latest_transaction = models.ForeignKey("awards.Transaction", related_name="latest_for_award", null=True, help_text="The latest transaction by action_date associated with this award")

    objects = models.Manager()
    nonempty = AwardManager()

    def manual_hash(self):
        """Used to manually establish equality between instances.

        Useful for unsaved records where `.id` is not yet set.
        Possibly this could be converted to __hash__"""

        return hash((self.piid, self.fain, self.uri,
                    (self.parent_award and
                     (self.parent_award.piid,
                      self.parent_award.fain,
                      self.parent_award.uri))))

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
        return '%s piid: %s fain: %s uri: %s' % (self.type_description, self.piid, self.fain, self.uri)

    @staticmethod
    def get_or_create_summary_award(awarding_agency=None, piid=None, fain=None,
                                    uri=None, parent_award_id=None, use_cache=False):
        """
        Given a set of award identifiers and awarding agency information,
        find a corresponding Award record. If we can't find one, create it.

        Returns:
            created: a list of new awards created (or that need to be created
                if using cache), used to enable bulk insert
            summary_award: the summary award that the calling process can map to
        """
        # If an award transaction's ID is a piid, it's contract data
        # If the ID is fain or a uri, it's financial assistance. If the award transaction
        # has both a fain and a uri, fain takes precedence.
        q_kwargs = {}
        for i in [(piid, "piid"), (fain, "fain"), (uri, "uri")]:
            if i[0]:
                q_kwargs[i[1]] = i[0]
                if parent_award_id:
                    q_kwargs["parent_award__" + i[1]] = parent_award_id
                    # parent_award__piid, parent_award__fain, parent_award__uri
                else:
                    q_kwargs["parent_award"] = None

                # Now search for it
                # Do we want to log something if the the query below turns up
                # more than one award record?
                if use_cache:
                    q_kwargs_fixed = list(q_kwargs.items()) + [('awarding_agency', awarding_agency), ]
                    q_kwargs_fixed.sort()
                    summary_award = awards_cache.get(q_kwargs_fixed)
                    if summary_award:
                        return [], summary_award

                # Look for an existing award record
                summary_award = Award.objects \
                    .filter(Q(**q_kwargs)) \
                    .filter(awarding_agency=awarding_agency) \
                    .first()
                if (summary_award is None and
                        awarding_agency is not None and
                        awarding_agency.toptier_agency.name != awarding_agency.subtier_agency.name):
                    # No award match found when searching by award id info +
                    # awarding subtier agency. Relax the awarding agency
                    # critera to just the toptier agency instead of the subtier
                    # agency and try the search again.
                    awarding_agency_toptier = Agency.objects.filter(
                        toptier_agency__cgac_code=awarding_agency.toptier_agency.cgac_code,
                        subtier_agency__name=F('toptier_agency__name'))
                    summary_award = Award.objects \
                        .filter(Q(**q_kwargs)) \
                        .filter(awarding_agency=awarding_agency_toptier) \
                        .first()

                if summary_award:
                    if use_cache:
                        awards_cache.set(q_kwargs_fixed, summary_award)
                    return [], summary_award

                # We weren't able to match, so create a new award record.
                if parent_award_id:
                    # If parent award id was supplied, recursively get/create
                    # an award record for it
                    parent_created, parent_award = Award.get_or_create_summary_award(
                        use_cache=use_cache,
                        **{i[1]: parent_award_id, 'awarding_agency': awarding_agency})
                else:
                    parent_created, parent_award = [], None

                # Now create the award record for this award transaction
                summary_award = Award(**{
                    i[1]: i[0],
                    "parent_award": parent_award,
                    "awarding_agency": awarding_agency})
                created = [summary_award, ]
                created.extend(parent_created)

                if use_cache:
                    awards_cache.set(q_kwargs_fixed, summary_award)
                else:
                    summary_award.save()
                return created, summary_award

        raise ValueError(
            'Unable to find or create an award with the provided information: '
            'piid={}, fain={}, uri={}, parent_id={}, awarding_agency={}'.format(
                piid, fain, uri, parent_award_id, awarding_agency))

    class Meta:
        db_table = 'awards'


class Transaction(DataSourceTrackedModel):
    award = models.ForeignKey(Award, models.CASCADE, help_text="The award which this transaction is contained in")
    usaspending_unique_transaction_id = models.CharField(max_length=256, blank=True, null=True, help_text="If this record is legacy USASpending data, this is the unique transaction identifier from that system")
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE, help_text="The submission which created this record")
    type = models.CharField(max_length=5, verbose_name="Action Type", null=True, help_text="The type for this transaction. For example, A, B, C, D")
    type_description = models.TextField(blank=True, verbose_name="Action Type Description", null=True, help_text="The plain text description of the transaction type")
    period_of_performance_start_date = models.DateField(max_length=10, verbose_name="Period of Performance Start Date", null=True, help_text="The period of performance start date")
    period_of_performance_current_end_date = models.DateField(max_length=10, verbose_name="Period of Performance Current End Date", null=True, help_text="The current end date of the period of performance")
    action_date = models.DateField(max_length=10, verbose_name="Transaction Date", help_text="The date this transaction was actioned")
    action_type = models.CharField(max_length=1, blank=True, null=True, help_text="The type of transaction. For example, A, B, C, D")
    action_type_description = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, db_index=True, decimal_places=2, blank=True, null=True, help_text="The obligation of the federal government for this transaction")
    modification_number = models.CharField(max_length=50, blank=True, null=True, verbose_name="Modification Number", help_text="The modification number for this transaction")
    awarding_agency = models.ForeignKey(Agency, related_name='%(app_label)s_%(class)s_awarding_agency', null=True, help_text="The agency which awarded this transaction")
    funding_agency = models.ForeignKey(Agency, related_name='%(app_label)s_%(class)s_funding_agency', null=True, help_text="The agency which is funding this transaction")
    recipient = models.ForeignKey(LegalEntity, null=True, help_text="The recipient for this transaction")
    description = models.CharField(max_length=4000, null=True, help_text="The description of this transaction")
    place_of_performance = models.ForeignKey(Location, null=True, help_text="The location where the work on this transaction was performed")
    drv_award_transaction_usaspend = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_current_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True, help_text="The date this transaction was last modified")
    certified_date = models.DateField(blank=True, null=True, help_text="The date this transaction was certified")
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True, help_text="The date this transaction was created in the API")
    update_date = models.DateTimeField(auto_now=True, null=True, help_text="The last time this transaction was updated in the API")

    def __str__(self):
        return '%s award: %s' % (self.type_description, self.award)

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
            "action_type_description",
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

    class Meta:
        db_table = 'transaction'
        index_together = ['award', 'action_date']


class TransactionContract(DataSourceTrackedModel):
    transaction = models.OneToOneField(
        Transaction, on_delete=models.CASCADE,
        primary_key=True, related_name='contract_data', help_text="Non-specific transaction data, fields shared among both assistance and contract transactions")
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    piid = models.CharField(max_length=50, blank=True, help_text="The PIID of this transaction")
    parent_award_id = models.CharField(max_length=50, blank=True, null=True, verbose_name="Parent Award ID", help_text="The parent award id for this transaction. This is generally the piid of an IDV")
    cost_or_pricing_data = models.CharField(max_length=1, blank=True, null=True, help_text="")
    cost_or_pricing_data_description = models.TextField(blank=True, null=True)
    type_of_contract_pricing = models.CharField(max_length=2, default="UN", blank=True, null=True, verbose_name="Type of Contract Pricing", help_text="The type of contract pricing data, as a code")
    type_of_contract_pricing_description = models.TextField(blank=True, null=True, verbose_name="Type of Contract Pricing Description", help_text="A plain text description of the type of contract pricing data")
    naics = models.CharField(max_length=6, blank=True, null=True, verbose_name="NAICS", help_text="Specified which industry the work for this transaction falls into. A 6-digit code")
    naics_description = models.CharField(max_length=150, blank=True, null=True, verbose_name="NAICS Description", help_text="A plain text description of the NAICS code")
    period_of_performance_potential_end_date = models.DateField(max_length=10, verbose_name="Period of Performance Potential End Date", null=True, help_text="The potential end date of the period of performance")
    ordering_period_end_date = models.CharField(max_length=8, blank=True, null=True, help_text="The end date for the ordering period")
    current_total_value_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True, help_text="The current value of the award")
    potential_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True, verbose_name="Potential Total Value of Award", help_text="The potential total value of the award")
    referenced_idv_agency_identifier = models.CharField(max_length=4, blank=True, null=True, help_text="The agency identifier of the agency on the IDV")
    idv_type = models.CharField(max_length=1, blank=True, null=True, verbose_name="IDV Type", help_text="The IDV type code")
    idv_type_description = models.TextField(null=True, blank=True)
    multiple_or_single_award_idv = models.CharField(max_length=1, blank=True, null=True, help_text="Specifies whether the IDV is a single more multiple award vehicle")
    multiple_or_single_award_idv_description = models.TextField(null=True, blank=True)
    type_of_idc = models.CharField(max_length=1, blank=True, null=True, verbose_name="Type of IDC", help_text="Code representing the type of IDC")
    type_of_idc_description = models.TextField(null=True, blank=True)
    a76_fair_act_action = models.CharField(max_length=1, blank=True, null=True, verbose_name="A-76 FAIR Act Action", help_text="A-76 FAIR act action")
    dod_claimant_program_code = models.CharField(max_length=3, blank=True, null=True)
    clinger_cohen_act_planning = models.CharField(max_length=1, blank=True, null=True)
    commercial_item_acquisition_procedures = models.CharField(max_length=1, blank=True, null=True)
    commercial_item_acquisition_procedures_description = models.TextField(blank=True, null=True)
    commercial_item_test_program = models.CharField(max_length=1, blank=True, null=True)
    consolidated_contract = models.CharField(max_length=1, blank=True, null=True)
    contingency_humanitarian_or_peacekeeping_operation = models.CharField(max_length=1, blank=True, null=True)
    contingency_humanitarian_or_peacekeeping_operation_description = models.TextField(blank=True, null=True)
    contract_bundling = models.CharField(max_length=1, blank=True, null=True)
    contract_bundling_description = models.TextField(blank=True, null=True)
    contract_financing = models.CharField(max_length=1, blank=True, null=True)
    contract_financing_description = models.TextField(blank=True, null=True)
    contracting_officers_determination_of_business_size = models.CharField(max_length=1, blank=True, null=True)
    cost_accounting_standards = models.CharField(max_length=1, blank=True, null=True)
    cost_accounting_standards_description = models.TextField(blank=True, null=True)
    country_of_product_or_service_origin = models.CharField(max_length=3, blank=True, null=True)
    davis_bacon_act = models.CharField(max_length=1, blank=True, null=True)
    davis_bacon_act_description = models.TextField(null=True, blank=True)
    evaluated_preference = models.CharField(max_length=6, blank=True, null=True)
    evaluated_preference_description = models.TextField(null=True, blank=True)
    extent_competed = models.CharField(max_length=3, blank=True, null=True)
    extent_competed_description = models.TextField(null=True, blank=True)
    fed_biz_opps = models.CharField(max_length=1, blank=True, null=True)
    fed_biz_opps_description = models.TextField(null=True, blank=True)
    foreign_funding = models.CharField(max_length=1, blank=True, null=True)
    foreign_funding_description = models.TextField(null=True, blank=True)
    gfe_gfp = models.CharField(max_length=1, blank=True, null=True)
    information_technology_commercial_item_category = models.CharField(max_length=1, blank=True, null=True)
    information_technology_commercial_item_category_description = models.TextField(null=True, blank=True)
    interagency_contracting_authority = models.CharField(max_length=1, blank=True, null=True)
    interagency_contracting_authority_description = models.TextField(null=True, blank=True)
    local_area_set_aside = models.CharField(max_length=1, blank=True, null=True)
    major_program = models.CharField(max_length=100, blank=True, null=True)
    purchase_card_as_payment_method = models.CharField(max_length=1, blank=True, null=True)
    multi_year_contract = models.CharField(max_length=1, blank=True, null=True)
    national_interest_action = models.CharField(max_length=20, blank=True, null=True)
    national_interest_action_description = models.TextField(null=True, blank=True)
    number_of_actions = models.CharField(max_length=6, blank=True, null=True)
    number_of_offers_received = models.CharField(max_length=3, blank=True, null=True)
    other_statutory_authority = models.CharField(max_length=1000, blank=True, null=True)
    performance_based_service_acquisition = models.CharField(max_length=1, blank=True, null=True)
    performance_based_service_acquisition_description = models.TextField(null=True, blank=True)
    place_of_manufacture = models.CharField(max_length=1, blank=True, null=True)
    place_of_manufacture_description = models.TextField(null=True, blank=True)
    price_evaluation_adjustment_preference_percent_difference = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    product_or_service_code = models.CharField(max_length=4, blank=True, null=True)
    program_acronym = models.CharField(max_length=25, blank=True, null=True)
    other_than_full_and_open_competition = models.CharField(max_length=3, blank=True, null=True)
    recovered_materials_sustainability = models.CharField(max_length=1, blank=True, null=True)
    recovered_materials_sustainability_description = models.TextField(null=True, blank=True)
    research = models.CharField(max_length=3, blank=True, null=True)
    research_description = models.TextField(null=True, blank=True)
    sea_transportation = models.CharField(max_length=1, blank=True, null=True)
    sea_transportation_description = models.TextField(null=True, blank=True)
    service_contract_act = models.CharField(max_length=1, blank=True, null=True)
    service_contract_act_description = models.TextField(null=True, blank=True)
    small_business_competitiveness_demonstration_program = models.CharField(max_length=1, blank=True, null=True)
    solicitation_identifier = models.CharField(max_length=25, blank=True, null=True, verbose_name="Solicitation ID")
    solicitation_procedures = models.CharField(max_length=5, blank=True, null=True)
    solicitation_procedures_description = models.TextField(null=True, blank=True)
    fair_opportunity_limited_sources = models.CharField(max_length=50, blank=True, null=True)
    fair_opportunity_limited_sources_description = models.TextField(null=True, blank=True)
    subcontracting_plan = models.CharField(max_length=1, blank=True, null=True)
    subcontracting_plan_description = models.TextField(null=True, blank=True)
    program_system_or_equipment_code = models.CharField(max_length=4, blank=True, null=True)
    type_set_aside = models.CharField(max_length=10, blank=True, null=True, verbose_name="Type Set Aside")
    type_set_aside_description = models.TextField(null=True, blank=True)
    epa_designated_product = models.CharField(max_length=1, blank=True, null=True)
    epa_designated_product_description = models.TextField(null=True, blank=True)
    walsh_healey_act = models.CharField(max_length=1, blank=True, null=True, help_text="Denotes whether this transaction is subject to the Walsh-Healey act")
    transaction_number = models.CharField(max_length=6, blank=True, null=True, help_text="The transaction number for this transaction")
    referenced_idv_modification_number = models.CharField(max_length=25, blank=True, null=True, help_text="The modification number for the referenced IDV")
    rec_flag = models.CharField(max_length=1, blank=True, null=True, help_text="The rec flag")
    drv_parent_award_awarding_agency_code = models.CharField(max_length=4, blank=True, null=True)
    drv_current_aggregated_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_current_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_award_idv_amount_total_estimate = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_aggregated_award_idv_amount_total_estimate = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_aggregated_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_potential_total_value_of_award = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True, help_text="The date this record was created in the API")
    update_date = models.DateTimeField(auto_now=True, null=True, help_text="The last time this record was updated in the API")
    last_modified_date = models.DateField(blank=True, null=True, help_text="The last time this transaction was modified")
    certified_date = models.DateField(blank=True, null=True, help_text="The date this record was certified")
    reporting_period_start = models.DateField(blank=True, null=True, help_text="The date marking the start of the reporting period")
    reporting_period_end = models.DateField(blank=True, null=True, help_text="The date marking the end of the reporting period")

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
    cfda = models.ForeignKey(CFDAProgram, models.DO_NOTHING, null=True)
    business_funds_indicator = models.CharField(max_length=3)
    business_funds_indicator_description = models.TextField(blank=True, null=True)
    non_federal_funding_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    total_funding_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    record_type = models.IntegerField()
    record_type_description = models.TextField(null=True, blank=True)
    correction_late_delete_indicator = models.CharField(max_length=1, blank=True, null=True)
    correction_late_delete_indicator_description = models.TextField(blank=True, null=True)
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
            "cfda",
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
