import warnings

from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models import Q

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.models import (
    Agency, Cfda, LegalEntity, Location, ObjectClass, RefProgramActivity)
from usaspending_api.common.models import DataSourceTrackedModel
from usaspending_api.common.helpers import fy
from django.core.cache import caches, CacheKeyWarning

from django_pgviews import view as pg


warnings.simplefilter("ignore", CacheKeyWarning)


class FinancialAccountsByAwards(DataSourceTrackedModel):
    financial_accounts_by_awards_id = models.AutoField(primary_key=True)
    treasury_account = models.ForeignKey(TreasuryAppropriationAccount, models.CASCADE, null=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    award = models.ForeignKey('awards.Award', models.CASCADE, null=True, related_name="financial_set")
    program_activity = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, null=True, db_index=True)
    object_class = models.ForeignKey(ObjectClass, models.DO_NOTHING, null=True, db_index=True)
    piid = models.TextField(blank=True, null=True)
    parent_award_id = models.TextField(blank=True, null=True)
    fain = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True)
    award_type = models.TextField(blank=True, null=True)
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                blank=True, null=True)
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                blank=True, null=True)
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                      blank=True, null=True)
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                       blank=True, null=True)
    ussgl490100_delivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=21, decimal_places=2,
                                                                              blank=True, null=True)
    ussgl490100_delivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                              blank=True, null=True)
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                    blank=True, null=True)
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                      blank=True, null=True)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                    blank=True, null=True)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                    blank=True, null=True)
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                       blank=True, null=True)
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                      blank=True, null=True)
    ussgl490200_delivered_orders_obligations_paid_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True,
                                                                            null=True)
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = models.DecimalField(max_digits=21, decimal_places=2,
                                                                               blank=True, null=True)
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                               blank=True, null=True)
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                    blank=True, null=True)
    obligations_undelivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True,
                                                                          null=True)
    obligations_delivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True,
                                                                        null=True)
    obligations_delivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True,
                                                                        null=True)
    gross_outlays_undelivered_orders_prepaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2,
                                                                             blank=True, null=True)
    gross_outlays_undelivered_orders_prepaid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                             blank=True, null=True)
    gross_outlays_delivered_orders_paid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True,
                                                                        null=True)
    gross_outlay_amount_by_award_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    gross_outlay_amount_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    obligations_incurred_total_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True,
                                                                  null=True)
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                       blank=True, null=True)
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                       blank=True, null=True)
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                       blank=True, null=True)
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                      blank=True, null=True)
    deobligations_recoveries_refunds_of_prior_year_by_award_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                                      blank=True, null=True)
    obligations_undelivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=21, decimal_places=2,
                                                                          blank=True, null=True)
    gross_outlays_delivered_orders_paid_total_cpe = models.DecimalField(max_digits=21, decimal_places=2,
                                                                        blank=True, null=True)
    drv_award_id_field_type = models.TextField(blank=True, null=True)
    drv_obligations_incurred_total_by_award = models.DecimalField(max_digits=21, decimal_places=2, blank=True,
                                                                  null=True)
    transaction_obligated_amount = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

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

    type = models.TextField(db_index=True, verbose_name="Award Type", null=True,
                            help_text="The mechanism used to distribute funding. The federal government can distribute "
                                      "funding in several forms. These award types include contracts, grants, loans, "
                                      "and direct payments.")
    type_description = models.TextField(verbose_name="Award Type Description", blank=True, null=True,
                                        help_text="The plain text description of the type of the award")
    category = models.TextField(db_index=True, verbose_name="Category", null=True,
                                help_text="A field that generalizes the award's type.")
    piid = models.TextField(db_index=True, blank=True, null=True,
                            help_text="Procurement Instrument Identifier - A unique identifier assigned to a federal "
                                      "contract, purchase order, basic ordering agreement, basic agreement, and "
                                      "blanket purchase agreement. It is used to track the contract, and any "
                                      "modifications or transactions related to it. After October 2017, it is "
                                      "between 13 and 17 digits, both letters and numbers.")
    parent_award = models.ForeignKey('awards.Award', related_name='child_award', null=True,
                                     help_text="The parent award, if applicable")
    fain = models.TextField(db_index=True, blank=True, null=True,
                            help_text="An identification code assigned to each financial assistance award tracking "
                                      "purposes. The FAIN is tied to that award (and all future modifications to that "
                                      "award) throughout the award’s life. Each FAIN is assigned by an agency. Within "
                                      "an agency, FAIN are unique: each new award must be issued a new FAIN. FAIN "
                                      "stands for Federal Award Identification Number, though the digits are letters, "
                                      "not numbers.")
    uri = models.TextField(db_index=True, blank=True, null=True, help_text="The uri of the award")
    total_obligation = models.DecimalField(max_digits=15, db_index=True, decimal_places=2, null=True,
                                           verbose_name="Total Obligated",
                                           help_text="The amount of money the government is obligated to pay for the "
                                                     "award")
    total_outlay = models.DecimalField(max_digits=15, db_index=True, decimal_places=2, null=True,
                                       help_text="The total amount of money paid out for this award")
    awarding_agency = models.ForeignKey(Agency, related_name='+', null=True,
                                        help_text="The awarding agency for the award", db_index=True)
    funding_agency = models.ForeignKey(Agency, related_name='+', null=True,
                                       help_text="The funding agency for the award", db_index=True)
    date_signed = models.DateField(null=True, db_index=True, verbose_name="Award Date",
                                   help_text="The date the award was signed")
    recipient = models.ForeignKey(LegalEntity, null=True, help_text="The recipient of the award", db_index=True)
    description = models.TextField(null=True, verbose_name="Award Description", help_text="A description of the award",
                                   db_index=True)
    period_of_performance_start_date = models.DateField(null=True, db_index=True, verbose_name="Start Date",
                                                        help_text="The start date for the period of performance")
    period_of_performance_current_end_date = models.DateField(null=True, db_index=True, verbose_name="End Date",
                                                              help_text="The current, not original, period of "
                                                                        "performance end date")
    place_of_performance = models.ForeignKey(Location, null=True,
                                             help_text="The principal place of business, where the majority of the "
                                                       "work is performed. For example, in a manufacturing contract, "
                                                       "this would be the main plant where items are produced.",
                                             db_index=True)
    potential_total_value_of_award = models.DecimalField(max_digits=20, db_index=True, decimal_places=2, blank=True,
                                                         null=True, verbose_name="Potential Total Value of Award",
                                                         help_text="The sum of the potential_value_of_award from "
                                                                   "associated transactions")
    base_and_all_options_value = models.DecimalField(max_digits=20, db_index=True, decimal_places=2, blank=True,
                                                     null=True, verbose_name="Base and All Options Value",
                                                     help_text="The sum of the base_and_all_options_value from "
                                                               "associated transactions")
    last_modified_date = models.DateField(blank=True, null=True, help_text="The date this award was last modified")
    certified_date = models.DateField(blank=True, null=True, help_text="The date this record was certified")
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True,
                                       help_text="The date this record was created in the API")
    update_date = models.DateTimeField(auto_now=True, null=True,
                                       help_text="The last time this record was updated in the API")
    latest_transaction = models.ForeignKey("awards.TransactionNormalized", related_name="latest_for_award", null=True,
                                           help_text="The latest transaction by action_date associated with this award")

    # Subaward aggregates
    total_subaward_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True)
    subaward_count = models.IntegerField(default=0)

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

    def __str__(self):
        return '%s piid: %s fain: %s uri: %s' % (self.type_description, self.piid, self.fain, self.uri)

    @staticmethod
    def get_or_create_summary_award(awarding_agency=None, piid=None, fain=None,
                                    uri=None, parent_award_id=None, use_cache=False, save=True,
                                    agency_toptier_map=None):
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
        # has both a fain and a uri, include both.
        try:
            lookup_kwargs = {"awarding_agency": awarding_agency, "parent_award": None}
            for i in [(piid, "piid"), (fain, "fain"), (uri, "uri")]:
                lookup_kwargs[i[1]] = i[0]
                if parent_award_id and i[0]:
                    # parent_award__piid, parent_award__fain, parent_award__uri
                    lookup_kwargs["parent_award__" + i[1]] = parent_award_id
                    if "parent_award" in lookup_kwargs:
                        del lookup_kwargs["parent_award"]

            # Look for an existing award record
            summary_award = Award.objects \
                .filter(Q(**lookup_kwargs)) \
                .filter(awarding_agency=awarding_agency) \
                .first()
            if (summary_award is None and
                    awarding_agency is not None and
                    awarding_agency.toptier_agency.name != awarding_agency.subtier_agency.name):
                # No award match found when searching by award id info +
                # awarding subtier agency. Relax the awarding agency
                # critera to just the toptier agency instead of the subtier
                # agency and try the search again.
                if agency_toptier_map:
                    awarding_agency_toptier = agency_toptier_map[awarding_agency.toptier_agency.cgac_code]
                else:
                    awarding_agency_toptier = Agency.get_by_toptier(
                        awarding_agency.toptier_agency.cgac_code)

                summary_award = Award.objects \
                    .filter(Q(**lookup_kwargs)) \
                    .filter(awarding_agency=awarding_agency_toptier) \
                    .first()

            if summary_award:
                return [], summary_award

            # We weren't able to match, so create a new award record.
            if parent_award_id:
                # If parent award id was supplied, recursively get/create
                # an award record for it
                parent_q_kwargs = {'awarding_agency': awarding_agency}
                for i in [(piid, "piid"), (fain, "fain"), (uri, "uri")]:
                    parent_q_kwargs[i[1]] = parent_award_id if i[0] else None
                parent_created, parent_award = Award.get_or_create_summary_award(**parent_q_kwargs)
            else:
                parent_created, parent_award = [], None

            # Now create the award record for this award transaction
            create_kwargs = {"awarding_agency": awarding_agency, "parent_award": parent_award}
            for i in [(piid, "piid"), (fain, "fain"), (uri, "uri")]:
                create_kwargs[i[1]] = i[0]
            summary_award = Award(**create_kwargs)
            created = [summary_award, ]
            created.extend(parent_created)

            if save:
                summary_award.save()

            return created, summary_award

        # Do not use bare except
        except ValueError:
            raise ValueError(
                'Unable to find or create an award with the provided information: '
                'piid={}, fain={}, uri={}, parent_id={}, awarding_agency={}'.format(
                    piid, fain, uri, parent_award_id, awarding_agency))

    class Meta:
        db_table = 'awards'


class TransactionNormalized(models.Model):
    award = models.ForeignKey(Award, models.CASCADE, help_text="The award which this transaction is contained in")
    usaspending_unique_transaction_id = models.TextField(blank=True, null=True,
                                                         help_text="If this record is legacy USASpending data, this is "
                                                                   "the unique transaction identifier from that system")
    type = models.TextField(verbose_name="Action Type", null=True,
                            help_text="The type for this transaction. For example, A, B, C, D", db_index=True)
    type_description = models.TextField(blank=True, verbose_name="Action Type Description", null=True,
                                        help_text="The plain text description of the transaction type")
    period_of_performance_start_date = models.DateField(verbose_name="Period of Performance Start Date", null=True,
                                                        help_text="The period of performance start date")
    period_of_performance_current_end_date = models.DateField(verbose_name="Period of Performance Current End Date",
                                                              null=True,
                                                              help_text="The current end date of the period of "
                                                                        "performance")
    action_date = models.DateField(verbose_name="Transaction Date", help_text="The date this transaction was actioned",
                                   db_index=True)
    action_type = models.TextField(blank=True, null=True, help_text="The type of transaction. For example, A, B, C, D")
    action_type_description = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(max_digits=20, db_index=True, decimal_places=2, blank=True,
                                                    null=True,
                                                    help_text="The obligation of the federal government for this "
                                                              "transaction")
    modification_number = models.TextField(blank=True, null=True, verbose_name="Modification Number",
                                           help_text="The modification number for this transaction")
    awarding_agency = models.ForeignKey(Agency, related_name='%(app_label)s_%(class)s_awarding_agency', null=True,
                                        help_text="The agency which awarded this transaction")
    funding_agency = models.ForeignKey(Agency, related_name='%(app_label)s_%(class)s_funding_agency', null=True,
                                       help_text="The agency which is funding this transaction")
    recipient = models.ForeignKey(LegalEntity, null=True, help_text="The recipient for this transaction")
    description = models.TextField(null=True, help_text="The description of this transaction")
    place_of_performance = models.ForeignKey(Location, null=True,
                                             help_text="The location where the work on this transaction was performed")
    drv_award_transaction_usaspend = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    drv_current_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True,
                                                                          null=True)
    drv_potential_total_award_value_amount_adjustment = models.DecimalField(max_digits=20, decimal_places=2, blank=True,
                                                                            null=True)
    last_modified_date = models.DateField(blank=True, null=True,
                                          help_text="The date this transaction was last modified")
    certified_date = models.DateField(blank=True, null=True, help_text="The date this transaction was certified")
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True,
                                       help_text="The date this transaction was created in the API")
    update_date = models.DateTimeField(auto_now=True, null=True,
                                       help_text="The last time this transaction was updated in the API")
    fiscal_year = models.IntegerField(blank=True, null=True, help_text="Fiscal Year calculated based on Action Date")

    def __str__(self):
        return '%s award: %s' % (self.type_description, self.award)

    def newer_than(self, dct):
        """Compares age of this instance to a Python dictionary

        Determines the age of each by last_modified_date, if set,
        otherwise action_date.
        Returns `False` if either side lacks a date completely.
        """

        my_date = self.last_modified_date
        their_date = dct.get('last_modified_date')
        if my_date and their_date:
            return my_date > their_date
        else:
            return False

    @classmethod
    def get_or_create_transaction(cls, **kwargs):
        """Gets and updates, or creates, a Transaction

        Transactions must be unique on Award, Awarding Agency, and Mod Number
        """
        transaction = cls.objects.filter(
            award=kwargs.get('award'),
            modification_number=kwargs.get('modification_number')
        ).order_by('-update_date').first()
        if transaction:
            if not transaction.newer_than(kwargs):
                for (k, v) in kwargs.items():

                    setattr(transaction, k, v)

            return transaction
        return cls(**kwargs)

    def save(self, *args, **kwargs):
        self.fiscal_year = fy(self.action_date)
        super().save(*args, **kwargs)

    class Meta:
        db_table = 'transaction_normalized'
        index_together = ['award', 'action_date']


class TransactionFPDS(models.Model):
    transaction = models.OneToOneField(
        TransactionNormalized, on_delete=models.CASCADE,
        primary_key=True, related_name='contract_data',
        help_text="Non-specific transaction data, fields shared among both assistance and contract transactions")
    detached_award_procurement_id = models.TextField(blank=True, null=True, db_index=True)
    detached_award_proc_unique = models.TextField(unique=True, null=False)
    piid = models.TextField(blank=True, null=True, db_index=True)
    agency_id = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_n = models.TextField(blank=True, null=True)
    awarding_agency_code = models.TextField(blank=True, null=True)
    awarding_agency_name = models.TextField(blank=True, null=True)
    parent_award_id = models.TextField(blank=True, null=True)
    award_modification_amendme = models.TextField(blank=True, null=True)
    type_of_contract_pricing = models.TextField(blank=True, null=True, db_index=True)
    type_of_contract_pric_desc = models.TextField(blank=True, null=True)
    contract_award_type = models.TextField(blank=True, null=True)
    contract_award_type_desc = models.TextField(blank=True, null=True)
    naics = models.TextField(blank=True, null=True, db_index=True)
    naics_description = models.TextField(blank=True, null=True)
    awardee_or_recipient_uniqu = models.TextField(blank=True, null=True)
    ultimate_parent_legal_enti = models.TextField(blank=True, null=True)
    ultimate_parent_unique_ide = models.TextField(blank=True, null=True)
    award_description = models.TextField(blank=True, null=True)
    place_of_performance_zip4a = models.TextField(blank=True, null=True)
    place_of_perform_city_name = models.TextField(blank=True, null=True)
    place_of_perform_county_na = models.TextField(blank=True, null=True)
    place_of_performance_congr = models.TextField(blank=True, null=True)
    awardee_or_recipient_legal = models.TextField(blank=True, null=True)
    legal_entity_city_name = models.TextField(blank=True, null=True)
    legal_entity_state_code = models.TextField(blank=True, null=True)
    legal_entity_state_descrip = models.TextField(blank=True, null=True)
    legal_entity_zip4 = models.TextField(blank=True, null=True)
    legal_entity_congressional = models.TextField(blank=True, null=True)
    legal_entity_address_line1 = models.TextField(blank=True, null=True)
    legal_entity_address_line2 = models.TextField(blank=True, null=True)
    legal_entity_address_line3 = models.TextField(blank=True, null=True)
    legal_entity_country_code = models.TextField(blank=True, null=True)
    legal_entity_country_name = models.TextField(blank=True, null=True)
    period_of_performance_star = models.TextField(blank=True, null=True)
    period_of_performance_curr = models.TextField(blank=True, null=True)
    period_of_perf_potential_e = models.TextField(blank=True, null=True)
    ordering_period_end_date = models.TextField(blank=True, null=True)
    action_date = models.TextField(blank=True, null=True)
    action_type = models.TextField(blank=True, null=True)
    action_type_description = models.TextField(blank=True, null=True)
    federal_action_obligation = models.TextField(blank=True, null=True)
    current_total_value_award = models.TextField(blank=True, null=True)
    potential_total_value_awar = models.TextField(blank=True, null=True)
    base_exercised_options_val = models.TextField(blank=True, null=True)
    base_and_all_options_value = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_na = models.TextField(blank=True, null=True)
    funding_office_code = models.TextField(blank=True, null=True)
    funding_office_name = models.TextField(blank=True, null=True)
    awarding_office_code = models.TextField(blank=True, null=True)
    awarding_office_name = models.TextField(blank=True, null=True)
    referenced_idv_agency_iden = models.TextField(blank=True, null=True)
    referenced_idv_agency_desc = models.TextField(blank=True, null=True)
    funding_agency_code = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    place_of_performance_locat = models.TextField(blank=True, null=True)
    place_of_performance_state = models.TextField(blank=True, null=True)
    place_of_perfor_state_desc = models.TextField(blank=True, null=True)
    place_of_perform_country_c = models.TextField(blank=True, null=True)
    place_of_perf_country_desc = models.TextField(blank=True, null=True)
    idv_type = models.TextField(blank=True, null=True)
    idv_type_description = models.TextField(blank=True, null=True)
    award_or_idv_flag = models.TextField(blank=True, null=True)
    referenced_idv_type = models.TextField(blank=True, null=True)
    referenced_idv_type_desc = models.TextField(blank=True, null=True)
    vendor_doing_as_business_n = models.TextField(blank=True, null=True)
    vendor_phone_number = models.TextField(blank=True, null=True)
    vendor_fax_number = models.TextField(blank=True, null=True)
    multiple_or_single_award_i = models.TextField(blank=True, null=True)
    multiple_or_single_aw_desc = models.TextField(blank=True, null=True)
    referenced_mult_or_single = models.TextField(blank=True, null=True)
    referenced_mult_or_si_desc = models.TextField(blank=True, null=True)
    type_of_idc = models.TextField(blank=True, null=True)
    type_of_idc_description = models.TextField(blank=True, null=True)
    a_76_fair_act_action = models.TextField(blank=True, null=True)
    a_76_fair_act_action_desc = models.TextField(blank=True, null=True)
    dod_claimant_program_code = models.TextField(blank=True, null=True)
    dod_claimant_prog_cod_desc = models.TextField(blank=True, null=True)
    clinger_cohen_act_planning = models.TextField(blank=True, null=True)
    clinger_cohen_act_pla_desc = models.TextField(blank=True, null=True)
    commercial_item_acquisitio = models.TextField(blank=True, null=True)
    commercial_item_acqui_desc = models.TextField(blank=True, null=True)
    commercial_item_test_progr = models.TextField(blank=True, null=True)
    commercial_item_test_desc = models.TextField(blank=True, null=True)
    consolidated_contract = models.TextField(blank=True, null=True)
    consolidated_contract_desc = models.TextField(blank=True, null=True)
    contingency_humanitarian_o = models.TextField(blank=True, null=True)
    contingency_humanitar_desc = models.TextField(blank=True, null=True)
    contract_bundling = models.TextField(blank=True, null=True)
    contract_bundling_descrip = models.TextField(blank=True, null=True)
    contract_financing = models.TextField(blank=True, null=True)
    contract_financing_descrip = models.TextField(blank=True, null=True)
    contracting_officers_deter = models.TextField(blank=True, null=True)
    contracting_officers_desc = models.TextField(blank=True, null=True)
    cost_accounting_standards = models.TextField(blank=True, null=True)
    cost_accounting_stand_desc = models.TextField(blank=True, null=True)
    cost_or_pricing_data = models.TextField(blank=True, null=True)
    cost_or_pricing_data_desc = models.TextField(blank=True, null=True)
    country_of_product_or_serv = models.TextField(blank=True, null=True)
    country_of_product_or_desc = models.TextField(blank=True, null=True)
    davis_bacon_act = models.TextField(blank=True, null=True)
    davis_bacon_act_descrip = models.TextField(blank=True, null=True)
    evaluated_preference = models.TextField(blank=True, null=True)
    evaluated_preference_desc = models.TextField(blank=True, null=True)
    extent_competed = models.TextField(blank=True, null=True, db_index=True)
    extent_compete_description = models.TextField(blank=True, null=True)
    fed_biz_opps = models.TextField(blank=True, null=True)
    fed_biz_opps_description = models.TextField(blank=True, null=True)
    foreign_funding = models.TextField(blank=True, null=True)
    foreign_funding_desc = models.TextField(blank=True, null=True)
    government_furnished_equip = models.TextField(blank=True, null=True)
    government_furnished_desc = models.TextField(blank=True, null=True)
    information_technology_com = models.TextField(blank=True, null=True)
    information_technolog_desc = models.TextField(blank=True, null=True)
    interagency_contracting_au = models.TextField(blank=True, null=True)
    interagency_contract_desc = models.TextField(blank=True, null=True)
    local_area_set_aside = models.TextField(blank=True, null=True)
    local_area_set_aside_desc = models.TextField(blank=True, null=True)
    major_program = models.TextField(blank=True, null=True)
    purchase_card_as_payment_m = models.TextField(blank=True, null=True)
    purchase_card_as_paym_desc = models.TextField(blank=True, null=True)
    multi_year_contract = models.TextField(blank=True, null=True)
    multi_year_contract_desc = models.TextField(blank=True, null=True)
    national_interest_action = models.TextField(blank=True, null=True)
    national_interest_desc = models.TextField(blank=True, null=True)
    number_of_actions = models.TextField(blank=True, null=True)
    number_of_offers_received = models.TextField(blank=True, null=True)
    other_statutory_authority = models.TextField(blank=True, null=True)
    performance_based_service = models.TextField(blank=True, null=True)
    performance_based_se_desc = models.TextField(blank=True, null=True)
    place_of_manufacture = models.TextField(blank=True, null=True)
    place_of_manufacture_desc = models.TextField(blank=True, null=True)
    price_evaluation_adjustmen = models.TextField(blank=True, null=True)
    product_or_service_code = models.TextField(blank=True, null=True, db_index=True)
    product_or_service_co_desc = models.TextField(blank=True, null=True)
    program_acronym = models.TextField(blank=True, null=True)
    other_than_full_and_open_c = models.TextField(blank=True, null=True)
    other_than_full_and_o_desc = models.TextField(blank=True, null=True)
    recovered_materials_sustai = models.TextField(blank=True, null=True)
    recovered_materials_s_desc = models.TextField(blank=True, null=True)
    research = models.TextField(blank=True, null=True)
    research_description = models.TextField(blank=True, null=True)
    sea_transportation = models.TextField(blank=True, null=True)
    sea_transportation_desc = models.TextField(blank=True, null=True)
    service_contract_act = models.TextField(blank=True, null=True)
    service_contract_act_desc = models.TextField(blank=True, null=True)
    small_business_competitive = models.TextField(blank=True, null=True)
    solicitation_identifier = models.TextField(blank=True, null=True)
    solicitation_procedures = models.TextField(blank=True, null=True)
    solicitation_procedur_desc = models.TextField(blank=True, null=True)
    fair_opportunity_limited_s = models.TextField(blank=True, null=True)
    fair_opportunity_limi_desc = models.TextField(blank=True, null=True)
    subcontracting_plan = models.TextField(blank=True, null=True)
    subcontracting_plan_desc = models.TextField(blank=True, null=True)
    program_system_or_equipmen = models.TextField(blank=True, null=True)
    program_system_or_equ_desc = models.TextField(blank=True, null=True)
    type_set_aside = models.TextField(blank=True, null=True, db_index=True)
    type_set_aside_description = models.TextField(blank=True, null=True)
    epa_designated_product = models.TextField(blank=True, null=True)
    epa_designated_produc_desc = models.TextField(blank=True, null=True)
    walsh_healey_act = models.TextField(blank=True, null=True)
    walsh_healey_act_descrip = models.TextField(blank=True, null=True)
    transaction_number = models.TextField(blank=True, null=True)
    sam_exception = models.TextField(blank=True, null=True)
    sam_exception_description = models.TextField(blank=True, null=True)
    city_local_government = models.TextField(blank=True, null=True)
    county_local_government = models.TextField(blank=True, null=True)
    inter_municipal_local_gove = models.TextField(blank=True, null=True)
    local_government_owned = models.TextField(blank=True, null=True)
    municipality_local_governm = models.TextField(blank=True, null=True)
    school_district_local_gove = models.TextField(blank=True, null=True)
    township_local_government = models.TextField(blank=True, null=True)
    us_state_government = models.TextField(blank=True, null=True)
    us_federal_government = models.TextField(blank=True, null=True)
    federal_agency = models.TextField(blank=True, null=True)
    federally_funded_research = models.TextField(blank=True, null=True)
    us_tribal_government = models.TextField(blank=True, null=True)
    foreign_government = models.TextField(blank=True, null=True)
    community_developed_corpor = models.TextField(blank=True, null=True)
    labor_surplus_area_firm = models.TextField(blank=True, null=True)
    corporate_entity_not_tax_e = models.TextField(blank=True, null=True)
    corporate_entity_tax_exemp = models.TextField(blank=True, null=True)
    partnership_or_limited_lia = models.TextField(blank=True, null=True)
    sole_proprietorship = models.TextField(blank=True, null=True)
    small_agricultural_coopera = models.TextField(blank=True, null=True)
    international_organization = models.TextField(blank=True, null=True)
    us_government_entity = models.TextField(blank=True, null=True)
    emerging_small_business = models.TextField(blank=True, null=True)
    c8a_program_participant = models.TextField(blank=True, null=True)
    sba_certified_8_a_joint_ve = models.TextField(blank=True, null=True)
    dot_certified_disadvantage = models.TextField(blank=True, null=True)
    self_certified_small_disad = models.TextField(blank=True, null=True)
    historically_underutilized = models.TextField(blank=True, null=True)
    small_disadvantaged_busine = models.TextField(blank=True, null=True)
    the_ability_one_program = models.TextField(blank=True, null=True)
    historically_black_college = models.TextField(blank=True, null=True)
    c1862_land_grant_college = models.TextField(blank=True, null=True)
    c1890_land_grant_college = models.TextField(blank=True, null=True)
    c1994_land_grant_college = models.TextField(blank=True, null=True)
    minority_institution = models.TextField(blank=True, null=True)
    private_university_or_coll = models.TextField(blank=True, null=True)
    school_of_forestry = models.TextField(blank=True, null=True)
    state_controlled_instituti = models.TextField(blank=True, null=True)
    tribal_college = models.TextField(blank=True, null=True)
    veterinary_college = models.TextField(blank=True, null=True)
    educational_institution = models.TextField(blank=True, null=True)
    alaskan_native_servicing_i = models.TextField(blank=True, null=True)
    community_development_corp = models.TextField(blank=True, null=True)
    native_hawaiian_servicing = models.TextField(blank=True, null=True)
    domestic_shelter = models.TextField(blank=True, null=True)
    manufacturer_of_goods = models.TextField(blank=True, null=True)
    hospital_flag = models.TextField(blank=True, null=True)
    veterinary_hospital = models.TextField(blank=True, null=True)
    hispanic_servicing_institu = models.TextField(blank=True, null=True)
    foundation = models.TextField(blank=True, null=True)
    woman_owned_business = models.TextField(blank=True, null=True)
    minority_owned_business = models.TextField(blank=True, null=True)
    women_owned_small_business = models.TextField(blank=True, null=True)
    economically_disadvantaged = models.TextField(blank=True, null=True)
    joint_venture_women_owned = models.TextField(blank=True, null=True)
    joint_venture_economically = models.TextField(blank=True, null=True)
    veteran_owned_business = models.TextField(blank=True, null=True)
    service_disabled_veteran_o = models.TextField(blank=True, null=True)
    contracts = models.TextField(blank=True, null=True)
    grants = models.TextField(blank=True, null=True)
    receives_contracts_and_gra = models.TextField(blank=True, null=True)
    airport_authority = models.TextField(blank=True, null=True)
    council_of_governments = models.TextField(blank=True, null=True)
    housing_authorities_public = models.TextField(blank=True, null=True)
    interstate_entity = models.TextField(blank=True, null=True)
    planning_commission = models.TextField(blank=True, null=True)
    port_authority = models.TextField(blank=True, null=True)
    transit_authority = models.TextField(blank=True, null=True)
    subchapter_s_corporation = models.TextField(blank=True, null=True)
    limited_liability_corporat = models.TextField(blank=True, null=True)
    foreign_owned_and_located = models.TextField(blank=True, null=True)
    american_indian_owned_busi = models.TextField(blank=True, null=True)
    alaskan_native_owned_corpo = models.TextField(blank=True, null=True)
    indian_tribe_federally_rec = models.TextField(blank=True, null=True)
    native_hawaiian_owned_busi = models.TextField(blank=True, null=True)
    tribally_owned_business = models.TextField(blank=True, null=True)
    asian_pacific_american_own = models.TextField(blank=True, null=True)
    black_american_owned_busin = models.TextField(blank=True, null=True)
    hispanic_american_owned_bu = models.TextField(blank=True, null=True)
    native_american_owned_busi = models.TextField(blank=True, null=True)
    subcontinent_asian_asian_i = models.TextField(blank=True, null=True)
    other_minority_owned_busin = models.TextField(blank=True, null=True)
    for_profit_organization = models.TextField(blank=True, null=True)
    nonprofit_organization = models.TextField(blank=True, null=True)
    other_not_for_profit_organ = models.TextField(blank=True, null=True)
    us_local_government = models.TextField(blank=True, null=True)
    referenced_idv_modificatio = models.TextField(blank=True, null=True)
    undefinitized_action = models.TextField(blank=True, null=True)
    undefinitized_action_desc = models.TextField(blank=True, null=True)
    domestic_or_foreign_entity = models.TextField(blank=True, null=True)
    domestic_or_foreign_e_desc = models.TextField(blank=True, null=True)
    annual_revenue = models.TextField(blank=True, null=True)
    division_name = models.TextField(blank=True, null=True)
    division_number_or_office = models.TextField(blank=True, null=True)
    number_of_employees = models.TextField(blank=True, null=True)
    vendor_alternate_name = models.TextField(blank=True, null=True)
    vendor_alternate_site_code = models.TextField(blank=True, null=True)
    vendor_enabled = models.TextField(blank=True, null=True)
    vendor_legal_org_name = models.TextField(blank=True, null=True)
    vendor_location_disabled_f = models.TextField(blank=True, null=True)
    vendor_site_code = models.TextField(blank=True, null=True)
    pulled_from = models.TextField(blank=True, null=True)
    last_modified = models.TextField(blank=True, null=True)
    initial_report_date = models.TextField(blank=True, null=True)

    # Timestamp field auto generated by broker
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    @classmethod
    def get_or_create_2(cls, transaction, **kwargs):
        try:
            if not transaction.newer_than(kwargs):
                for (k, v) in kwargs.items():
                    setattr(transaction.contract_data, k, v)
        except ObjectDoesNotExist:
            transaction.contract_data = cls(**kwargs)
        return transaction.contract_data

    class Meta:
        db_table = 'transaction_fpds'


class TransactionFABS(models.Model):
    transaction = models.OneToOneField(
        TransactionNormalized, on_delete=models.CASCADE,
        primary_key=True, related_name='assistance_data')
    published_award_financial_assistance_id = models.TextField(blank=True, null=True, db_index=True)
    afa_generated_unique = models.TextField(blank=True, null=False)
    action_date = models.TextField(blank=True, null=True)
    action_type = models.TextField(blank=True, null=True)
    assistance_type = models.TextField(blank=True, null=True)
    award_description = models.TextField(blank=True, null=True)
    awardee_or_recipient_legal = models.TextField(blank=True, null=True)
    awardee_or_recipient_uniqu = models.TextField(blank=True, null=True)
    awarding_agency_code = models.TextField(blank=True, null=True)
    awarding_agency_name = models.TextField(blank=True, null=True)
    awarding_office_code = models.TextField(blank=True, null=True)
    awarding_office_name = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_c = models.TextField(blank=True, null=True)
    awarding_sub_tier_agency_n = models.TextField(blank=True, null=True)
    award_modification_amendme = models.TextField(blank=True, null=True)
    business_funds_indicator = models.TextField(blank=True, null=True)
    business_types = models.TextField(blank=True, null=True)
    cfda_number = models.TextField(blank=True, null=True, db_index=True)
    cfda_title = models.TextField(blank=True, null=True)
    correction_late_delete_ind = models.TextField(blank=True, null=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    fain = models.TextField(blank=True, null=True, db_index=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    fiscal_year_and_quarter_co = models.TextField(blank=True, null=True)
    funding_agency_code = models.TextField(blank=True, null=True)
    funding_agency_name = models.TextField(blank=True, null=True)
    funding_office_code = models.TextField(blank=True, null=True)
    funding_office_name = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_co = models.TextField(blank=True, null=True)
    funding_sub_tier_agency_na = models.TextField(blank=True, null=True)
    is_active = models.BooleanField(null=False, default=False)
    is_historical = models.NullBooleanField()
    legal_entity_address_line1 = models.TextField(blank=True, null=True)
    legal_entity_address_line2 = models.TextField(blank=True, null=True)
    legal_entity_address_line3 = models.TextField(blank=True, null=True)
    legal_entity_city_name = models.TextField(blank=True, null=True)
    legal_entity_city_code = models.TextField(blank=True, null=True)
    legal_entity_congressional = models.TextField(blank=True, null=True)
    legal_entity_country_code = models.TextField(blank=True, null=True)
    legal_entity_country_name = models.TextField(blank=True, null=True)
    legal_entity_county_code = models.TextField(blank=True, null=True)
    legal_entity_county_name = models.TextField(blank=True, null=True)
    legal_entity_foreign_city = models.TextField(blank=True, null=True)
    legal_entity_foreign_posta = models.TextField(blank=True, null=True)
    legal_entity_foreign_provi = models.TextField(blank=True, null=True)
    legal_entity_state_code = models.TextField(blank=True, null=True)
    legal_entity_state_name = models.TextField(blank=True, null=True)
    legal_entity_zip5 = models.TextField(blank=True, null=True)
    legal_entity_zip_last4 = models.TextField(blank=True, null=True)
    modified_at = models.DateTimeField(blank=True, null=True)
    non_federal_funding_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    period_of_performance_curr = models.TextField(blank=True, null=True)
    period_of_performance_star = models.TextField(blank=True, null=True)
    place_of_performance_city = models.TextField(blank=True, null=True)
    place_of_performance_code = models.TextField(blank=True, null=True)
    place_of_performance_congr = models.TextField(blank=True, null=True)
    place_of_perform_country_c = models.TextField(blank=True, null=True)
    place_of_perform_country_n = models.TextField(blank=True, null=True)
    place_of_perform_county_c = models.TextField(blank=True, null=True)
    place_of_perform_county_na = models.TextField(blank=True, null=True)
    place_of_performance_forei = models.TextField(blank=True, null=True)
    place_of_perform_state_nam = models.TextField(blank=True, null=True)
    place_of_performance_zip4a = models.TextField(blank=True, null=True)
    record_type = models.IntegerField(blank=True, null=True)
    sai_number = models.TextField(blank=True, null=True)
    total_funding_amount = models.TextField(blank=True, null=True)
    uri = models.TextField(blank=True, null=True, db_index=True)

    # Timestamp field auto generated by broker
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    @classmethod
    def get_or_create_2(cls, transaction, **kwargs):
        try:
            if not transaction.newer_than(kwargs):
                for (k, v) in kwargs.items():
                    setattr(transaction.assistance_data, k, v)
        except ObjectDoesNotExist:
            transaction.assistance_data = cls(**kwargs)
        return transaction.assistance_data

    class Meta:
        db_table = 'transaction_fabs'
        unique_together = (('awarding_sub_tier_agency_c', 'award_modification_amendme', 'fain', 'uri'),)


class Subaward(DataSourceTrackedModel):
    # Foreign keys
    award = models.ForeignKey(Award, models.CASCADE, related_name="subawards")
    recipient = models.ForeignKey(LegalEntity, models.DO_NOTHING)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    cfda = models.ForeignKey(Cfda, models.DO_NOTHING, related_name="related_subawards", null=True)
    awarding_agency = models.ForeignKey(Agency, models.DO_NOTHING, related_name="awarding_subawards", null=True)
    funding_agency = models.ForeignKey(Agency, models.DO_NOTHING, related_name="funding_subawards", null=True)
    place_of_performance = models.ForeignKey(Location, models.DO_NOTHING, null=True)

    subaward_number = models.TextField(db_index=True)
    amount = models.DecimalField(max_digits=20, decimal_places=2)
    description = models.TextField(null=True, blank=True)

    recovery_model_question1 = models.TextField(null=True, blank=True)
    recovery_model_question2 = models.TextField(null=True, blank=True)

    action_date = models.DateField(blank=True, null=True)
    award_report_fy_month = models.IntegerField()
    award_report_fy_year = models.IntegerField()

    naics = models.TextField(blank=True, null=True, verbose_name="NAICS",
                             help_text="Specified which industry the work for this transaction falls into. A "
                                       "6-digit code")
    naics_description = models.TextField(blank=True, null=True, verbose_name="NAICS Description",
                                         help_text="A plain text description of the NAICS code")

    class Meta:
        managed = True
        unique_together = (('subaward_number', 'award'),)


# class MatviewMinimalFabs(pg.View):
#     projection = ['transaction_id', 'cfda_number', 'awarding_agency_code']
#     dependencies = ['myapp.TransactionFABS']
#     sql = '''
# CREATE MATERIALIZED VIEW matview_minimal_fabs AS (
# SELECT
#   "transaction_fabs"."transaction_id",
#   "transaction_fabs"."cfda_number",
#   "transaction_fabs"."awarding_agency_code"
# FROM
#   "transaction_fabs"
# ORDER BY
#   "cfda_number" ASC)
# '''

#     class Meta:
#         # app_label = 'myapp'
#         db_table = 'matview_minimal_fabs'
#         managed = False

class MatviewAwardSearch(models.Model):
    # Fields
    action_date = models.DateField(blank=True, null=False)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    transaction_id = models.IntegerField(blank=False, null=False, primary_key=True)
    action_type = models.TextField()
    award_id = models.IntegerField()
    award_category = models.TextField()
    total_obligation = models.DecimalField(
        max_digits=15, decimal_places=2, blank=True,
        null=True)
    federal_action_obligation = models.DecimalField(
        max_digits=20, db_index=True, decimal_places=2, blank=True,
        null=True)

    place_of_performance_id = models.IntegerField()
    pop_country_name = models.TextField()
    pop_country_code = models.TextField()
    pop_county_code = models.TextField()
    pop_state_code = models.TextField()
    pop_congressional_code = models.TextField()

    awarding_agency_id = models.IntegerField()
    awarding_agency_code = models.TextField()
    funding_agency_id = models.IntegerField()

    naics_code = models.TextField()
    naics_description = models.TextField()
    piid = models.TextField()
    pcs_code = models.TextField()
    pcs_description = models.TextField()

    type_of_contract_pricing = models.TextField()
    type_set_aside = models.TextField()
    extent_competed = models.TextField()

    cfda_number = models.TextField()
    cfda_title = models.TextField()
    cfda_popular_name = models.TextField()

    recipient_id = models.IntegerField()
    recipient_name = models.TextField()
    recipient_unique_id = models.TextField()
    parent_recipient_unique_id = models.TextField()
    business_types_description = models.TextField()

    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_state_name = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_congressional_code = models.TextField()

    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_abbreviation = models.TextField()
    funding_toptier_agency_abbreviation = models.TextField()
    awarding_subtier_agency_abbreviation = models.TextField()
    funding_subtier_agency_abbreviation = models.TextField()

    class Meta:
        managed = False
        db_table = 'matview_award_search'
