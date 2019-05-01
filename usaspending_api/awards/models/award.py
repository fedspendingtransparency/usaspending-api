from django.db import models
from django.db.models import Q

from usaspending_api.common.models import DataSourceTrackedModel


class AwardManager(models.Manager):
    def get_queryset(self):
        """
        A generated award will have these set to null, but will also receive no
        transactions. Thus, these will remain null. This finds those awards and
        throws them out. As soon as one of those awards gets a transaction
        (i.e. it is no longer empty), these will be updated via update_from_transaction
        and the award will no longer match these criteria
        """
        q_kwargs = {
            "latest_transaction__isnull": True,
            "date_signed__isnull": True,
            "total_obligation__isnull": True,
        }

        return super(AwardManager, self).get_queryset().filter(~Q(**q_kwargs))


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

    id = models.BigAutoField(primary_key=True)
    type = models.TextField(
        db_index=True,
        verbose_name="Award Type",
        null=True,
        help_text="The mechanism used to distribute funding. The federal government can distribute "
        "funding in several forms. These award types include contracts, grants, loans, "
        "and direct payments.",
    )
    type_description = models.TextField(
        verbose_name="Award Type Description",
        blank=True,
        null=True,
        help_text="The plain text description of the type of the award",
    )
    category = models.TextField(
        db_index=True,
        verbose_name="Category",
        null=True,
        help_text="A field that generalizes the award's type.",
    )
    piid = models.TextField(
        db_index=True,
        blank=True,
        null=True,
        help_text="Procurement Instrument Identifier - A unique identifier assigned to a federal "
        "contract, purchase order, basic ordering agreement, basic agreement, and "
        "blanket purchase agreement. It is used to track the contract, and any "
        "modifications or transactions related to it. After October 2017, it is "
        "between 13 and 17 digits, both letters and numbers.",
    )
    fpds_agency_id = models.TextField(blank=True, null=True)
    fpds_parent_agency_id = models.TextField(blank=True, null=True)
    fain = models.TextField(
        db_index=True,
        blank=True,
        null=True,
        help_text="An identification code assigned to each financial assistance award tracking "
        "purposes. The FAIN is tied to that award (and all future modifications to that "
        "award) throughout the award's life. Each FAIN is assigned by an agency. Within "
        "an agency, FAIN are unique: each new award must be issued a new FAIN. FAIN "
        "stands for Federal Award Identification Number, though the digits are letters, "
        "not numbers.",
    )
    uri = models.TextField(db_index=True, blank=True, null=True, help_text="The uri of the award")
    total_obligation = models.DecimalField(
        max_digits=23,
        db_index=True,
        decimal_places=2,
        null=True,
        verbose_name="Total Obligated",
        help_text="The amount of money the government is obligated to pay for the " "award",
    )
    total_outlay = models.DecimalField(
        max_digits=23,
        db_index=True,
        decimal_places=2,
        null=True,
        help_text="The total amount of money paid out for this award",
    )
    total_subsidy_cost = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="The total of the original_loan_subsidy_cost from associated " "transactions",
    )
    total_loan_value = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="The total of the face_value_loan_guarantee from associated " "transactions",
    )
    awarding_agency = models.ForeignKey(
        "references.Agency",
        related_name="+",
        null=True,
        help_text="The awarding agency for the award",
        db_index=True,
    )
    funding_agency = models.ForeignKey(
        "references.Agency",
        related_name="+",
        null=True,
        help_text="The funding agency for the award",
        db_index=True,
    )
    date_signed = models.DateField(
        null=True, db_index=False, verbose_name="Award Date", help_text="The date the award was signed"
    )
    recipient = models.ForeignKey(
        "references.LegalEntity", null=True, help_text="The recipient of the award", db_index=True
    )
    description = models.TextField(
        null=True, verbose_name="Award Description", help_text="A description of the award"
    )
    period_of_performance_start_date = models.DateField(
        null=True,
        db_index=True,
        verbose_name="Start Date",
        help_text="The start date for the period of performance",
    )
    period_of_performance_current_end_date = models.DateField(
        null=True,
        db_index=True,
        verbose_name="End Date",
        help_text="The current, not original, period of " "performance end date",
    )
    place_of_performance = models.ForeignKey(
        "references.Location",
        null=True,
        help_text="The principal place of business, where the majority of the "
        "work is performed. For example, in a manufacturing contract, "
        "this would be the main plant where items are produced.",
        db_index=True,
    )
    potential_total_value_of_award = models.DecimalField(
        max_digits=23,
        db_index=False,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Potential Total Value of Award",
        help_text="The sum of the potential_value_of_award from " "associated transactions",
    )
    base_and_all_options_value = models.DecimalField(
        max_digits=23,
        db_index=False,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Base and All Options Value",
        help_text="The sum of the base_and_all_options_value from " "associated transactions",
    )
    base_exercised_options_val = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        blank=True,
        null=True,
        verbose_name="Combined Base and Exercised Options",
        help_text="The sum of the base_exercised_options_val from " "associated transactions",
    )
    last_modified_date = models.DateField(
        blank=True, null=True, help_text="The date this award was last modified"
    )
    certified_date = models.DateField(blank=True, null=True, help_text="The date this record was certified")
    create_date = models.DateTimeField(
        auto_now_add=True, blank=True, null=True, help_text="The date this record was created in the API"
    )
    update_date = models.DateTimeField(
        auto_now=True, null=True, help_text="The last time this record was updated in the API"
    )
    latest_transaction = models.ForeignKey(
        "awards.TransactionNormalized",
        related_name="latest_for_award",
        null=True,
        help_text="The latest transaction by action_date associated with this award",
    )
    parent_award_piid = models.TextField(
        db_index=True,
        null=True,
        verbose_name="Parent Award Piid",
        help_text="The piid of the Award's parent Award",
    )
    generated_unique_award_id = models.TextField(
        blank=False, null=False, default="NONE", verbose_name="Generated Unique Award ID"
    )
    is_fpds = models.BooleanField(blank=False, null=False, default=False, verbose_name="Is FPDS")
    transaction_unique_id = models.TextField(
        blank=False, null=False, default="NONE", verbose_name="Transaction Unique ID"
    )
    total_funding_amount = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        blank=True,
        null=True,
        help_text="A summation of this award's transactions'" " funding amount",
    )
    non_federal_funding_amount = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="A summation of this award's transactions' non-federal funding amount",
    )
    fiscal_year = models.IntegerField(
        blank=True, null=True, help_text="Fiscal Year calculated based on Action Date"
    )

    # Subaward aggregates
    total_subaward_amount = models.DecimalField(max_digits=23, decimal_places=2, null=True)
    subaward_count = models.IntegerField(default=0)

    objects = models.Manager()
    nonempty = AwardManager()

    def __str__(self):
        return "%s piid: %s fain: %s uri: %s" % (self.type_description, self.piid, self.fain, self.uri)

    @staticmethod
    def get_or_create_summary_award(
        awarding_agency=None,
        piid=None,
        fain=None,
        uri=None,
        parent_award_piid=None,
        save=True,
        record_type=None,
        generated_unique_award_id=None,
    ):
        """
        Given a set of award identifiers and awarding agency information,
        find a corresponding Award record. If we can't find one, create it.

        Returns:
            created: a list of new awards created (or that need to be created if using cache) used to enable bulk insert
            summary_award: the summary award that the calling process can map to
        """
        try:
            # Contract data uses piid as transaction ID. Financial assistance data depends on the record_type and
            # uses either uri (record_type=1) or fain (record_type=2 or 3).
            lookup_value = (piid, "piid")
            if record_type:
                if str(record_type) in ("2", "3"):
                    lookup_value = (fain, "fain")
                else:
                    lookup_value = (uri, "uri")

            if generated_unique_award_id:
                # Use the generated unique ID if available
                lookup_kwargs = {"generated_unique_award_id": generated_unique_award_id}
            else:
                # Use the lookup_value is generated unique ID is not available
                lookup_kwargs = {"awarding_agency": awarding_agency, lookup_value[1]: lookup_value[0]}

            # Look for an existing award record
            summary_award = Award.objects.filter(Q(**lookup_kwargs)).first()

            if summary_award:
                return [], summary_award

            # Now create the award record for this award transaction
            create_kwargs = {
                "awarding_agency": awarding_agency,
                "parent_award_piid": parent_award_piid,
                lookup_value[1]: lookup_value[0],
            }
            if generated_unique_award_id:
                create_kwargs["generated_unique_award_id"] = generated_unique_award_id
                if generated_unique_award_id.startswith("CONT_AW_"):
                    create_kwargs["is_fpds"] = True

            summary_award = Award(**create_kwargs)

            if save:
                summary_award.save()

            return [summary_award], summary_award

        # Do not use bare except
        except ValueError:
            raise ValueError(
                "Unable to find or create an award with the provided information: piid={}, fain={}, uri={}, "
                "parent_award_piid={}, awarding_agency={}, generated_unique_award_id={}".format(
                    piid, fain, uri, parent_award_piid, awarding_agency, generated_unique_award_id
                )
            )

    class Meta:
        db_table = "awards"
        indexes = [
            models.Index(fields=["-update_date"], name="awards_update_date_desc_idx"),
            models.Index(fields=["generated_unique_award_id"], name="award_unique_id"),
        ]
