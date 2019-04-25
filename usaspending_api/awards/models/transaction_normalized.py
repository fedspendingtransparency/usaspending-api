from django.db import models

from usaspending_api.common.helpers.generic_helper import fy


class TransactionNormalized(models.Model):
    id = models.BigAutoField(primary_key=True)
    award = models.ForeignKey(
        "awards.Award", models.CASCADE, help_text="The award which this transaction is contained in"
    )
    usaspending_unique_transaction_id = models.TextField(
        blank=True,
        null=True,
        help_text="If this record is legacy USASpending data, this is "
        "the unique transaction identifier from that system",
    )
    type = models.TextField(
        verbose_name="Action Type",
        null=True,
        help_text="The type for this transaction. For example, A, B, C, D",
        db_index=False,
    )
    type_description = models.TextField(
        blank=True,
        verbose_name="Action Type Description",
        null=True,
        help_text="The plain text description of the transaction type",
    )
    period_of_performance_start_date = models.DateField(
        verbose_name="Period of Performance Start Date",
        null=True,
        help_text="The period of performance start date",
    )
    period_of_performance_current_end_date = models.DateField(
        verbose_name="Period of Performance Current End Date",
        null=True,
        help_text="The current end date of the period of " "performance",
    )
    action_date = models.DateField(
        verbose_name="Transaction Date", help_text="The date this transaction was actioned", db_index=True
    )
    action_type = models.TextField(
        blank=True, null=True, help_text="The type of transaction. For example, A, B, C, D"
    )
    action_type_description = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(
        max_digits=23,
        db_index=False,
        decimal_places=2,
        blank=True,
        null=True,
        help_text="The obligation of the federal government for this " "transaction",
    )
    original_loan_subsidy_cost = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="The original_loan_subsidy_cost for loan type " "transactions",
    )
    face_value_loan_guarantee = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="The face_value_loan_guarantee for loan type " "transactions",
    )
    modification_number = models.TextField(
        blank=True,
        null=True,
        verbose_name="Modification Number",
        help_text="The modification number for this transaction",
    )
    awarding_agency = models.ForeignKey(
        "references.Agency",
        related_name="%(app_label)s_%(class)s_awarding_agency",
        null=True,
        help_text="The agency which awarded this transaction",
    )
    funding_agency = models.ForeignKey(
        "references.Agency",
        related_name="%(app_label)s_%(class)s_funding_agency",
        null=True,
        help_text="The agency which is funding this transaction",
    )
    recipient = models.ForeignKey(
        "references.LegalEntity", null=True, help_text="The recipient for this transaction"
    )
    description = models.TextField(null=True, help_text="The description of this transaction")
    place_of_performance = models.ForeignKey(
        "references.Location",
        null=True,
        help_text="The location where the work on this transaction was performed",
    )
    drv_award_transaction_usaspend = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    drv_current_total_award_value_amount_adjustment = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    drv_potential_total_award_value_amount_adjustment = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    last_modified_date = models.DateField(
        blank=True, null=True, help_text="The date this transaction was last modified"
    )
    certified_date = models.DateField(
        blank=True, null=True, help_text="The date this transaction was certified"
    )
    create_date = models.DateTimeField(
        auto_now_add=True, blank=True, null=True, help_text="The date this transaction was created in the API"
    )
    update_date = models.DateTimeField(
        auto_now=True,
        null=True,
        help_text="The last time this transaction was updated in the API",
        db_index=True,
    )
    fiscal_year = models.IntegerField(
        blank=True, null=True, help_text="Fiscal Year calculated based on Action Date"
    )
    transaction_unique_id = models.TextField(
        blank=False, null=False, default="NONE", verbose_name="Transaction Unique ID"
    )
    generated_unique_award_id = models.TextField(
        blank=False, null=False, default="NONE", verbose_name="Generated Unique Award ID"
    )
    is_fpds = models.BooleanField(blank=False, null=False, default=False, verbose_name="Is FPDS")
    funding_amount = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        blank=True,
        null=True,
        help_text="Assistance data variable.  non_federal_funding_amount + " "federal_action_obligation",
    )
    non_federal_funding_amount = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, help_text="Assistance Data variable."
    )
    unique_award_key = models.TextField(null=True, db_index=True)  # From broker.

    def __str__(self):
        return "%s award: %s" % (self.type_description, self.award)

    def newer_than(self, dct):
        """Compares age of this instance to a Python dictionary

        Determines the age of each by last_modified_date, if set,
        otherwise action_date.
        Returns `False` if either side lacks a date completely.
        """

        my_date = self.last_modified_date
        their_date = dct.get("last_modified_date")
        if my_date and their_date:
            return my_date > their_date
        else:
            return False

    @classmethod
    def get_or_create_transaction(cls, **kwargs):
        """Gets and updates, or creates, a Transaction

        Transactions must be unique on Award, Awarding Agency, and Mod Number
        """
        transaction = (
            cls.objects.filter(
                award=kwargs.get("award"), modification_number=kwargs.get("modification_number")
            )
            .order_by("-update_date")
            .first()
        )
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
        db_table = "transaction_normalized"
        index_together = ["award", "action_date"]
