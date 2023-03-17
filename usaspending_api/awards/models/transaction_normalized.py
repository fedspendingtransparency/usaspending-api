import os

from django.db import models

from django.contrib.postgres.fields import ArrayField


class TransactionNormalized(models.Model):
    id = models.BigAutoField(primary_key=True)
    award = models.ForeignKey(
        "search.AwardSearch", on_delete=models.DO_NOTHING, help_text="The award which this transaction is contained in"
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
        verbose_name="Period of Performance Start Date", null=True, help_text="The period of performance start date"
    )
    period_of_performance_current_end_date = models.DateField(
        verbose_name="Period of Performance Current End Date",
        null=True,
        help_text="The current end date of the period of performance",
    )
    action_date = models.DateField(
        verbose_name="Transaction Date", help_text="The date this transaction was actioned", db_index=True
    )
    action_type = models.TextField(blank=True, null=True, help_text="The type of transaction. For example, A, B, C, D")
    action_type_description = models.TextField(blank=True, null=True)
    federal_action_obligation = models.DecimalField(
        max_digits=23,
        db_index=False,
        decimal_places=2,
        blank=True,
        null=True,
        help_text="The obligation of the federal government for this transaction",
    )
    original_loan_subsidy_cost = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="The original_loan_subsidy_cost for loan type transactions",
    )
    face_value_loan_guarantee = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="The face_value_loan_guarantee for loan type transactions",
    )
    indirect_federal_sharing = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="The indirect_federal_sharing for this transaction",
    )
    modification_number = models.TextField(
        blank=True,
        null=True,
        verbose_name="Modification Number",
        help_text="The modification number for this transaction",
    )
    awarding_agency = models.ForeignKey(
        "references.Agency",
        on_delete=models.DO_NOTHING,
        related_name="%(app_label)s_%(class)s_awarding_agency",
        null=True,
        help_text="The agency which awarded this transaction",
    )
    funding_agency = models.ForeignKey(
        "references.Agency",
        on_delete=models.DO_NOTHING,
        related_name="%(app_label)s_%(class)s_funding_agency",
        null=True,
        help_text="The agency which is funding this transaction",
    )
    description = models.TextField(null=True, help_text="The description of this transaction")
    last_modified_date = models.DateField(
        blank=True, null=True, help_text="The date this transaction was last modified"
    )
    certified_date = models.DateField(blank=True, null=True, help_text="The date this transaction was certified")
    create_date = models.DateTimeField(
        auto_now_add=True, blank=True, null=True, help_text="The date this transaction was created in the API"
    )
    update_date = models.DateTimeField(
        auto_now=True, null=True, help_text="The last time this transaction was updated in the API", db_index=True
    )
    fiscal_year = models.IntegerField(blank=True, null=True, help_text="Fiscal Year calculated based on Action Date")
    transaction_unique_id = models.TextField(
        blank=False, null=False, default="NONE", verbose_name="Transaction Unique ID"
    )
    is_fpds = models.BooleanField(blank=False, null=False, default=False, verbose_name="Is FPDS")
    funding_amount = models.DecimalField(
        max_digits=23,
        decimal_places=2,
        blank=True,
        null=True,
        help_text="Assistance data variable.  non_federal_funding_amount + federal_action_obligation",
    )
    non_federal_funding_amount = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True, help_text="Assistance Data variable."
    )
    unique_award_key = models.TextField(null=True, db_index=True)  # From broker.
    business_categories = ArrayField(models.TextField(), default=list)

    def __str__(self):
        return "%s award: %s" % (self.type_description, self.award)

    class Meta:
        managed = False
        db_table = "vw_transaction_normalized"
        index_together = ["award", "action_date"]


NORM_ALT_COL_NAMES_IN_TRANSACTION_SEARCH = {
    # transaction_normalized col name : transaction_search col name
    "id": "transaction_id",
    "unique_award_key": "generated_unique_award_id",
    "certified_date": "award_certified_date",
    "description": "transaction_description",
}

NORM_CASTED_COL_MAP = {
    # transaction_normalized col name : type casting search -> normalized
    "indirect_federal_sharing": "NUMERIC(23,2)"
}

NORM_TO_TRANSACTION_SEARCH_COL_MAP = {
    f.column: NORM_ALT_COL_NAMES_IN_TRANSACTION_SEARCH.get(f.column, f.column)
    for f in TransactionNormalized._meta.fields
}

vw_transaction_normalized_sql = f"""
    CREATE OR REPLACE VIEW rpt.vw_transaction_normalized AS
        SELECT
            {(','+os.linesep+' '*12).join([
                (v+(f'::{NORM_CASTED_COL_MAP[k]}' if k in NORM_CASTED_COL_MAP else '')).ljust(62)+' AS '+k.ljust(48)
                for k, v in NORM_TO_TRANSACTION_SEARCH_COL_MAP.items()])}
        FROM
            rpt.transaction_search;
"""
