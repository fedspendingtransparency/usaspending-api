from django.db import models
from usaspending_api.accounts.models.appropriation_account_balances import AppropriationAccountBalances
from usaspending_api.common.models import DataSourceTrackedModel
from usaspending_api.submissions.models import SubmissionAttributes


class AppropriationAccountBalancesQuarterly(DataSourceTrackedModel):
    """
    Represents quarterly financial amounts by tas (aka, treasury account symbol, aka appropriation account).
    Each data broker submission provides a snapshot of the most recent numbers for that fiscal year. Thus, to populate
    this model, we subtract previous quarters' balances from the balances of the current submission.
    """

    treasury_account_identifier = models.ForeignKey("TreasuryAppropriationAccount", models.CASCADE)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    budget_authority_unobligated_balance_brought_forward_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    adjustments_to_unobligated_balance_brought_forward_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    budget_authority_appropriated_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    borrowing_authority_amount_total_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    contract_authority_amount_total_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    spending_authority_from_offsetting_collections_amount_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    other_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    deobligations_recoveries_refunds_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    unobligated_balance_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    status_of_budgetary_resources_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_incurred_total_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "appropriation_account_balances_quarterly"
        # TODO: find out why this unique index causes constraint violations w/ our data
        # unique_together = ('treasury_account_identifier', 'submission')

    @classmethod
    def insert_quarterly_numbers(cls, submission_id=None):
        """Bulk insert tas quarterly finanical numbers."""
        field_list = [field.column for field in AppropriationAccountBalancesQuarterly._meta.get_fields()]

        # delete existing quarterly data
        if submission_id is None:
            AppropriationAccountBalancesQuarterly.objects.all().delete()
        else:
            AppropriationAccountBalancesQuarterly.objects.filter(submission_id=submission_id).delete()

        # retrieve RawQuerySet of quarterly breakouts
        qtr_records = AppropriationAccountBalances.get_quarterly_numbers(submission_id)
        qtr_list = []

        # for each record in the RawQuerySet, a corresponding AppropriationAccountBalancesQuarterly object and save it
        # for subsequent bulk insert
        # TODO: maybe we don't want this entire list in memory?
        for rec in qtr_records:
            # remove any fields in the qtr_record RawQuerySet object so that we can create its
            # AppropriationAccountBalancesQuarterly counterpart more easily
            rec_dict = rec.__dict__
            rec_dict = {key: rec_dict[key] for key in rec_dict if key in field_list}
            qtr_list.append(AppropriationAccountBalancesQuarterly(**rec_dict))

        AppropriationAccountBalancesQuarterly.objects.bulk_create(qtr_list)
