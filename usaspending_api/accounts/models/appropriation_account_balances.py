from django.db import models, connection
from django_cte import CTEManager

from usaspending_api.common.models import DataSourceTrackedModel


class AbstractAppropriationAccountBalances(DataSourceTrackedModel):
    appropriation_account_balances_id = models.AutoField(primary_key=True)
    treasury_account_identifier = models.ForeignKey(
        "accounts.TreasuryAppropriationAccount",
        models.CASCADE,
        db_column="treasury_account_identifier",
        related_name="account_balances",
    )
    submission = models.ForeignKey("submissions.SubmissionAttributes", models.CASCADE)
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
    drv_appropriation_availability_period_start_date = models.DateField(blank=True, null=True)
    drv_appropriation_availability_period_end_date = models.DateField(blank=True, null=True)
    drv_appropriation_account_expired_status = models.TextField(blank=True, null=True)
    drv_obligations_unpaid_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    drv_other_obligated_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    final_of_fy = models.BooleanField(blank=False, null=False, default=False, db_index=True)

    class Meta:
        abstract = True


class AppropriationAccountBalancesManager(models.Manager):
    def get_queryset(self):
        """Get only records from the last submission per TAS per fiscal year."""
        return super(AppropriationAccountBalancesManager, self).get_queryset().filter(final_of_fy=True)


class AppropriationAccountBalances(AbstractAppropriationAccountBalances):
    """
    Represents Treasury Account Symbol (TAS) balances for each DATA Act
    broker submission. Each submission provides a snapshot of the most
    recent numbers for that fiscal year. In other words, the lastest
    submission for a fiscal year reflects the balances for the entire
    fiscal year.
    """

    class Meta:
        managed = True
        db_table = "appropriation_account_balances"

    objects = CTEManager()
    final_objects = AppropriationAccountBalancesManager()

    FINAL_OF_FY_SQL = """
        with submission_and_tai as (
            select distinct treasury_account_identifier, submission_id from (
                select distinct on (aab.treasury_account_identifier, s.reporting_fiscal_year)
                    aab.treasury_account_identifier,
                    s.submission_id
                from
                    submission_attributes s
                    inner join appropriation_account_balances aab on s.submission_id = aab.submission_id
                order by
                    aab.treasury_account_identifier,
                    s.reporting_fiscal_year,
                    s.reporting_period_end desc,
                    s.submission_id desc
            ) t
        )
        update  appropriation_account_balances
        set     final_of_fy = ((treasury_account_identifier, submission_id) in
                    (select treasury_account_identifier, submission_id from submission_and_tai))
        where   final_of_fy != ((treasury_account_identifier, submission_id) in
                    (select treasury_account_identifier, submission_id from submission_and_tai));
    """

    @classmethod
    def populate_final_of_fy(cls):
        with connection.cursor() as cursor:
            cursor.execute(cls.FINAL_OF_FY_SQL)
