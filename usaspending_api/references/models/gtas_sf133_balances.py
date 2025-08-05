from django.db import models
from django_cte import CTEManager


class GTASSF133Balances(models.Model):
    fiscal_year = models.IntegerField()
    fiscal_period = models.IntegerField()
    budget_authority_unobligated_balance_brought_forward_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    adjustments_to_unobligated_balance_brought_forward_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_incurred_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    budget_authority_appropriation_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    borrowing_authority_amount = models.DecimalField(max_digits=23, decimal_places=2)
    contract_authority_amount = models.DecimalField(max_digits=23, decimal_places=2)
    spending_authority_from_offsetting_collections_amount = models.DecimalField(max_digits=23, decimal_places=2)
    other_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_incurred = models.DecimalField(max_digits=23, decimal_places=2)
    deobligations_or_recoveries_or_refunds_from_prior_year_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    unobligated_balance_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    total_budgetary_resources_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    status_of_budgetary_resources_total_cpe = models.DecimalField(max_digits=23, decimal_places=2, default=0.0)
    disaster_emergency_fund = models.ForeignKey(
        "references.DisasterEmergencyFundCode",
        models.DO_NOTHING,
        blank=True,
        null=True,
        db_column="disaster_emergency_fund_code",
    )
    treasury_account_identifier = models.ForeignKey(
        "accounts.TreasuryAppropriationAccount",
        models.DO_NOTHING,
        null=True,
        db_column="treasury_account_identifier",
        related_name="gtas",
    )
    tas_rendering_label = models.TextField(null=True, db_index=True)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)
    anticipated_prior_year_obligation_recoveries = models.DecimalField(max_digits=23, decimal_places=2)
    prior_year_paid_obligation_recoveries = models.DecimalField(max_digits=23, decimal_places=2)
    adjustments_to_unobligated_balance_brought_forward_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, default=0.00
    )

    objects = CTEManager()

    class Meta:
        managed = True
        db_table = "gtas_sf133_balances"
        unique_together = ("fiscal_year", "fiscal_period", "disaster_emergency_fund", "tas_rendering_label")
