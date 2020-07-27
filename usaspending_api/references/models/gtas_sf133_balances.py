from django.db import models


class GTASSF133Balances(models.Model):
    fiscal_year = models.IntegerField()
    fiscal_period = models.IntegerField()
    obligations_incurred_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    budget_authority_appropriation_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    other_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    unobligated_balance_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    total_budgetary_resources_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    disaster_emergency_fund_code = models.TextField(null=True)
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

    class Meta:
        managed = True
        db_table = "gtas_sf133_balances"
        unique_together = ("fiscal_year", "fiscal_period", "disaster_emergency_fund_code", "tas_rendering_label")
