from django.db import models


class SummaryNaicsCodesView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    naics_code = models.TextField(blank=True, null=True)
    naics_description = models.TextField(blank=True, null=True)

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = "summary_view_naics_codes"
