from django.db import models


class SummaryCfdaNumbersView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField(blank=True, null=True)
    fiscal_year = models.IntegerField()
    type = models.TextField(blank=True, null=True)
    cfda_number = models.TextField(blank=True, null=True)
    cfda_title = models.TextField(blank=True, null=True)
    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2, null=True, blank=True)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = "summary_view_cfda_number"
