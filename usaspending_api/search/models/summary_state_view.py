from django.contrib.postgres.fields import ArrayField
from django.db import models


class SummaryStateView(models.Model):
    duh = models.UUIDField(primary_key=True, help_text="Deterministic Unique Hash")
    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    type = models.TextField()
    distinct_awards = ArrayField(models.TextField(), default=list)

    pop_country_code = models.TextField()
    pop_state_code = models.TextField()

    generated_pragmatic_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    federal_action_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    original_loan_subsidy_cost = models.DecimalField(max_digits=23, decimal_places=2)
    face_value_loan_guarantee = models.DecimalField(max_digits=23, decimal_places=2)
    counts = models.IntegerField()

    class Meta:
        managed = False
        db_table = "summary_state_view"
