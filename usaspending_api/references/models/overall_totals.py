from django.db import models


class OverallTotals(models.Model):
    id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    fiscal_year = models.IntegerField(blank=True, null=True)
    total_budget_authority = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = True
        db_table = "overall_totals"
