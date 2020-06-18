from django.db import models


class GTASTotalObligation(models.Model):
    fiscal_year = models.IntegerField()
    fiscal_period = models.IntegerField()
    total_obligation = models.DecimalField(max_digits=23, decimal_places=2)
    disaster_emergency_fund_code = models.TextField(null=True)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "gtas_total_obligation"
        unique_together = ("fiscal_year", "fiscal_period", "disaster_emergency_fund_code")
