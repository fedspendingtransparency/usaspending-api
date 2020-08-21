from django.db import models
from django.contrib.postgres.fields import ArrayField

from usaspending_api.awards.models import Award


class CovidFinancialAccountMatview(models.Model):
    award = models.OneToOneField(
        Award, on_delete=models.DO_NOTHING, primary_key=True, related_name="covid_financial_account"
    )
    type = models.TextField()
    def_codes = ArrayField(models.TextField(), default=None)

    outlay = models.DecimalField(max_digits=23, decimal_places=2)
    obligation = models.DecimalField(max_digits=23, decimal_places=2)
    total_loan_value = models.DecimalField(max_digits=23, decimal_places=2)

    recipient_hash = models.UUIDField()
    recipient_name = models.TextField()

    class Meta:
        managed = False
        db_table = "mv_covid_financial_account"
