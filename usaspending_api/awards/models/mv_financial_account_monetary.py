from django.db import models
from django.contrib.postgres.fields import ArrayField


class FinancialAccountMonetaryMatview(models.Model):
    award = models.OneToOneField("awards.AwardSearchView", on_delete=models.DO_NOTHING, primary_key=True, related_name="%(class)s")
    def_codes = ArrayField(models.TextField(), default=None)
    outlay = models.DecimalField(max_digits=23, decimal_places=2)
    obligation = models.DecimalField(max_digits=23, decimal_places=2)

    class Meta:
        managed = False
        db_table = "mv_financial_account_monetary"
