from django.db import models


class BudgetAuthority(models.Model):

    agency_identifier = models.TextField(db_index=True)  # aka CGAC
    fr_entity_code = models.TextField(null=True, db_index=True)  # aka FREC
    year = models.IntegerField(null=False)
    amount = models.BigIntegerField(null=True)

    class Meta:
        db_table = "budget_authority"
        unique_together = (("agency_identifier", "fr_entity_code", "year"),)
