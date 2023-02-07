from django.db import models


class CToDLinkageUpdates(models.Model):

    financial_accounts_by_awards_id = models.IntegerField(primary_key=True)
    award_id = models.IntegerField(unique=False)

    class Meta:
        managed = True
        db_table = "c_to_d_linkage_updates"
