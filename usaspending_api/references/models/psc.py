from django.db import models


class PSC(models.Model):
    """Based on https://www.acquisition.gov/PSC_Manual"""

    code = models.CharField(primary_key=True, max_length=4)
    description = models.TextField(null=False)

    class Meta:
        managed = True
        db_table = "psc"
