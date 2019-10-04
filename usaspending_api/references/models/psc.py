from django.db import models


class PSC(models.Model):
    """Based on https://www.acquisition.gov/PSC_Manual"""

    code = models.CharField(primary_key=True, max_length=4)
    length = models.IntegerField(null=False, default=0)
    description = models.TextField(null=False)
    start_date = models.DateField(blank=True, null=True)
    end_date = models.DateField(blank=True, null=True)
    full_name = models.TextField(blank=True, null=True)
    excludes = models.TextField(blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    includes = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "psc"
