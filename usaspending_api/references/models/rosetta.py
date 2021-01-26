from django.db import models
from django.contrib.postgres.fields import JSONField


class Rosetta(models.Model):
    """
    Based on the "public" tab in Schema's Rosetta Crosswalk Data Dictionary:
        Data Transparency Rosetta Stone_All_Versions.xlsx
    """

    document_name = models.TextField(primary_key=True)
    document = JSONField()

    class Meta:
        managed = True
        db_table = "rosetta"
