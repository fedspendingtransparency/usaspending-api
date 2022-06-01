from django.db import models


class Rosetta(models.Model):
    """
    Based on the "public" tab in Schema's Rosetta Crosswalk Data Dictionary:
        Data Transparency Rosetta Stone_All_Versions.xlsx
    """

    document_name = models.TextField(primary_key=True)
    document = models.JSONField()

    class Meta:
        managed = True
        db_table = "rosetta"
