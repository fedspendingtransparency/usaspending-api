from django.db import models


class ToptierAgency(models.Model):
    toptier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)
    toptier_code = models.TextField(db_index=True, unique=True)
    abbreviation = models.TextField(blank=True, null=True)
    name = models.TextField(db_index=True)
    mission = models.TextField(blank=True, null=True)
    website = models.URLField(blank=True, null=True)
    justification = models.URLField(blank=True, null=True)
    icon_filename = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "toptier_agency"
