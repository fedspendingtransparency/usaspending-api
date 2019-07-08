from django.db import models


class ToptierAgency(models.Model):
    toptier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    cgac_code = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Code", db_index=True)
    fpds_code = models.TextField(blank=True, null=True)
    abbreviation = models.TextField(blank=True, null=True, verbose_name="Agency Abbreviation")
    name = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Name", db_index=True)
    mission = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Mission Statement")
    website = models.URLField(blank=True, null=True, verbose_name="Top-Tier Agency Website")
    justification = models.URLField(blank=True, null=True, verbose_name="Top-Tier Agency Congressional Justification")
    icon_filename = models.TextField(blank=True, null=True, verbose_name="Top-Tier Agency Icon Filename")

    class Meta:
        managed = True
        db_table = "toptier_agency"
