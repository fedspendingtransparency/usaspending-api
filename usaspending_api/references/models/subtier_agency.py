from django.db import models


class SubtierAgency(models.Model):
    subtier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    subtier_code = models.TextField(blank=True, null=True, verbose_name="Sub-Tier Agency Code")
    abbreviation = models.TextField(blank=True, null=True, verbose_name="Agency Abbreviation")
    name = models.TextField(blank=True, null=True, verbose_name="Sub-Tier Agency Name", db_index=True)

    class Meta:
        managed = True
        db_table = "subtier_agency"
