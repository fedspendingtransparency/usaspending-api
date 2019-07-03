from django.db import models


class OfficeAgency(models.Model):
    office_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    aac_code = models.TextField(blank=True, null=True, verbose_name="Office Code")
    name = models.TextField(blank=True, null=True, verbose_name="Office Name")

    class Meta:
        managed = True
        db_table = "office_agency"
