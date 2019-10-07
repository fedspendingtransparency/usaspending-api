from django.db import models


class SubtierAgency(models.Model):
    subtier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)
    subtier_code = models.TextField(db_index=True, unique=True)
    abbreviation = models.TextField(blank=True, null=True)
    name = models.TextField(db_index=True)

    class Meta:
        db_table = "subtier_agency"
