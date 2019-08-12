from django.db import models


class ExternalDataType(models.Model):
    external_data_type_id = models.AutoField(primary_key=True)
    name = models.TextField(blank=False, null=False)
    description = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "external_data_type"


class ExternalDataLoadDate(models.Model):
    external_data_load_date_id = models.AutoField(primary_key=True)
    last_load_date = models.DateTimeField(blank=False, null=False)
    external_data_type = models.ForeignKey(ExternalDataType, models.DO_NOTHING, blank=False, null=False)

    class Meta:
        managed = True
        unique_together = (("last_load_date", "external_data_type"),)
        db_table = "external_data_load_date"
