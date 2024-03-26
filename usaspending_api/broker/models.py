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


class DeltaTableLoadVersion(models.Model):
    delta_table_load_version_id = models.AutoField(primary_key=True)
    name = models.TextField(blank=False, null=False)
    last_version_copied_to_staging = models.IntegerField(default=-1, null=False)
    last_version_copied_to_live = models.IntegerField(default=-1, null=False)
    description = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        # Schema is set to `public` in migration `0003_deltatableloadversion`
        db_table = "delta_table_load_version"


class LoadTrackerStep(models.Model):
    load_tracker_step_id = models.AutoField(primary_key=True)
    name = models.TextField(blank=False, null=False)

    class Meta:
        managed = True
        db_table = "load_tracker_step"


class LoadTrackerLoadType(models.Model):
    load_tracker_load_type_id = models.AutoField(primary_key=True)
    name = models.TextField(blank=False, null=False)

    class Meta:
        managed = True
        db_table = "load_tracker_load_type"


class LoadTracker(models.Model):
    load_tracker_id = models.AutoField(primary_key=True)
    load_tracker_load_type = models.ForeignKey("LoadTrackerLoadType", on_delete=models.deletion.DO_NOTHING)
    load_tracker_step =  models.ForeignKey("LoadTrackerStep", on_delete=models.deletion.DO_NOTHING)
    start_date_time = models.DateTimeField(auto_now=True, null=False)
    end_date_time = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "load_tracker"
