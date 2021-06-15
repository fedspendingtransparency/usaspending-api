from django.db import models
from django.utils import timezone


class JobStatus(models.Model):
    job_status_id = models.AutoField(primary_key=True)
    name = models.TextField(blank=False, null=False)
    description = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "job_status"


class DownloadJob(models.Model):
    download_job_id = models.AutoField(primary_key=True)
    job_status = models.ForeignKey(JobStatus, models.DO_NOTHING, null=False)
    file_name = models.TextField(blank=False, null=False)
    file_size = models.BigIntegerField(blank=True, null=True)
    number_of_rows = models.IntegerField(blank=True, null=True)
    number_of_columns = models.IntegerField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    monthly_download = models.BooleanField(default=False)
    json_request = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "download_job"

    def seconds_elapsed(self):
        if self.job_status.name == "running":
            return timezone.now() - self.create_date
        elif self.job_status.name in ("finished", "failed"):
            return self.update_date - self.create_date
