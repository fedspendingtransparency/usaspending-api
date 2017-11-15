import datetime

from django.db import models
from django.utils import timezone
from usaspending_api.download.models import JobStatus


class BulkDownloadJob(models.Model):
    bulk_download_job_id = models.AutoField(primary_key=True)
    job_status = models.ForeignKey(JobStatus, models.DO_NOTHING, null=False)
    file_name = models.TextField(blank=False, null=False)
    file_size = models.BigIntegerField(blank=True, null=True)
    number_of_rows = models.BigIntegerField(blank=True, null=True)
    number_of_columns = models.IntegerField(blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'bulk_download_job'

    def seconds_elapsed(self):
        if self.job_status.name == 'running':
            return timezone.now() - self.create_date
        elif self.job_status.name in ('finished', 'failed'):
            return self.update_date - self.create_date
