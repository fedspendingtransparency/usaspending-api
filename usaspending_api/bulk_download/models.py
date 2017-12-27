import datetime

from django.db import models
from django.utils import timezone
from usaspending_api.download.models import JobStatus
from usaspending_api.references.models import ToptierAgency, SubtierAgency


class BulkDownloadJob(models.Model):
    # Job data
    bulk_download_job_id = models.AutoField(primary_key=True)
    job_status = models.ForeignKey(JobStatus, models.DO_NOTHING, null=False)
    monthly_download = models.BooleanField(default=False)

    # Used to easily identify duplicate or search download jobs
    json_request = models.TextField(blank=True, null=True)
    prime_awards = models.BooleanField(default=False)
    sub_awards = models.BooleanField(default=False)
    contracts = models.BooleanField(default=False)
    grants = models.BooleanField(default=False)
    direct_payments = models.BooleanField(default=False)
    loans = models.BooleanField(default=False)
    other_financial_assistance = models.BooleanField(default=False)
    agency = models.ForeignKey(ToptierAgency, models.DO_NOTHING, null=True)
    sub_agency = models.TextField(blank=True, null=True)
    date_type = models.TextField(default="action_date")
    start_date = models.DateField(null=True)
    end_date = models.DateField(null=True)

    # File details
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
