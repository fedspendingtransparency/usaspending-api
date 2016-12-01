from django.db import models


class SubmissionAttributes(models.Model):
    id = models.AutoField(primary_key=True)
    broker_submission_id = models.IntegerField()
    usaspending_update = models.DateField(blank=True, null=True)
    user_id = models.IntegerField()
    cgac_code = models.CharField(max_length=3, blank=True, null=True)
    submitting_agency = models.CharField(max_length=150, blank=True, null=True)
    submitter_name = models.CharField(max_length=200, blank=True, null=True)
    submission_modification = models.NullBooleanField()
    version_number = models.IntegerField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'submission_attributes'
