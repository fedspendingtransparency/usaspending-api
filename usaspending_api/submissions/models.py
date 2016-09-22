from django.db import models


class SubmissionAttributes(models.Model):
    submission_id = models.AutoField(primary_key=True)
    user_id = models.IntegerField()
    cgac_code = models.CharField(max_length=3, blank=True, null=True)
    submitting_agency = models.CharField(max_length=150, blank=True, null=True)
    submitter_name = models.CharField(max_length=200, blank=True, null=True)
    submission_modification = models.NullBooleanField()
    version_number = models.IntegerField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'submission_attributes'


class SubmissionProcess(models.Model):
    submission_process_id = models.AutoField(primary_key=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    status = models.CharField(max_length=50, blank=True, null=True)
    file_a_submission = models.NullBooleanField()
    file_b_submission = models.NullBooleanField()
    file_c_submission = models.NullBooleanField()
    file_d1_submission = models.NullBooleanField()
    file_d2_submission = models.NullBooleanField()
    file_e_submission = models.NullBooleanField()
    file_f_submission = models.NullBooleanField()
    create_date = models.DateTimeField(blank=True, null=True)
    create_user_id = models.CharField(max_length=50, blank=True, null=True)
    update_date = models.DateTimeField(blank=True, null=True)
    update_user_id = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'submission_process'
