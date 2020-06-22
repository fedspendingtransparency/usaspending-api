from django.db import models


class DABSSubmissionWindowSchedule(models.Model):
    id = models.AutoField(primary_key=True)
    reporting_start = models.DateTimeField()
    reporting_end = models.DateTimeField()
    submission_start_date = models.DateTimeField()
    submission_end_date = models.DateTimeField()
    certification_end_date = models.DateTimeField()
    submission_fiscal_year = models.IntegerField()
    submission_fiscal_quarter = models.IntegerField()
    submission_fiscal_period = models.IntegerField()
    is_quarter = models.BooleanField()

    class Meta:
        managed = True
        db_table = "dabs_submission_window_schedule"
        unique_together = (
            "submission_fiscal_year",
            "submission_fiscal_quarter",
            "submission_fiscal_period",
            "is_quarter",
        )
