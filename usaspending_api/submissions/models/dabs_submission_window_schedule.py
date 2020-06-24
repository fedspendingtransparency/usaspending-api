from datetime import datetime, timezone
from django.db import models


class DABSSubmissionWindowSchedule(models.Model):
    id = models.AutoField(primary_key=True)
    period_start_date = models.DateTimeField()
    period_end_date = models.DateTimeField()
    submission_start_date = models.DateTimeField()
    submission_due_date = models.DateTimeField()
    certification_due_date = models.DateTimeField()
    submission_reveal_date = models.DateTimeField()
    submission_fiscal_year = models.IntegerField()
    submission_fiscal_quarter = models.IntegerField()
    submission_fiscal_month = models.IntegerField()
    is_quarter = models.BooleanField()

    class Meta:
        managed = True
        db_table = "dabs_submission_window_schedule"
        unique_together = (
            "submission_fiscal_year",
            "submission_fiscal_quarter",
            "submission_fiscal_month",
            "is_quarter",
        )

    @classmethod
    def latest_monthly_to_display(cls):
        return cls.get_latest_submission(is_quarter=False)

    @classmethod
    def latest_quarterly_to_display(cls):
        return cls.get_latest_submission(is_quarter=True)

    @classmethod
    def get_latest_submission(cls, is_quarter):
        return (
            cls.objects.filter(is_quarter=is_quarter, submission_reveal_date__lte=datetime.now(timezone.utc))
            .order_by("-submission_fiscal_year", "-submission_fiscal_quarter", "-submission_fiscal_month")
            .values(
                "submission_fiscal_year",
                "submission_fiscal_quarter",
                "submission_fiscal_month",
                "submission_reveal_date",
            )
            .first()
        )
