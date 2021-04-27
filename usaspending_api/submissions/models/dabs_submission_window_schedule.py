from datetime import datetime
from django.db import models


class DABSSubmissionWindowSchedule(models.Model):
    id = models.IntegerField(primary_key=True)
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

    def parse_dates_fields(self, timezone):
        self._parse_date_field("period_start_date", timezone)
        self._parse_date_field("period_end_date", timezone)
        self._parse_date_field("submission_start_date", timezone)
        self._parse_date_field("submission_due_date", timezone)
        self._parse_date_field("certification_due_date", timezone)
        self._parse_date_field("submission_reveal_date", timezone)

    def _parse_date_field(self, date_field, timezone):
        if isinstance(self.__getattribute__(date_field), str):
            self.__setattr__(date_field, datetime.strptime(self.__getattribute__(date_field), "%Y-%m-%d %H:%M:%SZ"))

        self.__setattr__(date_field, self.__getattribute__(date_field).replace(tzinfo=timezone))

    class Meta:
        managed = True
        db_table = "dabs_submission_window_schedule"
        unique_together = (
            "submission_fiscal_year",
            "submission_fiscal_quarter",
            "submission_fiscal_month",
            "is_quarter",
        )
