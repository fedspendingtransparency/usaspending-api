from django.db import models
from django_cte import CTEManager


class SubmissionAttributes(models.Model):
    submission_id = models.IntegerField(primary_key=True)
    submission_window = models.ForeignKey("submissions.DABSSubmissionWindowSchedule", on_delete=models.DO_NOTHING)
    published_date = models.DateTimeField(blank=True, null=True, db_index=True)
    certified_date = models.DateTimeField(blank=True, null=True)
    toptier_code = models.TextField(blank=True, null=True, db_index=True)
    reporting_agency_name = models.TextField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    reporting_fiscal_year = models.IntegerField(blank=True, null=True)
    reporting_fiscal_quarter = models.IntegerField(blank=True, null=True)
    reporting_fiscal_period = models.IntegerField(blank=True, null=True)
    quarter_format_flag = models.BooleanField(default=True)
    is_final_balances_for_fy = models.BooleanField(default=False)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    history = models.JSONField(null=True)

    objects = CTEManager()

    class Meta:
        db_table = "submission_attributes"

    def __str__(self):
        return (
            f"TOPTIER {self.toptier_code} "
            f"FY {self.reporting_fiscal_year} "
            f"QTR {self.reporting_fiscal_quarter} "
            f"PRD {self.reporting_fiscal_period}"
        )

    @classmethod
    def latest_available_fy(cls):
        result = cls.objects.aggregate(fy=models.Max("reporting_fiscal_year"))
        return result["fy"]
