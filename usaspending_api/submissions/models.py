from django.db import models


class SubmissionAttributes(models.Model):
    submission_id = models.AutoField(primary_key=True)
    broker_submission_id = models.IntegerField(null=True)
    certified_date = models.DateField(blank=True, null=True)
    usaspending_update = models.DateField(blank=True, null=True)
    cgac_code = models.TextField(blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    reporting_fiscal_year = models.IntegerField(blank=True, null=True)
    reporting_fiscal_quarter = models.IntegerField(blank=True, null=True)
    reporting_fiscal_period = models.IntegerField(blank=True, null=True)
    quarter_format_flag = models.BooleanField(default=True)
    previous_submission = models.OneToOneField(
        "self",
        on_delete=models.DO_NOTHING,
        null=True,
        blank=True,
        help_text="A reference to the most recent submission for this CGAC within the same fiscal year",
    )
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "submission_attributes"

    def __str__(self):
        return "CGAC {} FY {} QTR {}".format(self.cgac_code, self.reporting_fiscal_year, self.reporting_fiscal_quarter)

    @classmethod
    def last_certified_fy(cls):
        """FY for reporting purposes is the last FY for which submissions have been certified."""

        result = cls.objects.filter(certified_date__isnull=False).aggregate(fy=models.Max("reporting_fiscal_year"))
        return result["fy"]
