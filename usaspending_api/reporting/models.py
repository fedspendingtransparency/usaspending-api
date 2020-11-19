from django.db import models


class ReportingAgencyTas(models.Model):
    """
    Model representing reporting data for appropriation and object class program activity values grouped by TAS and
    period
    """

    reporting_agency_tas_id = models.IntegerField(primary_key=True)
    toptier_code = models.TextField()
    fiscal_year = models.IntegerField()
    fiscal_period = models.IntegerField()
    tas_rendering_label = models.TextField()
    appropriation_obligated_amount = models.DecimalField(max_digits=23, decimal_places=2)
    object_class_pa_obligated_amount = models.DecimalField(max_digits=23, decimal_places=2)
    diff_approp_ocpa_obligated_amounts = models.DecimalField(max_digits=23, decimal_places=2)

    class Meta:
        db_table = "reporting_agency_tas"
        indexes = [
            models.Index(
                fields=[
                    "fiscal_year",
                    "fiscal_period",
                    "toptier_code"
                ],
                name="reporting_agency_tas_group_idx"
            )
        ]
