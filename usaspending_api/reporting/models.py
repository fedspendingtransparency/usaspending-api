from django.db import models


class ReportingAgencyTas(models.Model):
    """
    Model representing reporting data for appropriation and object class program activity values grouped by TAS and
    period
    """

    reporting_agency_tas_id = models.AutoField(primary_key=True)
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
            models.Index(fields=["fiscal_year", "fiscal_period", "toptier_code"], name="reporting_agency_tas_group_idx")
        ]


class ReportingAgencyMissingTas(models.Model):
    """
    Model representing missing reporting data for appropriation and object class program activity values grouped by TAS and
    period
    """

    reporting_agency_missing_tas_id = models.AutoField(primary_key=True)
    toptier_code = models.TextField()
    fiscal_year = models.IntegerField()
    fiscal_period = models.IntegerField()
    tas_rendering_label = models.TextField()
    obligated_amount = models.DecimalField(max_digits=23, decimal_places=2)

    class Meta:
        db_table = "reporting_agency_missing_tas"
        indexes = [
            models.Index(fields=["fiscal_year", "fiscal_period", "toptier_code"], name="rpt_agency_missing_tas_grp_idx")
        ]


class ReportingAgencyOverview(models.Model):
    """
    Model representing reporting data for appropriation and object class program activity values grouped by TAS and
    period
    """

    reporting_agency_overview_id = models.AutoField(primary_key=True)
    toptier_code = models.TextField()
    fiscal_year = models.IntegerField()
    fiscal_period = models.IntegerField()
    total_dollars_obligated_gtas = models.DecimalField(max_digits=23, decimal_places=2, null=True)
    total_budgetary_resources = models.DecimalField(max_digits=23, decimal_places=2, null=True)
    total_diff_approp_ocpa_obligated_amounts = models.DecimalField(max_digits=23, decimal_places=2, null=True)
    unlinked_procurement_c_awards = models.IntegerField(null=True)
    unlinked_assistance_c_awards = models.IntegerField(null=True)
    unlinked_procurement_d_awards = models.IntegerField(null=True)
    unlinked_assistance_d_awards = models.IntegerField(null=True)
    linked_procurement_awards = models.IntegerField(null=True)
    linked_assistance_awards = models.IntegerField(null=True)

    class Meta:
        db_table = "reporting_agency_overview"
        indexes = [
            models.Index(fields=["fiscal_year", "fiscal_period", "toptier_code"], name="reporting_agency_ovr_group_idx")
        ]
