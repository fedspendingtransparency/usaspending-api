from django.db import models

from usaspending_api.awards.models import Award


class ReportingAwardDownloadView(models.Model):
    award = models.OneToOneField(Award, primary_key=True)
    type = models.TextField()
    piid = models.TextField()
    fain = models.TextField()
    uri = models.TextField()

    action_date = models.DateField()
    fiscal_year = models.IntegerField()
    last_modified_date = models.TextField()
    period_of_performance_start_date = models.DateField()
    period_of_performance_current_end_date = models.DateField()
    date_signed = models.DateField()
    ordering_period_end_date = models.DateField(null=True)

    awarding_agency_id = models.IntegerField()
    funding_agency_id = models.IntegerField()
    awarding_toptier_agency_name = models.TextField()
    funding_toptier_agency_name = models.TextField()
    awarding_subtier_agency_name = models.TextField()
    funding_subtier_agency_name = models.TextField()
    awarding_toptier_agency_code = models.TextField()
    funding_toptier_agency_code = models.TextField()
    awarding_subtier_agency_code = models.TextField()
    funding_subtier_agency_code = models.TextField()

    recipient_location_country_code = models.TextField()
    recipient_location_country_name = models.TextField()
    recipient_location_state_code = models.TextField()
    recipient_location_county_code = models.TextField()
    recipient_location_county_name = models.TextField()
    recipient_location_zip5 = models.TextField()
    recipient_location_congressional_code = models.TextField()
    recipient_location_city_name = models.TextField()

    recipient_id = models.IntegerField()

    class Meta:
        managed = False
        db_table = "reporting_award_download_view"
