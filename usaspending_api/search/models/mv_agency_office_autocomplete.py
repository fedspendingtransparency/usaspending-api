from django.db import models


class AgencyOfficeAutocompleteMatview(models.Model):
    """
    Supports Award Search 2.0 Agency and Office autocomplete.
    """

    agency_office_autocomplete_id = models.IntegerField(primary_key=True)
    toptier_flag = models.BooleanField()
    toptier_agency_id = models.IntegerField()
    toptier_code = models.TextField(null=True)
    toptier_abbreviation = models.TextField(null=True)
    toptier_name = models.TextField()
    subtier_abbreviation = models.TextField(null=True)
    subtier_name = models.TextField()
    subtier_code = models.TextField()
    has_awarding_data = models.BooleanField()
    has_funding_data = models.BooleanField()
    office_code = models.TextField(null=True)
    office_name = models.TextField(null=True)

    class Meta:
        db_table = "mv_agency_office_autocomplete"
        managed = False
        unique_together = (
            (
                "toptier_code",
                "toptier_abbreviation",
                "toptier_name",
                "subtier_abbreviation",
                "subtier_name",
                "subtier_code",
                "office_code",
                "office_name",
            ),
        )
