from django.db import models


class AgencyAutocompleteMatview(models.Model):
    """
    Supports Advanced Search Agency autocomplete.  For performance reasons, pre-filters agencies based
    on whether or not they have been cited in an award as an awarding or funding agency.
    """

    agency_autocomplete_id = models.IntegerField(primary_key=True)
    toptier_flag = models.BooleanField()
    toptier_agency_id = models.IntegerField()
    toptier_code = models.TextField(null=True)
    toptier_abbreviation = models.TextField(null=True)
    toptier_name = models.TextField()
    subtier_abbreviation = models.TextField(null=True)
    subtier_name = models.TextField()
    has_awarding_data = models.BooleanField()
    has_funding_data = models.BooleanField()

    class Meta:

        db_table = "mv_agency_autocomplete"
        managed = False
