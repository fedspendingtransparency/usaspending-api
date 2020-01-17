from django.db import models


class TASAutocompleteMatview(models.Model):
    """
    Supports TAS autocomplete.  For performance reasons, pre-filters the TAS
    codes/numbers/symbols/whatever that can be linked to File D data.
    """

    tas_autocomplete_id = models.IntegerField(primary_key=True)
    allocation_transfer_agency_id = models.TextField(null=True)
    agency_id = models.TextField()
    beginning_period_of_availability = models.TextField(null=True)
    ending_period_of_availability = models.TextField(null=True)
    availability_type_code = models.TextField(null=True)
    main_account_code = models.TextField()
    sub_account_code = models.TextField(null=True)
    tas_rendering_label = models.TextField()

    class Meta:

        db_table = "tas_autocomplete_matview"
        managed = False
