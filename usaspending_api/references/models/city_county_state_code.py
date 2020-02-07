from django.db import models


class CityCountyStateCode(models.Model):
    feature_id = models.IntegerField()
    feature_name = models.TextField(blank=True, null=True)
    feature_class = models.TextField(blank=True, null=True)
    census_code = models.TextField(blank=True, null=True)
    census_class_code = models.TextField(blank=True, null=True)
    gsa_code = models.TextField(blank=True, null=True)
    opm_code = models.TextField(blank=True, null=True)
    state_numeric = models.TextField(db_index=True, blank=True, null=True)
    state_alpha = models.TextField()
    county_sequence = models.IntegerField(blank=True, null=True)
    county_numeric = models.TextField(blank=True, null=True)
    county_name = models.TextField(blank=True, null=True)
    primary_latitude = models.DecimalField(max_digits=13, decimal_places=8)
    primary_longitude = models.DecimalField(max_digits=13, decimal_places=8)
    date_created = models.DateField(blank=True, null=True)
    date_edited = models.DateField(blank=True, null=True)

    class Meta:
        db_table = "ref_city_county_state_code"

        # Not shown here is a unique index on feature_id, state_alpha, county_sequence, county_numeric (see
        # migration file for exact syntax).  Because Postgres does not enforce uniquity on nullable columns
        # and because county_sequence and county_numeric are nullable, we have to perform some trickery to
        # enforce uniqueness on the natural key columns.  Note that, as far as I could tell, the source of
        # these data do not provide actual, sanctioned natural keys.  These were obtained through observation
        # of the data; the only time features were repeated was when they spanned counties or states.
