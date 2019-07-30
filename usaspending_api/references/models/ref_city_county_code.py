from django.db import models
from usaspending_api.references.helpers import canonicalize_string


class RefCityCountyCode(models.Model):
    city_county_code_id = models.AutoField(primary_key=True)
    state_code = models.TextField(blank=True, null=True)
    city_name = models.TextField(blank=True, null=True, db_index=True)
    city_code = models.TextField(blank=True, null=True)
    county_code = models.TextField(blank=True, null=True, db_index=True)
    county_name = models.TextField(blank=True, null=True, db_index=True)
    type_of_area = models.TextField(blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "ref_city_county_code"

    @classmethod
    def canonicalize(cls):
        """
        Transforms the values in `city_name` and `county_name`
        to their canonicalized (uppercase, regulare spaced) form.
        """
        for obj in cls.objects.all():
            obj.city_name = canonicalize_string(obj.city_name)
            obj.county_name = canonicalize_string(obj.county_name)
            obj.save()
