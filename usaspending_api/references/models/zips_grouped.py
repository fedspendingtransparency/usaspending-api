from django.db import models


class ZipsGrouped(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    zips_grouped_id = models.IntegerField(primary_key=True)
    zip5 = models.TextField()
    state_abbreviation = models.TextField()
    county_number = models.TextField()
    congressional_district_no = models.TextField()

    class Meta:
        db_table = "zips_grouped"
