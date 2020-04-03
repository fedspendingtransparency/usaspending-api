from django.db import models


class PopCounty(models.Model):
    id = models.AutoField(primary_key=True)
    state_code = models.CharField(max_length=2)
    state_name = models.TextField()
    county_number = models.CharField(max_length=3)
    county_name = models.TextField()
    latest_population = models.IntegerField()

    class Meta:
        managed = True
        db_table = "ref_population_county"
