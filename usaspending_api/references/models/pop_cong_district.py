from django.db import models


class PopCongressionalDistrict(models.Model):
    id = models.AutoField(primary_key=True)
    state_code = models.CharField(max_length=2)
    state_name = models.TextField()
    state_abbreviation = models.CharField(max_length=2)
    congressional_district = models.TextField()
    latest_population = models.IntegerField()

    class Meta:
        managed = True
        db_table = "ref_population_cong_district"
