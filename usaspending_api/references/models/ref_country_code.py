from django.db import models


class RefCountryCode(models.Model):
    country_code = models.TextField(primary_key=True)
    country_name = models.TextField(blank=True, null=True)
    valid_begin_date = models.DateTimeField(blank=True, null=True)
    valid_end_date = models.DateTimeField(blank=True, null=True)
    valid_code_indicator = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    latest_population = models.BigIntegerField(null=True)

    class Meta:
        managed = True
        db_table = "ref_country_code"

    def __str__(self):
        return "%s: %s" % (self.country_code, self.country_name)
