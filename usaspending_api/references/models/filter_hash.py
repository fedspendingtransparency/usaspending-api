from django.db import models


class FilterHash(models.Model):
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    filter = models.JSONField(blank=True, null=True, verbose_name="JSON of Filter")
    hash = models.TextField(blank=False, unique=True, verbose_name="Hash of JSON Filter")

    class Meta:
        managed = True
        db_table = "filter_hash"
