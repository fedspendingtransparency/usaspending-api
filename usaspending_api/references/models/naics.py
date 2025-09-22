from django.db import models


class NAICS(models.Model):
    """Based on United States Census Bureau"""

    code = models.TextField(primary_key=True)
    description = models.TextField(null=False)
    long_description = models.TextField(null=True)
    year = models.IntegerField(default=0)

    class Meta:
        managed = True
        db_table = "naics"
