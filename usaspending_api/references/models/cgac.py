from django.db import models


class CGAC(models.Model):
    """Common Government-Wide Accounting Classification"""

    cgac_code = models.TextField(primary_key=True)
    agency_name = models.TextField()
    agency_abbreviation = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "cgac"

    def __repr__(self):
        return "{} - [{}] {}".format(self.cgac_code, self.agency_abbreviation, self.agency_name)
