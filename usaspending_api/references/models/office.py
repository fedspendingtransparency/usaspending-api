from django.db import models


class Office(models.Model):
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)
    office_code = models.TextField(null=False, unique=True)
    office_name = models.TextField(null=True)
    sub_tier_code = models.TextField(null=False)
    agency_code = models.TextField(null=False)
    contract_awards_office = models.BooleanField()
    contract_funding_office = models.BooleanField()
    financial_assistance_awards_office = models.BooleanField()
    financial_assistance_funding_office = models.BooleanField()

    class Meta:
        managed = True
        db_table = "office"

    def __str__(self):
        return "%s" % self.office_name
