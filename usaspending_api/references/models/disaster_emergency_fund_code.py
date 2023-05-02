from django.db import models


class DisasterEmergencyFundCode(models.Model):
    """Based on Disaster Emergency Fund Code (DEFC)"""

    code = models.TextField(primary_key=True)
    public_law = models.TextField(null=False)
    title = models.TextField(null=True)
    group_name = models.TextField(null=True)
    urls = models.TextField(null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    earliest_public_law_enactment_date = models.DateField(null=True)

    class Meta:
        managed = True
        db_table = "disaster_emergency_fund_code"
