from django.db import models

class AgencyMatview(models.Model):
    agency_id = models.IntegerField(blank=True, null=True)
    subtier_code = models.TextField(blank=True, null=True, verbose_name="Sub-Tier Agency Code")

    class Meta:
        managed = False
        db_table = 'agency_matview'

class ExecCompMatview(models.Model):
    duns = models.TextField(blank=True, default='', null=True, verbose_name="DUNS Number")
    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'exec_comp_matview'
