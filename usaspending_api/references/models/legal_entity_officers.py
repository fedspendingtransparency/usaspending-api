from django.db import models


class LegalEntityOfficers(models.Model):
    legal_entity = models.OneToOneField(
        "references.LegalEntity", on_delete=models.CASCADE, primary_key=True, related_name="officers"
    )
    duns = models.TextField(blank=True, default="", null=True, verbose_name="DUNS Number", db_index=True)

    officer_1_name = models.TextField(null=True, blank=True)
    officer_1_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_2_name = models.TextField(null=True, blank=True)
    officer_2_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_3_name = models.TextField(null=True, blank=True)
    officer_3_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_4_name = models.TextField(null=True, blank=True)
    officer_4_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    officer_5_name = models.TextField(null=True, blank=True)
    officer_5_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)

    update_date = models.DateField(auto_now_add=True, blank=True, null=True)

    class Meta:
        managed = True
