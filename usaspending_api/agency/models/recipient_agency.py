from django.db import models


class RecipientAgency(models.Model):
    id = models.IntegerField(primary_key=True)
    fiscal_year = models.IntegerField()
    toptier_code = models.ForeignKey(
        "references.ToptierAgency", models.DO_NOTHING, related_name="(%class)s", null=False
    )
    recipient_hash = models.UUIDField(null=True, db_index=True)
    recipient_amount = models.DecimalField(max_digits=23, decimal_places=2)

    class Meta:
        managed = True
        db_table = "recipient_agency"
