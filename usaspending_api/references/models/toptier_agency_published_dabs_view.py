from django.db import models


class ToptierAgencyPublishedDABSView(models.Model):
    toptier_code = models.TextField()
    name = models.TextField()
    abbreviation = models.TextField()
    toptier_agency_id = models.OneToOneField(
        "references.ToptierAgency", on_delete=models.DO_NOTHING, primary_key=False, related_name="%(class)s"
    )
    agency_id = models.OneToOneField(
        "references.Agency", on_delete=models.DO_NOTHING, primary_key=False, related_name="%(class)s"
    )
    user_selectable = models.BooleanField()

    class Meta:
        db_table = "vw_published_dabs_toptier_agency"
        managed = False
