from django.db import models


class ToptierAgency(models.Model):
    toptier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)
    cgac_code = models.TextField(db_index=True, unique=True)
    abbreviation = models.TextField(blank=True, null=True)
    name = models.TextField(db_index=True)
    mission = models.TextField(blank=True, null=True)
    website = models.URLField(blank=True, null=True)
    justification = models.URLField(blank=True, null=True)
    icon_filename = models.TextField(blank=True, null=True)

    # Since toptier agencies are many to one with agencies, if all you have is a toptier agency,
    # use this to get to the agency that best represents it.  This could be an agency with a
    # subtier agency that has toptier_flag is True or an agency with no subtier.  This is denormalized
    # because figuring it out on the fly is nontrivial.
    preferred_agency = models.ForeignKey("references.Agency", models.DO_NOTHING, blank=True, null=True)

    class Meta:
        db_table = "toptier_agency"
