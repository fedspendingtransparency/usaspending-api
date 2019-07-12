from django.db import models
from django.db.models import F


class Agency(models.Model):

    id = models.AutoField(primary_key=True)  # meaningless id
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    toptier_agency = models.ForeignKey("references.ToptierAgency", models.DO_NOTHING, null=True, db_index=True)
    subtier_agency = models.ForeignKey("references.SubtierAgency", models.DO_NOTHING, null=True, db_index=True)
    office_agency = models.ForeignKey("references.OfficeAgency", models.DO_NOTHING, null=True, db_index=True)

    # 1182 This flag is true if toptier agency name and subtier agency name are equal.
    # This means the award is at the department level.
    toptier_flag = models.BooleanField(default=False)

    class Meta:
        managed = True
        db_table = "agency"
        unique_together = ("toptier_agency", "subtier_agency", "office_agency")

    @staticmethod
    def get_by_toptier(toptier_cgac_code):
        """
        Get an agency record by toptier information only

        Args:
            toptier_cgac_code: a CGAC (aka department) code

        Returns:
            an Agency instance

        """
        return Agency.objects.filter(
            toptier_agency__cgac_code=toptier_cgac_code,
            subtier_agency__name=F('toptier_agency__name')).order_by('-update_date').first()

    @staticmethod
    def get_by_subtier(subtier_code):
        """
        Get an agency record by subtier information only

        Args:
            subtier_code: subtier code

        Returns:
            an Agency instance

        If called with None / empty subtier code, returns None
        """
        if subtier_code:
            return Agency.objects.filter(subtier_agency__subtier_code=subtier_code).order_by('-update_date').first()

    @staticmethod
    def get_by_toptier_subtier(toptier_cgac_code, subtier_code):
        """
        Lookup an Agency record by toptier cgac code and subtier code

        Args:
            toptier_cgac_code: a CGAC (aka department) code
            subtier_code: an agency subtier code

        Returns:
            an Agency instance

        """
        return Agency.objects.filter(
            toptier_agency__cgac_code=toptier_cgac_code,
            subtier_agency__subtier_code=subtier_code
        ).order_by('-update_date').first()

    @staticmethod
    def get_by_subtier_only(subtier_code):
        """
        Lookup an Agency record by subtier code only

        Useful when data source has an inaccurate top tier code,
        but an accurate subtier code.  Will return an Agency
        if and only if a single match for the subtier code exists.

        Args:
            subtier_code: an agency subtier code

        Returns:
            an Agency instance

        """
        agencies = Agency.objects.filter(subtier_agency__subtier_code=subtier_code)
        if agencies.count() == 1:
            return agencies.first()
        else:
            return None

    def __str__(self):
        stringrep = ""
        for agency in [self.toptier_agency, self.subtier_agency, self.office_agency]:
            if agency:
                stringrep = stringrep + agency.name + " :: "
        return stringrep
