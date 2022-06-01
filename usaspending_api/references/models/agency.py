from django.db import models
from django.db.models import F
from django_cte import CTEManager


class Agency(models.Model):
    id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)
    toptier_agency = models.ForeignKey("references.ToptierAgency", models.DO_NOTHING, db_index=True)
    subtier_agency = models.OneToOneField("references.SubtierAgency", models.DO_NOTHING, null=True, db_index=True)
    # toptier_flag is ONLY used to show subtiers that represent toptiers.  Not all toptiers will have
    # a toptier_flag = True.  Do not use it to try to find agency records when only given a toptier.
    # There is a discussion about this in the documentation repository if you'd like further clarification.
    toptier_flag = models.BooleanField(default=False)
    user_selectable = models.BooleanField(default=False)

    # Not shown here is an index idx_agency_toptier_agency_id_null_subtier_agency_id_uniq that
    # is used to enforce uniquity on toptier_agency_id when subtier_agency_id is null.

    objects = CTEManager()

    class Meta:
        db_table = "agency"

    @staticmethod
    def get_by_toptier(toptier_code):
        """
        Get an agency record by toptier information only

        Args:
            toptier_code: a CGAC or FREC code

        Returns:
            an Agency instance

        """
        return (
            Agency.objects.filter(
                toptier_agency__toptier_code=toptier_code, subtier_agency__name=F("toptier_agency__name")
            )
            .order_by("-update_date")
            .first()
        )

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
            return Agency.objects.filter(subtier_agency__subtier_code=subtier_code).order_by("-update_date").first()

    @staticmethod
    def get_by_toptier_subtier(toptier_code, subtier_code):
        """
        Lookup an Agency record by toptier cgac code and subtier code

        Args:
            toptier_code: a CGAC or FREC code
            subtier_code: an agency subtier code

        Returns:
            an Agency instance

        """
        return (
            Agency.objects.filter(toptier_agency__toptier_code=toptier_code, subtier_agency__subtier_code=subtier_code)
            .order_by("-update_date")
            .first()
        )

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
        for agency in [self.toptier_agency, self.subtier_agency]:
            if agency:
                stringrep = stringrep + agency.name + " :: "
        return stringrep
