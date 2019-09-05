"""
! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !

This is pre-work for DEV-2752 and will be folded into mainline code as part of that ticket.

If this warning is still hanging around in the year 2020, it's probably safe to drop this
model as the original developer probably won the lottery or something and now owns an island
in the Pacific and can't be bothered with such nonsense.

! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !
"""
from django.db import transaction
from django.db.models import BooleanField, Case, F, ForeignKey, Manager, Max, Model, TextField, When
from usaspending_api.agencies.models.raw_agency_codes_csv import RawAgencyCodesCSV
from usaspending_api.common.helpers.orm_helpers import BoolOr


class SubtierAgencyManager(Manager):

    def perform_import(self):
        """
        Imports Subtier agencies from the raw agency codes table.  Note that Toptiers are
        intrinsically tied to subtiers.  If you re-import subtiers, you will likely also
        need to re-import toptiers.
        """
        subtiers = RawAgencyCodesCSV.objects.annotate(
            toptier_code=Case(When(is_frec=True, then=F("frec")), default=F("cgac_agency_code")),
            toptier_agency_name=Case(When(is_frec=True, then=F("frec_entity_description")), default=F("agency_name")),
        ).filter(
            subtier_code__isnull=False,
            subtier_name__isnull=False,
            toptier_code__isnull=False,
            toptier_agency_name__isnull=False,
        ).values(
            "subtier_code",
            "toptier_code",
        ).annotate(
            agency_name=Max("agency_name"),
            agency_abbreviation=Max("agency_abbreviation"),
            is_toptier=BoolOr("toptier_flag"),
        ).values(
            "subtier_code", "toptier_code", "agency_name", "agency_abbreviation", "is_toptier"
        )

        with transaction.atomic():
            self.get_queryset().all().delete()
            self.bulk_create([
                SubtierAgency(
                    subtier_code=subtier["subtier_code"],
                    toptier_id=subtier["toptier_code"],
                    agency_name=subtier["agency_name"],
                    agency_abbreviation=subtier["agency_abbreviation"],
                    is_toptier=subtier["is_toptier"],
                )
                for subtier in subtiers
            ])

        return len(subtiers)


class SubtierAgency(Model):
    subtier_code = TextField(primary_key=True)
    toptier = ForeignKey("agencies.ToptierAgency", db_column="toptier_code")
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)
    is_toptier = BooleanField()

    objects = SubtierAgencyManager()

    class Meta:
        db_table = "subtier_agency_new"
