"""
! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !

This is pre-work for DEV-2752 and will be folded into mainline code as part of that ticket.

If this warning is still hanging around in the year 2020, it's probably safe to drop this
model as the original developer probably won the lottery or something and now owns an island
in the Pacific and can't be bothered with such nonsense.

! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !
"""
from django.db import transaction
from django.db.models import F, Manager, Max, Model, TextField, URLField
from usaspending_api.agencies.models.raw_agency_codes_csv import RawAgencyCodesCSV


class ToptierAgencyManager(Manager):

    def perform_import(self):
        """
        Imports Toptier agencies from the raw agency codes table.
        """
        toptiers = RawAgencyCodesCSV.objects.filter(
            cgac_agency_code__isnull=False,
            agency_name__isnull=False,
            is_frec=False,
        ).annotate(
            toptier_code=F("cgac_agency_code")
        ).values(
            "toptier_code"
        ).annotate(
            agency_name=Max("agency_name"),
            agency_abbreviation=Max("agency_abbreviation"),
            mission=Max("mission"),
            website=Max("website"),
            congressional_justification=Max("congressional_justification"),
            icon_filename=Max("icon_filename"),
        ).union(
            RawAgencyCodesCSV.objects.filter(
                frec__isnull=False,
                frec_entity_description__isnull=False,
                is_frec=True,
            ).annotate(
                toptier_code=F("frec")
            ).values(
                "toptier_code"
            ).annotate(
                agency_name=Max("frec_entity_description"),
                agency_abbreviation=Max("frec_abbreviation"),
                mission=Max("mission"),
                website=Max("website"),
                congressional_justification=Max("congressional_justification"),
                icon_filename=Max("icon_filename"),
            ),
            all=True
        ).values(
            "toptier_code", "agency_name", "agency_abbreviation", "mission",
            "website", "congressional_justification", "icon_filename"
        )

        with transaction.atomic():
            self.get_queryset().all().delete()
            self.bulk_create([
                ToptierAgency(
                    toptier_code=toptier["toptier_code"],
                    agency_name=toptier["agency_name"],
                    agency_abbreviation=toptier["agency_abbreviation"],
                    mission=toptier["mission"],
                    website=toptier["website"],
                    congressional_justification=toptier["congressional_justification"],
                    icon_filename=toptier["icon_filename"],
                )
                for toptier in toptiers
            ])

        return len(toptiers)


class ToptierAgency(Model):
    toptier_code = TextField(primary_key=True)
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)
    mission = TextField(blank=True, null=True)
    website = URLField(blank=True, null=True)
    congressional_justification = URLField(blank=True, null=True)
    icon_filename = TextField(blank=True, null=True)

    objects = ToptierAgencyManager()

    class Meta:
        db_table = "toptier_agency_new"
