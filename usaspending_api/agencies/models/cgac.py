from django.db import transaction
from django.db.models import Manager, Max, Model, TextField
from usaspending_api.agencies.models.raw_agency import RawAgency


class CGACManager(Manager):

    def perform_import(self):
        """Imports CGACs from the raw agency codes table."""
        cgacs = RawAgency.objects.filter(
            cgac_agency_code__isnull=False,
            agency_name__isnull=False,
        ).values(
            "cgac_agency_code"
        ).annotate(
            agency_name=Max("agency_name"),
            agency_abbreviation=Max("agency_abbreviation"),
        ).values(
            "cgac_agency_code", "agency_name", "agency_abbreviation"
        )

        with transaction.atomic():
            self.get_queryset().all().delete()
            self.bulk_create([
                CGAC(
                    cgac_code=cgac["cgac_agency_code"],
                    agency_name=cgac["agency_name"],
                    agency_abbreviation=cgac["agency_abbreviation"],
                )
                for cgac in cgacs
            ])

        return len(cgacs)


class CGAC(Model):
    """
    Common Government-Wide Accounting Classification
    """
    cgac_code = TextField(primary_key=True)
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)

    objects = CGACManager()

    class Meta:
        db_table = "cgac"

    def __repr__(self):
        return "{} - [{}] {}".format(self.cgac_code, self.agency_abbreviation, self.agency_name)
