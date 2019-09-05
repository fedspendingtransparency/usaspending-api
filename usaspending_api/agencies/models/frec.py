from django.db import transaction
from django.db.models import Manager, Max, Model, TextField
from usaspending_api.agencies.models.raw_agency_codes_csv import RawAgencyCodesCSV


class FRECManager(Manager):

    def perform_import(self):
        """
        Imports FRECs from the raw agency codes table.
        """
        frecs = RawAgencyCodesCSV.objects.filter(
            frec__isnull=False,
            frec_entity_description__isnull=False,
        ).values(
            "frec"
        ).annotate(
            frec_entity_description=Max("frec_entity_description"),
            frec_abbreviation=Max("frec_abbreviation"),
        ).values(
            "frec", "frec_entity_description", "frec_abbreviation"
        )

        with transaction.atomic():
            self.get_queryset().all().delete()
            self.bulk_create([
                FREC(
                    frec_code=frec["frec"],
                    agency_name=frec["frec_entity_description"],
                    agency_abbreviation=frec["frec_abbreviation"],
                )
                for frec in frecs
            ])

        return len(frecs)


class FREC(Model):
    """
    Financial Reporting Entity Code (a.k.a. FR Entity Code)
    """
    frec_code = TextField(primary_key=True)
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)

    objects = FRECManager()

    class Meta:
        db_table = "frec"

    def __repr__(self):
        return "{} - [{}] {}".format(self.frec_code, self.agency_abbreviation, self.agency_name)
