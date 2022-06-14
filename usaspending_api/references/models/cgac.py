from django.db.models import BooleanField, Model, TextField
from django_cte import CTEManager


class CGAC(Model):
    """
    Common Government-Wide Accounting Classification
    """

    cgac_code = TextField(primary_key=True)
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)

    # Derived from the IS_FREC column in the CSV source file.  There are some agencies that are referred
    # to via their FREC code rather than their CGAC code.  FREC is a slightly more granular level of agency
    # classification which allows us to break certain agencies down into smaller chunks.  These agencies
    # are referred to as FREC agencies.  Occasionally you will also see them referred to as "shared CGAC"
    # or "shared AID" agencies.
    is_frec_agency = BooleanField(default=False)

    objects = CTEManager()

    class Meta:
        db_table = "cgac"

    def __repr__(self):
        return f"{self.cgac_code} - [{self.agency_abbreviation}] {self.agency_name}"
