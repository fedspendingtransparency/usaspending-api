from django.db.models import Model, TextField


class FREC(Model):
    """
    Financial Reporting Entity Code (a.k.a. FR Entity Code)
    """

    frec_code = TextField(primary_key=True)
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)

    # Derived from the FREC CGAC ASSOCIATION column in the agency_codes.csv agency reference data.  This is used
    # exclusively for mapping treasury and federal accounts to our toptier agencies when the AID of the account
    # is a shared AID (a.k.a. a FREC agency).  I would not recommend using this to convert FREC to CGAC in any
    # other context, but you know your problem/solution better than I do...
    associated_cgac_code = TextField(blank=True, null=True)

    class Meta:
        db_table = "frec"

    def __repr__(self):
        return f"{self.frec_code} - [{self.agency_abbreviation}] {self.agency_name}"
