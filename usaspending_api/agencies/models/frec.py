from django.db.models import Model, TextField


class FREC(Model):
    """
    Financial Reporting Entity Code (a.k.a. FR Entity Code)
    """
    frec_code = TextField(primary_key=True)
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)

    class Meta:
        db_table = "frec"

    def __repr__(self):
        return "{} - [{}] {}".format(self.frec_code, self.agency_abbreviation, self.agency_name)
