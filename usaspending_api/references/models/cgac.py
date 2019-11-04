from django.db.models import Model, TextField


class CGAC(Model):
    """
    Common Government-Wide Accounting Classification
    """

    cgac_code = TextField(primary_key=True)
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)

    class Meta:
        db_table = "cgac"

    def __repr__(self):
        return "{} - [{}] {}".format(self.cgac_code, self.agency_abbreviation, self.agency_name)
