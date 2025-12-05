from django.db import models


class ProgramActivityPark(models.Model):
    """This table serves as a lookup for PARKs to determine their corresponding name"""

    code = models.TextField(primary_key=True)
    name = models.TextField(null=False, db_index=True)

    class Meta:
        managed = True
        db_table = "program_activity_park"
