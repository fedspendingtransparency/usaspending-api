"""
! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !

This is pre-work for DEV-2752 and will be folded into mainline code as part of that ticket.

If this warning is still hanging around in the year 2020, it's probably safe to drop this
model as the original developer probably won the lottery or something and now owns an island
in the Pacific and can't be bothered with such nonsense.

! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !
"""
from django.db.models import Model, TextField, URLField


class ToptierAgency(Model):
    toptier_code = TextField(primary_key=True)
    agency_name = TextField()
    agency_abbreviation = TextField(blank=True, null=True)
    mission = TextField(blank=True, null=True)
    website = URLField(blank=True, null=True)
    congressional_justification = URLField(blank=True, null=True)
    icon_filename = TextField(blank=True, null=True)

    class Meta:
        db_table = "toptier_agency_new"
