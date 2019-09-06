import pytest

from django.core.management import call_command
from django.core.management.base import CommandError
from pathlib import Path
from usaspending_api.agencies.models import CGAC, FREC, SubtierAgency, ToptierAgency, RawAgency


AGENCY_FILE = Path(__file__).resolve().parent / "test_load_agencies.csv"
BOGUS_ABBREVIATION = "THIS IS MY NEW ABBREVIATION"


@pytest.mark.django_db
def test_load_agencies():

    # Confirm everything is empty.
    assert CGAC.objects.all().count() == 0
    assert FREC.objects.all().count() == 0
    assert SubtierAgency.objects.all().count() == 0
    assert ToptierAgency.objects.all().count() == 0
    assert RawAgency.objects.all().count() == 0

    # This should error since agency file is required.
    with pytest.raises(CommandError):
        call_command("load_agencies_new")

    # Load all the things.
    call_command("load_agencies_new", AGENCY_FILE)

    # Confirm nothing is empty.
    assert CGAC.objects.all().count() > 0
    assert FREC.objects.all().count() > 0
    assert SubtierAgency.objects.all().count() > 0
    assert ToptierAgency.objects.all().count() > 0
    assert RawAgency.objects.all().count() > 0

    # Make an arbitrary change to one of the downstream tables.
    cgac_code = CGAC.objects.first().cgac_code
    CGAC.objects.filter(cgac_code=cgac_code).update(agency_abbreviation=BOGUS_ABBREVIATION)
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation == BOGUS_ABBREVIATION

    # Reload the agency file.
    call_command("load_agencies_new", AGENCY_FILE)

    # Since nothing has changed in the agency file, downstream data sets should not have changed.
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation == BOGUS_ABBREVIATION

    # Now force a reload.
    call_command("load_agencies_new", AGENCY_FILE, "--force")

    # This should fix the CGAC we changed.
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation != BOGUS_ABBREVIATION

    # Remake our arbitrary change.
    CGAC.objects.filter(cgac_code=cgac_code).update(agency_abbreviation=BOGUS_ABBREVIATION)
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation == BOGUS_ABBREVIATION

    # Make a change to the raw table.
    RawAgency.objects.all().delete()

    # Perform a non-force reload.
    call_command("load_agencies_new", AGENCY_FILE)

    # This should also fix our CGAC since changes in the source file trigger reloads.
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation != BOGUS_ABBREVIATION

    # Remake our arbitrary change.
    CGAC.objects.filter(cgac_code=cgac_code).update(agency_abbreviation=BOGUS_ABBREVIATION)
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation == BOGUS_ABBREVIATION

    # Make a TINY change to the raw table.
    RawAgency.objects.filter(cgac_agency_code=cgac_code).update(frec_abbreviation=BOGUS_ABBREVIATION)

    # Perform a non-force reload.
    call_command("load_agencies_new", AGENCY_FILE)

    # Once again, this should fix our CGAC.
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation != BOGUS_ABBREVIATION
