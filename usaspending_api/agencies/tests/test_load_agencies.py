import pytest

from django.core.management import call_command
from django.core.management.base import CommandError
from pathlib import Path
from usaspending_api.agencies.models import CGAC, FREC, SubtierAgency, ToptierAgency


AGENCY_FILE = Path(__file__).resolve().parent / "test_load_agencies.csv"
BOGUS_ABBREVIATION = "THIS IS MY TEST ABBREVIATION"


@pytest.mark.django_db
def test_load_agencies(monkeypatch):

    # Can't run vacuums in a transaction.  Since tests are run in a transaction, we'll NOOP the
    # function that performs the vacuuming.
    monkeypatch.setattr(
        "usaspending_api.agencies.management.commands.load_agencies_new.Command._vacuum_tables", lambda a: None
    )

    # Confirm everything is empty.
    assert CGAC.objects.all().count() == 0
    assert FREC.objects.all().count() == 0
    assert SubtierAgency.objects.all().count() == 0
    assert ToptierAgency.objects.all().count() == 0

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

    # Make an arbitrary change to one of the downstream tables.
    cgac_code = CGAC.objects.first().cgac_code
    CGAC.objects.filter(cgac_code=cgac_code).update(agency_abbreviation=BOGUS_ABBREVIATION)
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation == BOGUS_ABBREVIATION

    # Reload the agency file.
    call_command("load_agencies_new", AGENCY_FILE)

    # This should fix the CGAC we changed.
    assert CGAC.objects.filter(cgac_code=cgac_code).first().agency_abbreviation != BOGUS_ABBREVIATION
