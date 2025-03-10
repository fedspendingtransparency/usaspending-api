from datetime import datetime
from pathlib import Path

import pytest
from django.core.management import call_command
from model_bakery import baker

from usaspending_api.references.models import PSC


@pytest.mark.django_db
def test_load_psc():
    """
    Test to ensure the ingested data size is correct
    Test to make sure previously missing information has been added
    """
    baker.make("references.PSC", code="1005", description="whatever")
    baker.make("references.PSC", code="10", description="whatever", start_date="1978-12-31")

    call_command("load_psc", path=f"{Path(__file__).parent / '..' / 'data' / 'psc_test_data.xlsx'}")

    assert PSC.objects.all().count() == 8

    psc = PSC.objects.get(pk="1005")
    psc2 = PSC.objects.get(pk="10")

    # this is different than the number we would get if we ran on the current table,
    # since there are some codes not included in the file
    assert psc.description == "GUNS, THROUGH 30MM"
    assert psc.length == 4
    assert psc.full_name == "Guns, through 30 mm"
    assert psc.includes == "Machine guns; Brushes, Machine Gun and Pistol."
    assert psc.excludes == "Turrets, Aircraft."
    assert psc.notes is None
    assert psc.start_date == datetime.strptime("2011-10-01", "%Y-%m-%d").date()
    assert psc.end_date is None

    assert psc2.description == "WEAPONS"
    assert psc2.length == 2
    assert psc2.full_name is None
    assert psc2.includes is None
    assert psc2.excludes is None
    assert psc2.notes == (
        "This group includes combat weapons as well as weapon-like noncombat items, such as line throwing devices "
        "and pyrotechnic pistols. Also included in this group are weapon neutralizing equipment, such as degaussers, "
        "and deception equipment, such as camouflage nets. Excluded from this group are fire control and night devices "
        "classifiable in groups 12 or 58."
    )
    assert psc2.start_date == datetime.strptime("1979-10-1", "%Y-%m-%d").date()
    assert psc2.end_date is None
