import pytest
from django.core.management import call_command
from datetime import datetime
from usaspending_api.references.models import PSC


@pytest.mark.django_db
def test_load_psc():
    """
    Test to ensure the ingested data size is correct
    Test to make sure previously missing information has been added

    """

    call_command("load_psc")

    psc_count = PSC.objects.all().count()

    psc = PSC.objects.get(pk="1005")

    assert psc_count == 3193
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
