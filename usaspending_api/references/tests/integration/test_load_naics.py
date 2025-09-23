import pytest
from django.core.management import call_command

from usaspending_api.references.models import NAICS


@pytest.mark.django_db
def test_naics_existing_and_new_files():
    """
    Test to ensure the ingested data size is correct
    Test to make sure previously missing information has been added

    """

    call_command("load_naics")

    naics = NAICS.objects.all()
    naics_count_2017 = naics.filter(year=2017).count()
    naics_count_2012 = naics.filter(year=2012).count()
    naics_count_2002 = naics.filter(year=2002).count()
    naics_count_all = naics.count()
    naics_long_desc_2017 = NAICS.objects.get(pk=111411)
    naics_retired_count_2012 = naics.filter(year_retired=2012).count()
    naics_retired_count_2002 = naics.filter(year_retired=2002).count()
    naics_retired_count_2007 = naics.filter(year_retired=2007).count()
    naics_retired_count_2017 = naics.filter(year_retired=2017).count()

    naics_2012_named_entry = NAICS.objects.get(pk=541712)

    assert naics_2012_named_entry is not None
    assert naics_count_2002 == 15
    assert naics_count_2012 == 32
    assert naics_count_2017 == 1392
    assert naics_count_all == 1627
    assert (
        naics_long_desc_2017.long_description
        == "This U.S. industry comprises establishments primarily engaged in growing mushrooms under cover in mines underground, or in other controlled environments."
    )
    assert naics_retired_count_2012 == 188
    assert naics_retired_count_2002 == 0
    assert naics_retired_count_2007 == 15
    assert naics_retired_count_2017 == 32
