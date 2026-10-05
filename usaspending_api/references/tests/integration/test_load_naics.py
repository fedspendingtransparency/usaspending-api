from argparse import ArgumentTypeError

import pytest
from django.conf import settings
from django.core.management import call_command

from usaspending_api.references.models import NAICS


@pytest.mark.django_db
def test_naics_loader():
    """
    Test to ensure the ingested data size is correct
    Test to make sure previously missing information has been added

    """

    call_command("load_naics", "--overwrite", f"--path={settings.APP_DIR}/references/tests/data/naics_subset/")

    naics = NAICS.objects.all()
    naics_count_2022 = naics.filter(year=2022).count()
    naics_count_2017 = naics.filter(year=2017).count()
    naics_count_2012 = naics.filter(year=2012).count()
    naics_count_2002 = naics.filter(year=2002).count()
    naics_count_all = naics.count()
    naics_long_desc_2017 = NAICS.objects.get(code=112420)
    naics_retired_count_2012 = naics.filter(year_retired=2012).count()
    naics_retired_count_2002 = naics.filter(year_retired=2002).count()
    naics_retired_count_2007 = naics.filter(year_retired=2007).count()
    naics_retired_count_2017 = naics.filter(year_retired=2017).count()
    naics_retired_count_2022 = naics.filter(year_retired=2022).count()

    naics_2012_named_entry = NAICS.objects.get(code=112990)

    assert naics_2012_named_entry is not None
    assert naics_count_2002 == 0
    assert naics_count_2012 == 0
    assert naics_count_2017 == 1
    assert naics_count_2022 == 61
    assert naics_count_all == 62
    assert (
        naics_long_desc_2017.long_description
        == "This industry comprises establishments primarily engaged in raising goats."
    )
    assert naics_retired_count_2012 == 0
    assert naics_retired_count_2002 == 0
    assert naics_retired_count_2007 == 0
    assert naics_retired_count_2017 == 0
    assert naics_retired_count_2022 == 1


@pytest.mark.django_db
def test_naics_command_with_append_overwrite():
    with pytest.raises(ArgumentTypeError) as e:
        call_command("load_naics")

    assert "command must supply either --overwrite or --append" in str(e.value)


@pytest.mark.django_db
def test_naics_with_append_and_overwrite():
    with pytest.raises(ArgumentTypeError) as e:
        call_command("load_naics", "--overwrite", "--append")

    assert "command must supply either --overwrite or --append" in str(e.value)
