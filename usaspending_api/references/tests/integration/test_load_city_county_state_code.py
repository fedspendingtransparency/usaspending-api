from decimal import Decimal

import pytest

from datetime import date
from django.core.management import call_command
from django.core.management.base import CommandError
from model_bakery import baker
from usaspending_api.references.models import CityCountyStateCode
from usaspending_api.search.models import SubawardSearch
from usaspending_api import settings


TEST_TXT_FILE = settings.APP_DIR / "references" / "tests" / "data" / "NationalFedCodesTest.txt"
TEST_ZIP_FILE = settings.APP_DIR / "references" / "tests" / "data" / "NationalFedCodesTest.zip"


@pytest.fixture
def disable_vacuuming(monkeypatch):
    """
    We cannot run vacuums in a transaction.  Since tests are run in a transaction, we'll NOOP the
    function that performs the vacuuming.
    """
    monkeypatch.setattr(
        "usaspending_api.references.management.commands.load_city_county_state_code.Command._vacuum_tables",
        lambda a: None,
    )


@pytest.mark.django_db(transaction=True)
def test_txt_file():
    """One full test to ensure our vacuuming works.  The rest will be sans vacuum."""
    assert CityCountyStateCode.objects.count() == 0
    call_command("load_city_county_state_code", TEST_TXT_FILE, "--force")
    assert CityCountyStateCode.objects.count() > 0

    ccsc = CityCountyStateCode.objects.filter(feature_id=479).first()

    assert ccsc.feature_id == 479
    assert ccsc.feature_name == "Adamana"
    assert ccsc.feature_class == "Populated Place"
    assert ccsc.census_code == "00170"
    assert ccsc.census_class_code == "U6"
    assert ccsc.gsa_code is None
    assert ccsc.opm_code is None
    assert ccsc.state_numeric == "04"
    assert ccsc.state_alpha == "AZ"
    assert ccsc.county_sequence == 1
    assert ccsc.county_numeric == "001"
    assert ccsc.county_name == "Apache"
    assert ccsc.primary_latitude == Decimal("34.97669210")
    assert ccsc.primary_longitude == Decimal("-109.82233830")
    assert ccsc.date_created == date(1980, 2, 8)
    assert ccsc.date_edited is None


@pytest.mark.django_db
def test_zip_file(disable_vacuuming):
    assert CityCountyStateCode.objects.count() == 0
    call_command("load_city_county_state_code", TEST_ZIP_FILE, "--force")
    assert CityCountyStateCode.objects.count() > 0


@pytest.mark.django_db
def test_no_file_provided():

    # This should error since agency file is required.
    with pytest.raises(CommandError):
        call_command("load_city_county_state_code")


@pytest.mark.django_db
def test_change_reversion(disable_vacuuming):
    """Let's make some very targeted changes and ensure that they are all resolved correctly on a followup load."""

    # Mock a subaward.
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        sub_place_of_perform_city_name="AGUA FRIA",
        sub_place_of_perform_state_code="AZ",
        sub_legal_entity_city_name="AJO",
        sub_legal_entity_state_code="AZ",
    )

    call_command("load_city_county_state_code", TEST_TXT_FILE, "--force")

    s = SubawardSearch.objects.get(broker_subaward_id=1)
    assert s.sub_place_of_perform_county_code == "013"
    assert s.sub_place_of_perform_county_name == "MARICOPA"
    assert s.sub_place_of_perform_city_code == "00520"
    assert s.sub_legal_entity_county_code == "019"
    assert s.sub_legal_entity_county_name == "PIMA"
    assert s.sub_legal_entity_city_code == "00870"

    s.sub_place_of_perform_county_name = "bogus county name"
    s.sub_legal_entity_county_name = "bogus county name"
    s.save()

    # Renumber a feature id which will act as both a delete and an insert since feature id is part of the natural key.
    CityCountyStateCode.objects.filter(feature_id=479).update(feature_id=-479)

    # Change a "random" value.
    CityCountyStateCode.objects.filter(feature_id=492).update(feature_name="Bogus Feature Name")

    # Confirm everything is broken.
    assert SubawardSearch.objects.get(broker_subaward_id=1).sub_place_of_perform_county_name != "MARICOPA"
    assert SubawardSearch.objects.get(broker_subaward_id=1).sub_legal_entity_county_name != "PIMA"
    assert CityCountyStateCode.objects.filter(feature_id=-479).count() == 1
    assert CityCountyStateCode.objects.filter(feature_id=479).count() == 0
    assert CityCountyStateCode.objects.filter(feature_id=492).first().feature_name != "Adobe"

    # Reload
    call_command("load_city_county_state_code", TEST_TXT_FILE, "--force")

    # Ensure everything got fixed.
    assert SubawardSearch.objects.get(broker_subaward_id=1).sub_place_of_perform_county_name == "MARICOPA"
    assert SubawardSearch.objects.get(broker_subaward_id=1).sub_legal_entity_county_name == "PIMA"
    assert CityCountyStateCode.objects.filter(feature_id=-479).count() == 0
    assert CityCountyStateCode.objects.filter(feature_id=479).count() == 1
    assert CityCountyStateCode.objects.filter(feature_id=492).first().feature_name == "Adobe"


@pytest.mark.django_db
def test_exceeding_max_changes(disable_vacuuming):
    """
    We have a safety cutoff to prevent accidentally updating every record in the database.  Make
    sure we get an exception if we exceed that threshold.
    """
    call_command("load_city_county_state_code", TEST_TXT_FILE, "--force")

    # Make a change to every record.
    CityCountyStateCode.objects.update(gsa_code="bogus code")

    # This should fail.
    with pytest.raises(RuntimeError):
        call_command("load_city_county_state_code", TEST_TXT_FILE)

    # But this should succeed.
    call_command("load_city_county_state_code", TEST_TXT_FILE, "--force")
