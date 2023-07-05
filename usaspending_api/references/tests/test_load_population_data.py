import pandas as pd
import pytest
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import CommandError

from usaspending_api.references.models import PopCounty, PopCongressionalDistrict

COUNTY_TEST_CSV_FILE = str(settings.APP_DIR / "references" / "tests" / "data" / "census_2020_population_county.csv")
CD_TEST_CSV_FILE = str(
    settings.APP_DIR / "references" / "tests" / "data" / "census_2021_population_congressional_district.csv"
)


def test_no_file_provided():
    """Command should throw an error if the `file` argument is not provided"""

    with pytest.raises(CommandError):
        call_command("load_population_data", type="county")
        call_command("load_population_data", type="district")


def test_no_type_provided():
    """Command should throw an error if the `type` argument is not provided"""

    with pytest.raises(CommandError):
        call_command("load_population_data", file=COUNTY_TEST_CSV_FILE)
        call_command("load_population_data", file=CD_TEST_CSV_FILE)


def test_no_file_or_type_provided():
    """Command should throw an error if both `type` and `file` arguments are not provided"""

    with pytest.raises(CommandError):
        call_command("load_population_data")


@pytest.mark.django_db()
def test_load_county_population_data():
    """Test that the load_population_data command successfully imports the county CSV file"""

    csv_file_row_count = len(pd.read_csv(COUNTY_TEST_CSV_FILE))

    # Should be 0 objects in this table before the import
    assert PopCounty.objects.count() == 0

    # Load the county data then re-check the row count
    call_command("load_population_data", type="county", file=COUNTY_TEST_CSV_FILE)
    assert PopCounty.objects.count() == csv_file_row_count


@pytest.mark.django_db()
def test_all_states_and_territories_present_county():
    """Test that all U.S. states and territories are present in the table"""

    df = pd.read_csv(COUNTY_TEST_CSV_FILE)
    call_command("load_population_data", type="county", file=COUNTY_TEST_CSV_FILE)

    assert len(df.state_name.unique()) == len(PopCounty.objects.all().distinct("state_name").values("state_name"))
    assert len(df.state_code.unique()) == len(PopCounty.objects.all().distinct("state_code").values("state_code"))


@pytest.mark.django_db()
def test_load_district_population_data():
    """Test that the load_population_data command successfully import the district CSV file"""

    csv_file_row_count = len(pd.read_csv(CD_TEST_CSV_FILE))

    # Should be 0 objects in this table before the import
    assert PopCongressionalDistrict.objects.count() == 0

    # Load the district data then re-check the row count
    call_command("load_population_data", type="district", file=CD_TEST_CSV_FILE)
    assert PopCongressionalDistrict.objects.count() == csv_file_row_count


@pytest.mark.django_db()
def test_all_states_and_territories_present_district():
    """Test that all U.S. states and territories are present in the table"""

    df = pd.read_csv(CD_TEST_CSV_FILE)
    call_command("load_population_data", type="district", file=CD_TEST_CSV_FILE)

    assert len(df.state_name.unique()) == len(
        PopCongressionalDistrict.objects.all().distinct("state_name").values("state_name")
    )
    assert len(df.state_code.unique()) == len(
        PopCongressionalDistrict.objects.all().distinct("state_code").values("state_code")
    )
