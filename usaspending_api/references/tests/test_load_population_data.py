from pathlib import Path

import pandas as pd
import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from usaspending_api.references.models import PopCounty

TEST_CSV_FILE = Path(__file__).resolve().parent / "data" / "census_2020_population_county.csv"


@pytest.mark.django_db
def test_load_county_population_data():
    """Test that the load_population_data command successfully imports the CSV file"""

    # Count the rows in the CSV file, minus the header row
    csv_file_row_count = len(pd.read_csv(TEST_CSV_FILE)) - 1

    # Should be 0 objects in this table before the import
    assert PopCounty.objects.count() == 0

    call_command(
        "load_population_data",
        file="https://files.usaspending.gov/reference_data/census_2019_population_county.csv",
        type="county",
    )
    assert PopCounty.objects.count() == csv_file_row_count
