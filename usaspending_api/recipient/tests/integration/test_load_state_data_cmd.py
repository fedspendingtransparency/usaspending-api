# Stdlib imports

# Core Django imports
from django.core.management import call_command

# Third-party app imports
import pytest

# Imports from your apps
from usaspending_api.recipient.models import StateData


@pytest.mark.django_db
def test_program_activity_fresh_load():
    """
    Test the state data load to ensure data is loaded

    A unique set is defined by the following columns:
        fips code
        year
    """

    call_command("load_state_data", "usaspending_api/recipient/tests/data/CensusStateData.csv")

    expected_results = {
        "count": 448,
        "states_count": 50,
        "territories_count": 5,
        "districts_count": 1,
        "years_count": 8,
    }

    def count_type(type):
        return StateData.objects.filter(type=type).values("type", "fips").distinct("fips").count()

    actual_results = {
        "count": StateData.objects.count(),
        "states_count": count_type("state"),
        "territories_count": count_type("territory"),
        "districts_count": count_type("district"),
        "years_count": StateData.objects.distinct("year").count(),
    }

    assert expected_results == actual_results
