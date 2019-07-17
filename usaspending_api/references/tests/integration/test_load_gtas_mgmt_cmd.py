# Stdlib imports

# Core Django imports
from django.core.management import call_command

# Third-party app imports
import pytest
from unittest.mock import MagicMock

# Imports from your apps
from usaspending_api.references.models import GTASTotalObligation


DB_CURSOR_PARAMS = {
    "default": MagicMock(),
    "data_broker": MagicMock(),
    "data_broker_data_file": "usaspending_api/references/tests/data/broker_gtas.json",
}


@pytest.mark.django_db
@pytest.mark.parametrize("mock_db_cursor", [DB_CURSOR_PARAMS], indirect=True)
def test_program_activity_fresh_load(mock_db_cursor):
    """
        Test the gtas totals load to ensure data is loaded with the correct totals.
    """

    call_command("load_gtas")

    expected_results = {"count": 3, "row_tuples": [(1600, -1, -10), (1600, -2, -1), (1601, -1, -10)]}

    actual_results = {
        "count": GTASTotalObligation.objects.count(),
        "row_tuples": list(
            GTASTotalObligation.objects.values_list("fiscal_year", "fiscal_quarter", "total_obligation")
        ),
    }

    assert expected_results == actual_results
