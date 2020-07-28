import pytest

from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS
from unittest.mock import MagicMock
from usaspending_api.etl.broker_etl_helpers import PhonyCursor
from usaspending_api.references.models import GTASSF133Balances


@pytest.mark.django_db
def test_program_activity_fresh_load(monkeypatch):
    """
    Test the gtas totals load to ensure data is loaded with the correct totals.
    """

    data_broker_mock = MagicMock()
    data_broker_mock.cursor.return_value = PhonyCursor("usaspending_api/references/tests/data/broker_gtas.json")
    mock_connections = {
        DEFAULT_DB_ALIAS: MagicMock(),
        "data_broker": data_broker_mock,
    }

    monkeypatch.setattr("usaspending_api.references.management.commands.load_gtas.connections", mock_connections)

    call_command("load_gtas")

    expected_results = {
        "count": 3,
        "row_tuples": [
            (1600, -1, -10.00, -11.00, -11.00, -11.00, 11),
            (1600, -2, -9.00, -12.00, -12.00, -12.00, 12),
            (1601, -1, -8.00, -13.00, -13.00, -13.00, 13),
        ],
    }

    actual_results = {
        "count": GTASSF133Balances.objects.count(),
        "row_tuples": list(
            GTASSF133Balances.objects.values_list(
                "fiscal_year",
                "fiscal_period",
                "obligations_incurred_total_cpe",
                "budget_authority_appropriation_amount_cpe",
                "other_budgetary_resources_amount_cpe",
                "unobligated_balance_cpe",
                "total_budgetary_resources_cpe",
            )
        ),
    }

    assert expected_results == actual_results
