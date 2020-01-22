import pytest

from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS
from unittest.mock import MagicMock
from usaspending_api.etl.broker_etl_helpers import PhonyCursor
from usaspending_api.references.models import GTASTotalObligation


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

    expected_results = {"count": 3, "row_tuples": [(1600, -1, -10), (1600, -2, -1), (1601, -1, -10)]}

    actual_results = {
        "count": GTASTotalObligation.objects.count(),
        "row_tuples": list(
            GTASTotalObligation.objects.values_list("fiscal_year", "fiscal_quarter", "total_obligation")
        ),
    }

    assert expected_results == actual_results
