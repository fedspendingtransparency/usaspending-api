import pytest

from unittest.mock import MagicMock

from django.conf import settings
from django.core.management import call_command

from usaspending_api.etl.broker_etl_helpers import PhonyCursor
from usaspending_api.references.models import Office


@pytest.mark.django_db
def test_load_offices(monkeypatch):
    """
    Test the office load to ensure the data is being pulled over from broker correctly
    """

    data_broker_mock = MagicMock()
    data_broker_mock.cursor.return_value = PhonyCursor("usaspending_api/references/tests/data/broker_offices.json")
    mock_connections = {
        settings.DEFAULT_DB_ALIAS: MagicMock(),
        settings.DATA_BROKER_DB_ALIAS: data_broker_mock,
    }

    monkeypatch.setattr("usaspending_api.references.management.commands.load_offices.connections", mock_connections)

    call_command("load_offices")

    expected_results = {
        "count": 3,
        "row_tuples": [
            ("033103", "LIBRARY OF CONGRESS FEDLINK", "0300", "003", True, False, False, True),
            ("040897", "Customer Services", "0400", "004", False, False, False, False),
            ("040ADV", "Acquisition Services", "0400", "004", False, False, False, False),
        ],
    }

    actual_results = {
        "count": Office.objects.count(),
        "row_tuples": list(
            Office.objects.values_list(
                "office_code",
                "office_name",
                "sub_tier_code",
                "agency_code",
                "contract_awards_office",
                "financial_assistance_awards_office",
                "financial_assistance_funding_office",
                "contract_funding_office",
            ).order_by("office_code")
        ),
    }

    assert expected_results == actual_results
