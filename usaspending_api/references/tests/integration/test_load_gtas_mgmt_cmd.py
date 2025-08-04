import pytest

from unittest.mock import MagicMock

from django.conf import settings
from django.core.management import call_command
from model_bakery import baker

from usaspending_api.etl.broker_etl_helpers import PhonyCursor
from usaspending_api.references.models import GTASSF133Balances


@pytest.mark.django_db
def test_program_activity_fresh_load(monkeypatch):
    """
    Test the gtas totals load to ensure data is loaded with the correct totals.
    """

    baker.make("references.DisasterEmergencyFundCode", code="A")
    baker.make("accounts.TreasuryAppropriationAccount", tas_rendering_label="999-X-111")

    data_broker_mock = MagicMock()
    data_broker_mock.cursor.return_value = PhonyCursor("usaspending_api/references/tests/data/broker_gtas.json")
    mock_connections = {
        settings.DEFAULT_DB_ALIAS: MagicMock(),
        settings.DATA_BROKER_DB_ALIAS: data_broker_mock,
    }

    monkeypatch.setattr("usaspending_api.references.management.commands.load_gtas.connections", mock_connections)

    call_command("load_gtas")

    expected_results = {
        "count": 3,
        "row_tuples": [
            (
                1600,
                -1,
                -11.00,
                -11.00,
                -10.00,
                -11.00,
                -11.00,
                -11.00,
                -11.00,
                -11.00,
                -11.00,
                -11.00,
                -11.00,
                11,
                11,
                -111,
                -110,
                -11.00,
            ),
            (
                1600,
                -2,
                -12.00,
                -12.00,
                -9.00,
                -12.00,
                -12.00,
                -12.00,
                -12.00,
                -12.00,
                -12.00,
                -12.00,
                -12.00,
                12,
                12,
                -121,
                -120,
                -12.00,
            ),
            (
                1601,
                -1,
                -13.00,
                -13.00,
                -8.00,
                -13.00,
                -13.00,
                -13.00,
                -13.00,
                -13.00,
                -13.00,
                -13.00,
                -13.00,
                13,
                13,
                -131,
                -130,
                -13.00,
            ),
        ],
    }

    actual_results = {
        "count": GTASSF133Balances.objects.count(),
        "row_tuples": list(
            GTASSF133Balances.objects.values_list(
                "fiscal_year",
                "fiscal_period",
                "budget_authority_unobligated_balance_brought_forward_cpe",
                "adjustments_to_unobligated_balance_brought_forward_cpe",
                "obligations_incurred_total_cpe",
                "budget_authority_appropriation_amount_cpe",
                "borrowing_authority_amount",
                "contract_authority_amount",
                "spending_authority_from_offsetting_collections_amount",
                "other_budgetary_resources_amount_cpe",
                "obligations_incurred",
                "deobligations_or_recoveries_or_refunds_from_prior_year_cpe",
                "unobligated_balance_cpe",
                "total_budgetary_resources_cpe",
                "status_of_budgetary_resources_total_cpe",
                "anticipated_prior_year_obligation_recoveries",
                "prior_year_paid_obligation_recoveries",
                "adjustments_to_unobligated_balance_brought_forward_cpe",
            ).order_by("-budget_authority_unobligated_balance_brought_forward_cpe")
        ),
    }
    assert expected_results == actual_results
