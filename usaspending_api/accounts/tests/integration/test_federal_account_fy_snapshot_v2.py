import pytest

from model_bakery import baker
from rest_framework import status
from usaspending_api.accounts.models import FederalAccount


@pytest.fixture
def financial_spending_data(db):
    latest_subm = baker.make(
        "submissions.SubmissionAttributes", reporting_fiscal_year=2017, is_final_balances_for_fy=True
    )
    not_latest_subm = baker.make(
        "submissions.SubmissionAttributes", reporting_fiscal_year=2017, is_final_balances_for_fy=False
    )
    last_year_subm = baker.make(
        "submissions.SubmissionAttributes", reporting_fiscal_year=2016, is_final_balances_for_fy=True
    )
    federal_account = baker.make(FederalAccount, id=1)

    # create Object classes
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier__federal_account=federal_account,
        final_of_fy=True,
        submission=latest_subm,
        gross_outlay_amount_by_tas_cpe=1000000,
        total_budgetary_resources_amount_cpe=2000000,
        obligations_incurred_total_by_tas_cpe=3000000,
        unobligated_balance_cpe=4000000,
        budget_authority_unobligated_balance_brought_forward_fyb=5000000,
        adjustments_to_unobligated_balance_brought_forward_cpe=6000000,
        other_budgetary_resources_amount_cpe=7000000,
        budget_authority_appropriated_amount_cpe=8000000,
    )

    # these AAB records should not show up in the endpoint, they are too old
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier__federal_account=federal_account,
        final_of_fy=False,
        submission=not_latest_subm,
        gross_outlay_amount_by_tas_cpe=999,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier__federal_account=federal_account,
        final_of_fy=True,
        submission=last_year_subm,
        gross_outlay_amount_by_tas_cpe=999,
    )


def test_federal_account_fiscal_year_snapshot_v2_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get("/api/v2/federal_accounts/1/fiscal_year_snapshot")
    assert resp.status_code == status.HTTP_200_OK

    # test response in correct form

    assert "results" in resp.json()
    results = resp.json()["results"]
    assert "outlay" in results
    assert "budget_authority" in results
    assert "obligated" in results
    assert "unobligated" in results
    assert "balance_brought_forward" in results
    assert "other_budgetary_resources" in results
    assert "appropriations" in results

    assert results["outlay"] == 1000000
    assert results["budget_authority"] == 2000000
    assert results["obligated"] == 3000000
    assert results["unobligated"] == 4000000
    assert results["balance_brought_forward"] == 11000000
    assert results["other_budgetary_resources"] == 7000000
    assert results["appropriations"] == 8000000


def test_federal_account_fiscal_year_snapshot_v2_endpoint_no_results(client, financial_spending_data):
    """Test response when no AAB records found."""

    resp = client.get("/api/v2/federal_accounts/999/fiscal_year_snapshot")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == {}


def test_federal_account_fiscal_year_snapshot_v2_endpoint_specific_fy(client, financial_spending_data):
    """Test that fy parameter accepted and honored."""

    resp = client.get("/api/v2/federal_accounts/1/fiscal_year_snapshot/2016")
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()["results"]
    assert results["outlay"] == 999
