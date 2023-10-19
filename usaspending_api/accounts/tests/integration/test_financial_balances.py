import pytest

from model_bakery import baker
from rest_framework import status


@pytest.fixture
def financial_balances_models():
    sub = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2016,
        toptier_code="abc",
        is_final_balances_for_fy=True,
    )
    agency1_toptier = baker.make(
        "references.ToptierAgency", toptier_agency_id=123, toptier_code="abc", _fill_optional=True
    )
    baker.make("references.Agency", id=456, toptier_agency_id=123, _fill_optional=True)
    tas1 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=agency1_toptier)
    tas2 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=agency1_toptier)
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=tas1,
        total_budgetary_resources_amount_cpe=1000,
        obligations_incurred_total_by_tas_cpe=2000,
        gross_outlay_amount_by_tas_cpe=3000,
        submission=sub,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        treasury_account_identifier=tas2,
        total_budgetary_resources_amount_cpe=1000,
        obligations_incurred_total_by_tas_cpe=2000.01,
        gross_outlay_amount_by_tas_cpe=-2000,
        submission=sub,
    )
    # throw in some random noise to ensure only the balances for the specified
    # agency are returned in the response
    baker.make("accounts.AppropriationAccountBalances", _quantity=3, _fill_optional=True)


@pytest.mark.django_db
def test_financial_balances_agencies(client, financial_balances_models):
    """Test the financial_balances/agencies endpoint."""
    resp = client.get("/api/v2/financial_balances/agencies/?funding_agency_id=456&fiscal_year=2016")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1
    result = resp.data["results"][0]
    assert result["budget_authority_amount"] == "2000.00"
    assert result["obligated_amount"] == "4000.01"
    assert result["outlay_amount"] == "1000.00"


@pytest.mark.django_db
def test_financial_balances_agencies_params(client, financial_balances_models):
    """Test invalid financial_balances/agencies parameters."""
    # funding_agency_id is missing
    resp = client.get("/api/v2/financial_balances/agencies/?fiscal_year=2016")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
