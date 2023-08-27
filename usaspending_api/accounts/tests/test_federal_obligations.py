import pytest

from model_bakery import baker
from rest_framework import status


@pytest.fixture
def financial_obligations_models():
    fiscal_year = baker.make(
        "submissions.SubmissionAttributes", reporting_fiscal_year=2016, is_final_balances_for_fy=True
    )
    fiscal_year_2 = baker.make(
        "submissions.SubmissionAttributes", reporting_fiscal_year=2016, is_final_balances_for_fy=False
    )
    top_tier_id = baker.make("references.Agency", id=654, toptier_agency_id=987, _fill_optional=True).toptier_agency_id
    top_tier = baker.make("references.ToptierAgency", toptier_agency_id=top_tier_id, _fill_optional=True)
    federal_id_awesome = baker.make(
        "accounts.FederalAccount",
        id=6969,
        agency_identifier="867",
        main_account_code="5309",
        federal_account_code="867-5309",
        account_title="Turtlenecks and Chains",
    )
    federal_id_lame = baker.make(
        "accounts.FederalAccount",
        id=1234,
        agency_identifier="314",
        main_account_code="1592",
        federal_account_code="314-1592",
        account_title="Suits and Ties",
    )
    """
        Until this gets updated with mock.Mock(),
        the following cascade of variables applied to parameters,
        is the work around for annotating.
    """
    funding = baker.make(
        "accounts.TreasuryAppropriationAccount", funding_toptier_agency=top_tier
    ).funding_toptier_agency

    agency_name = baker.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=funding,
        reporting_agency_name="Department of Style",
    ).reporting_agency_name

    acct_awesome = baker.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=funding,
        reporting_agency_name=agency_name,
        account_title="Turtlenecks and Chains",
    ).account_title

    id_awesome = baker.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=funding,
        reporting_agency_name=agency_name,
        account_title=acct_awesome,
        federal_account=federal_id_awesome,
    )

    acct_lame = baker.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=top_tier,
        reporting_agency_name=agency_name,
        account_title="Suits and Ties",
    ).account_title

    id_lame = baker.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=funding,
        reporting_agency_name=agency_name,
        account_title=acct_lame,
        federal_account=federal_id_lame,
    )

    # AppropriationAccountBalances
    baker.make(
        "accounts.AppropriationAccountBalances",
        submission=fiscal_year,
        final_of_fy=True,
        obligations_incurred_total_by_tas_cpe=-100,
        treasury_account_identifier=id_awesome,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        submission=fiscal_year,
        final_of_fy=True,
        obligations_incurred_total_by_tas_cpe=200,
        treasury_account_identifier=id_awesome,
    )
    # Test to make sure False value is ignored in calculation
    baker.make(
        "accounts.AppropriationAccountBalances",
        submission=fiscal_year_2,
        final_of_fy=False,
        obligations_incurred_total_by_tas_cpe=200,
        treasury_account_identifier=id_awesome,
    )

    # Get Lame account values
    baker.make(
        "accounts.AppropriationAccountBalances",
        submission=fiscal_year,
        final_of_fy=True,
        obligations_incurred_total_by_tas_cpe=500,
        treasury_account_identifier=id_lame,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        submission=fiscal_year,
        final_of_fy=True,
        obligations_incurred_total_by_tas_cpe=-100,
        treasury_account_identifier=id_lame,
    )


@pytest.mark.django_db
def test_financial_obligations(client, financial_obligations_models):
    """Test the financial_obligations endpoint."""
    resp = client.get("/api/v2/federal_obligations/?funding_agency_id=654&fiscal_year=2016")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2
    res_awesome = resp.data["results"][0]
    assert res_awesome["id"] == "1234"
    assert res_awesome["account_number"] == "314-1592"
    assert res_awesome["account_title"] == "Suits and Ties"
    assert res_awesome["obligated_amount"] == "400.00"
    res_lame = resp.data["results"][1]
    assert res_lame["id"] == "6969"
    assert res_lame["account_number"] == "867-5309"
    assert res_lame["account_title"] == "Turtlenecks and Chains"
    assert res_lame["obligated_amount"] == "100.00"


@pytest.mark.django_db
def test_financial_obligations_params(client, financial_obligations_models):
    """Test invalid financial_obligations parameters."""
    resp = client.get("/api/v2/federal_obligations/?fiscal_year=2016")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
