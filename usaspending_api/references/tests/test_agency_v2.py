import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def create_agency_data(db):
    # Create Agency - Submission relationship
    # Create Agency AND TopTier Agency
    ttagency1 = mommy.make(
        "references.ToptierAgency",
        name="tta_name",
        toptier_code="100",
        website="http://test.com",
        mission="test",
        about_agency_data="about agency data test",
        icon_filename="test",
        justification="test.com/cj",
    )
    mommy.make("references.Agency", id=1, toptier_agency=ttagency1)

    # Create TAS
    tas = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency1)

    # Create GTAS
    mommy.make("references.GTASSF133Balances", fiscal_year=2017, fiscal_period=4, total_budgetary_resources_cpe=1000)
    mommy.make("references.GTASSF133Balances", fiscal_year=2017, fiscal_period=6, total_budgetary_resources_cpe=2000)

    # Create Submissions
    mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_quarter=2,
        reporting_fiscal_period=4,
        toptier_code="100",
        is_final_balances_for_fy=False,
    )
    submission_1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_quarter=2,
        reporting_fiscal_period=6,
        toptier_code="100",
        is_final_balances_for_fy=True,
    )

    # Create AppropriationAccountBalances
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        reporting_period_start="2017-1-1",
        submission=submission_1,
        total_budgetary_resources_amount_cpe=2,
        obligations_incurred_total_by_tas_cpe=2,
        gross_outlay_amount_by_tas_cpe=2,
        treasury_account_identifier=tas,
    )

    # Create OverallTotals
    mommy.make("references.OverallTotals", fiscal_year=2017, total_budget_authority=3860000000.00)


@pytest.mark.django_db
def test_agency_endpoint(client, create_agency_data):
    """Test the award_type endpoint."""

    resp = client.get("/api/v2/references/agency/1/")
    assert resp.status_code == status.HTTP_200_OK

    assert resp.data["results"]["outlay_amount"] == "2.00"
    assert resp.data["results"]["obligated_amount"] == "2.00"
    assert resp.data["results"]["budget_authority_amount"] == "2.00"
    assert resp.data["results"]["congressional_justification_url"] == "test.com/cj"
    assert resp.data["results"]["current_total_budget_authority_amount"] == "2000.00"

    # check for bad request due to missing params
    resp = client.get("/api/v2/references/agency/4/")
    assert resp.data == {"results": {}}
