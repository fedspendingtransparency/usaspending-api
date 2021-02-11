import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def create_agency_data(db):
    # Create agency - submission relationship
    # Create AGENCY AND TopTier AGENCY
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

    # create TAS
    tas = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency1)

    # CREATE SUBMISSIONS
    # submission_3 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2015, toptier_code='100')
    submission_1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_quarter=2,
        toptier_code="100",
        is_final_balances_for_fy=True,
    )
    # submission_2 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2016, toptier_code='100')

    # CREATE AppropriationAccountBalances
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

    # CREATE OverallTotals
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

    # check for bad request due to missing params
    resp = client.get("/api/v2/references/agency/4/")
    assert resp.data == {"results": {}}
