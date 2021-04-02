import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def create_agency_data():
    # Create agency - submission relationship
    # Create AGENCY AND TopTier AGENCY
    ttagency1 = mommy.make(
        "references.ToptierAgency",
        name="tta_name",
        toptier_code="100",
        abbreviation="tta_abrev",
        justification="test.com/cj",
    )
    ttagency2 = mommy.make(
        "references.ToptierAgency", name="tta_name_2", toptier_code="200", abbreviation="tta_abrev_2"
    )

    mommy.make("references.Agency", id=1, toptier_agency=ttagency1, toptier_flag=True)
    mommy.make("references.Agency", id=2, toptier_agency=ttagency2, toptier_flag=True)

    # create TAS
    tas = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency2)

    # CREATE SUBMISSIONS
    # submission_3 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2015, toptier_code='100')
    submission_1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_quarter=2,
        reporting_fiscal_period=6,
        toptier_code="100",
        is_final_balances_for_fy=True,
    )
    # submission_2 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2016, toptier_code='100')
    submission_2 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_quarter=2,
        reporting_fiscal_period=6,
        toptier_code="200",
        is_final_balances_for_fy=True,
    )

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
    mommy.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        reporting_period_start="2017-1-1",
        submission=submission_2,
        total_budgetary_resources_amount_cpe=14,
        obligations_incurred_total_by_tas_cpe=14,
        gross_outlay_amount_by_tas_cpe=14,
        treasury_account_identifier=tas2,
    )

    mommy.make(
        "references.GTASSF133Balances",
        total_budgetary_resources_cpe=100.00,
        obligations_incurred_total_cpe=100.00,
        fiscal_year=2017,
        fiscal_period=6,
    )


@pytest.mark.django_db
def test_award_type_endpoint(client, create_agency_data):
    """Test the toptier_agency endpoint."""

    resp = client.get("/api/v2/references/toptier_agencies/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [
            {
                "abbreviation": "tta_abrev",
                "active_fq": "2",
                "active_fy": "2017",
                "agency_id": 1,
                "agency_name": "tta_name",
                "congressional_justification_url": "test.com/cj",
                "budget_authority_amount": 2.0,
                "current_total_budget_authority_amount": 100.00,
                "obligated_amount": 2.0,
                "outlay_amount": 2.0,
                "percentage_of_total_budget_authority": 0.02,
                "toptier_code": "100",
            },
            {
                "abbreviation": "tta_abrev_2",
                "active_fq": "2",
                "active_fy": "2017",
                "agency_id": 2,
                "agency_name": "tta_name_2",
                "congressional_justification_url": None,
                "budget_authority_amount": 14.0,
                "current_total_budget_authority_amount": 100.00,
                "obligated_amount": 14.0,
                "outlay_amount": 14.00,
                "percentage_of_total_budget_authority": 0.14,
                "toptier_code": "200",
            },
        ]
    }

    resp = client.get("/api/v2/references/toptier_agencies/?sort=budget_authority_amount&order=desc")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {
        "results": [
            {
                "abbreviation": "tta_abrev_2",
                "active_fq": "2",
                "active_fy": "2017",
                "agency_id": 2,
                "agency_name": "tta_name_2",
                "congressional_justification_url": None,
                "budget_authority_amount": 14.0,
                "current_total_budget_authority_amount": 10.00,
                "obligated_amount": 14.0,
                "outlay_amount": 14.0,
                "percentage_of_total_budget_authority": 0.14,
                "toptier_code": "200",
            },
            {
                "abbreviation": "tta_abrev",
                "active_fq": "2",
                "active_fy": "2017",
                "agency_id": 1,
                "agency_name": "tta_name",
                "congressional_justification_url": "test.com/cj",
                "budget_authority_amount": 2.0,
                "current_total_budget_authority_amount": 10.00,
                "obligated_amount": 2.0,
                "outlay_amount": 2.0,
                "percentage_of_total_budget_authority": 0.02,
                "toptier_code": "100",
            },
        ]
    }
