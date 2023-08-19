import pytest

from model_bakery import baker
from rest_framework import status


@pytest.fixture
def create_agency_data():
    # Create agency - submission relationship
    # Create AGENCY AND TopTier AGENCY
    ttagency1 = baker.make(
        "references.ToptierAgency",
        name="TTA Name",
        toptier_code="100",
        abbreviation="tta_abrev",
        justification="test.com/cj",
    )
    ttagency2 = baker.make(
        "references.ToptierAgency", name="TTA Name 2", toptier_code="200", abbreviation="tta_abrev_2"
    )

    baker.make("references.Agency", id=1, toptier_agency=ttagency1, toptier_flag=True, _fill_optional=True)
    baker.make("references.Agency", id=2, toptier_agency=ttagency2, toptier_flag=True, _fill_optional=True)

    # create TAS
    tas = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency1)
    tas2 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency2)

    # Create Submissions
    dsws1 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_reveal_date="2017-02-01",
        submission_fiscal_quarter=2,
        submission_fiscal_month=4,
        is_quarter=False,
    )
    dsws2 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_reveal_date="2017-04-01",
        submission_fiscal_quarter=2,
        submission_fiscal_month=6,
        is_quarter=True,
    )
    dsws3 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2017,
        submission_reveal_date="2017-04-01",
        submission_fiscal_quarter=2,
        submission_fiscal_month=6,
        is_quarter=False,
    )
    baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_quarter=2,
        reporting_fiscal_period=4,
        toptier_code="100",
        is_final_balances_for_fy=False,
        submission_window=dsws1,
    )
    submission_1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_quarter=2,
        reporting_fiscal_period=6,
        toptier_code="100",
        is_final_balances_for_fy=True,
        submission_window=dsws2,
    )
    submission_2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2017,
        reporting_fiscal_quarter=2,
        reporting_fiscal_period=6,
        toptier_code="200",
        is_final_balances_for_fy=True,
        submission_window=dsws3,
    )

    # CREATE AppropriationAccountBalances
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        reporting_period_start="2017-1-1",
        submission=submission_1,
        total_budgetary_resources_amount_cpe=2,
        obligations_incurred_total_by_tas_cpe=2,
        gross_outlay_amount_by_tas_cpe=2,
        treasury_account_identifier=tas,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        final_of_fy=True,
        reporting_period_start="2017-1-1",
        submission=submission_2,
        total_budgetary_resources_amount_cpe=14,
        obligations_incurred_total_by_tas_cpe=14,
        gross_outlay_amount_by_tas_cpe=14,
        treasury_account_identifier=tas2,
    )

    baker.make(
        "references.GTASSF133Balances",
        total_budgetary_resources_cpe=100.00,
        obligations_incurred_total_cpe=1000.00,
        fiscal_year=2017,
        fiscal_period=4,
    )
    baker.make(
        "references.GTASSF133Balances",
        total_budgetary_resources_cpe=200.00,
        obligations_incurred_total_cpe=2000.00,
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
                "agency_name": "TTA Name",
                "congressional_justification_url": "test.com/cj",
                "budget_authority_amount": 2.0,
                "current_total_budget_authority_amount": 200.00,
                "obligated_amount": 2.0,
                "outlay_amount": 2.0,
                "percentage_of_total_budget_authority": 0.01,
                "toptier_code": "100",
                "agency_slug": "tta-name",
            },
            {
                "abbreviation": "tta_abrev_2",
                "active_fq": "2",
                "active_fy": "2017",
                "agency_id": 2,
                "agency_name": "TTA Name 2",
                "congressional_justification_url": None,
                "budget_authority_amount": 14.0,
                "current_total_budget_authority_amount": 200.00,
                "obligated_amount": 14.0,
                "outlay_amount": 14.00,
                "percentage_of_total_budget_authority": 0.07,
                "toptier_code": "200",
                "agency_slug": "tta-name-2",
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
                "agency_name": "TTA Name 2",
                "congressional_justification_url": None,
                "budget_authority_amount": 14.0,
                "current_total_budget_authority_amount": 200.00,
                "obligated_amount": 14.0,
                "outlay_amount": 14.0,
                "percentage_of_total_budget_authority": 0.07,
                "toptier_code": "200",
                "agency_slug": "tta-name-2",
            },
            {
                "abbreviation": "tta_abrev",
                "active_fq": "2",
                "active_fy": "2017",
                "agency_id": 1,
                "agency_name": "TTA Name",
                "congressional_justification_url": "test.com/cj",
                "budget_authority_amount": 2.0,
                "current_total_budget_authority_amount": 200.00,
                "obligated_amount": 2.0,
                "outlay_amount": 2.0,
                "percentage_of_total_budget_authority": 0.01,
                "toptier_code": "100",
                "agency_slug": "tta-name",
            },
        ]
    }
