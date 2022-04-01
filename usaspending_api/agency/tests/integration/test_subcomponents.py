import pytest

from model_mommy import mommy

from rest_framework import status

from usaspending_api.agency.tests.integration.conftest import CURRENT_FISCAL_YEAR

url = "/api/v2/agency/{toptier_code}/sub_components/{filter}"


@pytest.mark.django_db
def test_success(client, bureau_data, helpers):
    resp = client.get(url.format(toptier_code="001", filter=f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}"))

    expected_results = [
        {
            "name": "Test Bureau 1",
            "id": "test-bureau-1",
            "total_obligations": 1.0,
            "total_outlays": 10.0,
            "total_budgetary_resources": 100.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_year(client, bureau_data):
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Test Bureau 1",
            "id": "test-bureau-1",
            "total_obligations": 20.0,
            "total_outlays": 200.0,
            "total_budgetary_resources": 2000.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_alternate_agency(client, bureau_data):
    resp = client.get(url.format(toptier_code="002", filter="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK

    expected_results = [
        {
            "name": "Test Bureau 2",
            "id": "test-bureau-2",
            "total_obligations": 20.0,
            "total_outlays": 200.0,
            "total_budgetary_resources": 2000.0,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

@pytest.mark.django_db
def test_exclusion_bureau_codes(client, bureau_data):

    CURRENT_FISCAL_YEAR=2020

    ta1 = mommy.make("references.ToptierAgency", name="Agency 1", toptier_code="001")
    sa1 = mommy.make("references.SubtierAgency", name="Agency 1", subtier_code="0001")
    mommy.make("references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, subtier_agency=sa1)

    fa1 = mommy.make(
        "accounts.FederalAccount", account_title="FA 1", federal_account_code="001-0000", parent_toptier_agency=ta1
    )
    taa1 = mommy.make("accounts.TreasuryAppropriationAccount", federal_account=fa1)

    dabs1 = mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-01-01",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=True,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )

    sub_2020_ta1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs1.id,
    )

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=taa1,
        submission=sub_2020_ta1,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=10,
    )
    mommy.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=taa1,
        submission=sub_2020_ta1,
        total_budgetary_resources_amount_cpe=100,
    )

    # Setup all Data (no bureau)

    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0

    mommy.make(
        "references.BureauTitleLookup",
        federal_account_code="001-0000",
        bureau_title="New Bureau",
        bureau_slug="new-bureau",
    )

    # Setup bureau data
    expected_results = [
        {
            "name": "New Bureau",
            "id": "new-bureau",
            "total_obligations": 1,
            "total_outlays": 10,
            "total_budgetary_resources": 100,
        }
    ]
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2020"))
    assert resp.json()["results"] == expected_results
