import pytest

from model_bakery import baker
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
def test_invalid_agency(client, bureau_data):
    resp = client.get(url.format(toptier_code="XXX", filter="?fiscal_year=2021"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
def test_exclusion_bureau_codes(client):
    # Setup all Data (no bureau)
    ta1 = baker.make("references.ToptierAgency", name="Agency 1", toptier_code="001", _fill_optional=True)
    sa1 = baker.make("references.SubtierAgency", name="Agency 1", subtier_code="0001", _fill_optional=True)
    baker.make(
        "references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, subtier_agency=sa1, _fill_optional=True
    )

    fa1 = baker.make(
        "accounts.FederalAccount", account_title="FA 1", federal_account_code="001-0000", parent_toptier_agency=ta1
    )
    taa1 = baker.make("accounts.TreasuryAppropriationAccount", federal_account=fa1)

    dabs1 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-01-01",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=True,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )

    sub_2020_ta1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs1.id,
    )

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        treasury_account=taa1,
        submission=sub_2020_ta1,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=10,
        deobligations_recoveries_refund_pri_program_object_class_cpe=0,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=0,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=0,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=0,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=0,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=0,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=0,
        ussgl490200_delivered_orders_obligations_paid_cpe=0,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=0,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=0,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=0,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=0,
        ussgl480110_rein_undel_ord_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=0,
    )
    baker.make(
        "accounts.AppropriationAccountBalances",
        treasury_account_identifier=taa1,
        submission=sub_2020_ta1,
        total_budgetary_resources_amount_cpe=100,
    )

    # Request will return no results because Federal Account has no Bureau Lookup
    resp = client.get(url.format(toptier_code="001", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0

    # Create Bureau Lookup for existing Federal Account
    baker.make(
        "references.BureauTitleLookup",
        federal_account_code="001-0000",
        bureau_title="New Bureau",
        bureau_slug="new-bureau",
    )

    # Request will now return results because matching Bureau Lookup exists
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
