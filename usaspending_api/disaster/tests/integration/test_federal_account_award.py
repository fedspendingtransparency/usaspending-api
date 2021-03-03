import pytest
from model_mommy import mommy

from rest_framework import status

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.submissions.models import SubmissionAttributes

url = "/api/v2/disaster/federal_account/spending/"


@pytest.mark.django_db
def test_federal_account_award_success(client, generic_account_data, monkeypatch, helpers, elasticsearch_account_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/99",
                    "award_count": 1,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 100.0,
                    "outlay": 111.0,
                    "total_budgetary_resources": None,
                }
            ],
            "code": "000-0000",
            "award_count": 1,
            "description": "gifts",
            "id": 21,
            "obligation": 100.0,
            "outlay": 111.0,
            "total_budgetary_resources": None,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M", "L", "N", "O"], spending_type="award")
    expected_results = [
        {
            "id": 21,
            "code": "000-0000",
            "description": "gifts",
            "award_count": 4,
            "obligation": 304.0,
            "outlay": 667.0,
            "total_budgetary_resources": None,
            "children": [
                {
                    "code": "2020/52",
                    "award_count": 1,
                    "description": "ferns",
                    "id": 24,
                    "obligation": 3.0,
                    "outlay": 333.0,
                    "total_budgetary_resources": None,
                },
                {
                    "code": "2020/98",
                    "award_count": 2,
                    "description": "evergreens",
                    "id": 23,
                    "obligation": 201.0,
                    "outlay": 223.0,
                    "total_budgetary_resources": None,
                },
                {
                    "code": "2020/99",
                    "award_count": 1,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 100.0,
                    "outlay": 111.0,
                    "total_budgetary_resources": None,
                },
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 4, "obligation": 304.0, "outlay": 667.0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_federal_account_award_empty(client, monkeypatch, helpers, generic_account_data):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_federal_account_award_query(client, generic_account_data, monkeypatch, helpers, elasticsearch_account_index):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(
        client, url, query="flowers", def_codes=["M", "L", "N", "O"], spending_type="award"
    )
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/99",
                    "award_count": 1,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 100.0,
                    "outlay": 111.0,
                    "total_budgetary_resources": None,
                }
            ],
            "code": "000-0000",
            "award_count": 1,
            "description": "gifts",
            "id": 21,
            "obligation": 100.0,
            "outlay": 111.0,
            "total_budgetary_resources": None,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_outlay_calculations(client, generic_account_data, monkeypatch, helpers, elasticsearch_account_index):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)

    helpers.reset_dabs_cache()
    sub = SubmissionAttributes.objects.get(
        reporting_fiscal_year=2022, reporting_fiscal_quarter=3, reporting_fiscal_period=7
    )
    defc_l = DisasterEmergencyFundCode.objects.get(code="L")
    ta = TreasuryAppropriationAccount.objects.get(account_title="flowers")
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub,
        fain="43tgfvdvfv",
        award_id=333,
        transaction_obligated_amount=1,
        gross_outlay_amount_by_award_cpe=100,
        disaster_emergency_fund=defc_l,
        treasury_account=ta,
        distinct_award_key="||43tgfvdvfv|",
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-3,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=-6,
    )
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    resp = helpers.post_for_spending_endpoint(
        client, url, query="flowers", def_codes=["L"], spending_type="award", sort="outlay"
    )
    expected_results = [
        {
            "children": [
                {
                    "code": "2020/99",
                    "award_count": 1,
                    "description": "flowers",
                    "id": 22,
                    "obligation": 1.0,
                    "outlay": 91.0,
                    "total_budgetary_resources": None,
                }
            ],
            "code": "000-0000",
            "award_count": 1,
            "description": "gifts",
            "id": 21,
            "obligation": 1.0,
            "outlay": 91.0,
            "total_budgetary_resources": None,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results
