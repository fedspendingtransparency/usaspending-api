import pytest
from model_mommy import mommy

from rest_framework import status

from usaspending_api.disaster.tests.fixtures.object_class_data import major_object_class_with_children
from usaspending_api.disaster.v2.views.disaster_base import COVID_19_GROUP_NAME
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test
from usaspending_api.submissions.models import SubmissionAttributes

url = "/api/v2/disaster/object_class/spending/"


@pytest.mark.django_db
def test_basic_object_class_award_success(
    client, elasticsearch_account_index, basic_faba_with_object_class, monkeypatch, helpers
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    expected_results = [
        {
            "id": "001",
            "code": "001",
            "description": "001 name",
            "award_count": 1,
            "obligation": 1.0,
            "outlay": 0.0,
            "children": [
                {
                    "id": "1",
                    "code": "0001",
                    "description": "0001 name",
                    "award_count": 1,
                    "obligation": 1.0,
                    "outlay": 0.0,
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 1, "obligation": 1.0, "outlay": 0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_object_class_counts_awards(
    client, elasticsearch_account_index, faba_with_object_class_and_two_awards, monkeypatch, helpers
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 1
    assert resp.json()["results"][0]["award_count"] == 2
    assert len(resp.json()["results"][0]["children"]) == 1


@pytest.mark.django_db
def test_object_class_groups_by_object_classes(
    client, elasticsearch_account_index, faba_with_two_object_classes_and_two_awards, monkeypatch, helpers
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 2


@pytest.mark.django_db
def test_object_class_spending_filters_on_defc(
    client, elasticsearch_account_index, basic_faba_with_object_class, monkeypatch, helpers
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"], spending_type="award")
    assert len(resp.json()["results"]) == 0

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert len(resp.json()["results"]) == 1


@pytest.mark.django_db
def test_object_class_spending_filters_on_object_class_existence(
    client, elasticsearch_account_index, award_count_sub_schedule, basic_faba, monkeypatch, helpers
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"], spending_type="award")
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_object_class_query(client, elasticsearch_account_index, basic_faba_with_object_class, monkeypatch, helpers):
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(
        client, url, query="001 name", def_codes=["A", "M", "N"], spending_type="award"
    )
    expected_results = [
        {
            "id": "001",
            "code": "001",
            "description": "001 name",
            "award_count": 1,
            "obligation": 1.0,
            "outlay": 0.0,
            "children": [
                {
                    "id": "1",
                    "code": "0001",
                    "description": "0001 name",
                    "award_count": 1,
                    "obligation": 1.0,
                    "outlay": 0.0,
                }
            ],
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 1, "obligation": 1.0, "outlay": 0}
    assert resp.json()["totals"] == expected_totals


@pytest.mark.django_db
def test_outlay_calculations(client, elasticsearch_account_index, basic_faba_with_object_class, monkeypatch, helpers):
    oc = major_object_class_with_children("001", [1])
    mommy.make("references.DisasterEmergencyFundCode", code="L", group_name=COVID_19_GROUP_NAME),
    mommy.make(
        "awards.FinancialAccountsByAwards",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="L").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=oc[0],
        transaction_obligated_amount=1,
        gross_outlay_amount_by_award_cpe=100,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-3,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=-6,
    )
    setup_elasticsearch_test(monkeypatch, elasticsearch_account_index)
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    helpers.reset_dabs_cache()

    resp = helpers.post_for_spending_endpoint(client, url, query="001 name", def_codes=["L"], spending_type="award")
    expected_results = [
        {
            "id": "001",
            "code": "001",
            "description": "001 name",
            "award_count": 1,
            "obligation": 1.0,
            "outlay": 91.0,
            "children": [
                {
                    "id": "1",
                    "code": "0001",
                    "description": "0001 name",
                    "award_count": 1,
                    "obligation": 1.0,
                    "outlay": 91.0,
                }
            ],
        }
    ]
    print(resp.json())
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    expected_totals = {"award_count": 1, "obligation": 1.0, "outlay": 91.0}
    assert resp.json()["totals"] == expected_totals
