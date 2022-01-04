import json
import pytest

from model_mommy import mommy
from time import perf_counter
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

ENDPOINT = "/api/v2/search/spending_by_transaction/"


@pytest.fixture
def transaction_data():
    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        description="test",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        legal_entity_zip5="abcde",
        piid="IND12PB00323",
        awardee_or_recipient_uei="testuei",
    )
    mommy.make("awards.Award", id=1, latest_transaction_id=1, is_fpds=True, type="A", piid="IND12PB00323")


@pytest.mark.django_db
def test_spending_by_transaction_kws_success(client, elasticsearch_transaction_index):
    """Verify error on bad autocomplete
    request for budget function."""

    resp = client.post(
        ENDPOINT,
        content_type="application/json",
        data=json.dumps(
            {
                "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
                "fields": ["Award ID", "Recipient Name", "Mod"],
                "page": 1,
                "limit": 5,
                "sort": "Award ID",
                "order": "desc",
            }
        ),
    )

    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_transaction_kws_failure(client):
    """Verify error on bad autocomplete
    request for budget function."""

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps({"filters": {}}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_no_intersection(client):
    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D", "no intersection"]},
        "fields": ["Award ID", "Recipient Name", "Mod"],
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }
    api_start = perf_counter()

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))
    api_end = perf_counter()
    assert resp.status_code == status.HTTP_200_OK
    assert api_end - api_start < 0.5, "Response took over 0.5s! Investigate why"
    assert len(resp.data["results"]) == 0, "Results returned, there should be 0"


@pytest.mark.django_db
def test_all_fields_returned(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = [
        "Recipient Name",
        "Action Date",
        "Transaction Amount",
        "Award Type",
        "Awarding Agency",
        "Awarding Sub Agency",
        "Funding Agency",
        "Funding Sub Agency",
        "Issued Date",
        "Loan Value",
        "Subsidy Cost",
        "Mod",
        "Award ID",
        "awarding_agency_id",
        "internal_id",
        "generated_internal_id",
        "Last Date to Order",
    ]

    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) > 0
    for result in resp.data["results"]:
        for field in fields:
            assert field in result, f"Response item is missing field {field}"

        assert "Sausage" not in result
        assert "A" not in result


@pytest.mark.django_db
def test_subset_of_fields_returned(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = ["Award ID", "Recipient Name", "Mod"]

    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) > 0
    for result in resp.data["results"]:
        for field in fields:
            assert field in result, f"Response item is missing field {field}"

        assert "internal_id" in result
        assert "generated_internal_id" in result
        assert "Last Date to Order" not in result


@pytest.mark.django_db
def test_columns_can_be_sorted(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = [
        "Action Date",
        "Award ID",
        "Awarding Agency",
        "Awarding Sub Agency",
        "Award Type",
        "Mod",
        "Recipient Name",
        "Action Date",
    ]

    request = {
        "filters": {"keyword": "test", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "order": "desc",
    }

    for field in fields:
        request["sort"] = field
        resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))
        assert resp.status_code == status.HTTP_200_OK, f"Failed to sort column: {field}"


@pytest.mark.django_db
def test_uei(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = ["Award ID"]

    request = {
        "filters": {"keyword": "testuei", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) > 0
