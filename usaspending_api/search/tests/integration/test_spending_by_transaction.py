import json
from time import perf_counter

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

ENDPOINT = "/api/v2/search/spending_by_transaction/"


@pytest.fixture
def transaction_data():
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        award_id=1,
        action_date="2010-10-01",
        is_fpds=True,
        type="A",
        transaction_description="test",
        recipient_location_zip5="abcde",
        piid="IND12PB00323",
        recipient_uei="testuei",
        parent_uei="test_parent_uei",
        program_activities=[{"code": "0123", "name": "PROGRAM_ACTIVITY_123"}],
    )
    award1 = baker.make(
        "search.AwardSearch",
        award_id=1,
        latest_transaction_id=1,
        is_fpds=True,
        type="A",
        piid="IND12PB00323",
        program_activities=[{"code": "0123", "name": "PROGRAM_ACTIVITY_123"}],
    )
    ref_program_activity1 = baker.make(
        "references.RefProgramActivity",
        id=1,
        program_activity_code=123,
        program_activity_name="PROGRAM_ACTIVITY_123",
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        financial_accounts_by_awards_id=1,
        award=award1,
        program_activity_id=ref_program_activity1.id,
    )


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
        "Transaction Description",
        "Action Type",
        "Recipient UEI",
        "Recipient Location",
        "Primary Place of Performance",
        "NAICS",
        "PSC",
        "Assistance Listing",
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
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_parent_uei(client, monkeypatch, transaction_data, elasticsearch_transaction_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = ["Award ID"]

    request = {
        "filters": {"keyword": "test_parent_uei", "award_type_codes": ["A", "B", "C", "D"]},
        "fields": fields,
        "page": 1,
        "limit": 5,
        "sort": "Award ID",
        "order": "desc",
    }

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(request))

    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1


@pytest.mark.django_db
def test_spending_by_txn_program_activity(client, monkeypatch, elasticsearch_transaction_index, transaction_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    # Program Activites filter test
    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {
            "program_activities": [{"name": "program_activity_123"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
    }
    expected_response = [{"Award ID": "IND12PB00323", "generated_internal_id": None, "internal_id": 1}]

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "321"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
    }
    expected_response = []

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {
            "program_activities": [{"name": "program_activity_123", "code": "123"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
    }
    expected_response = [{"Award ID": "IND12PB00323", "generated_internal_id": None, "internal_id": 1}]

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"

    test_payload = {
        "fields": ["Award ID"],
        "sort": "Award ID",
        "filters": {
            "program_activities": [{"name": "program_activity_123"}, {"code": "123"}],
            "award_type_codes": ["A", "B", "C", "D"],
        },
    }
    expected_response = [{"Award ID": "IND12PB00323", "generated_internal_id": None, "internal_id": 1}]

    resp = client.post(ENDPOINT, content_type="application/json", data=json.dumps(test_payload))
    assert resp.status_code == status.HTTP_200_OK
    assert expected_response == resp.json().get("results"), "Unexpected or missing content!"


@pytest.mark.django_db
def test_additional_fields(client, monkeypatch, elasticsearch_transaction_index, transaction_data):
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index)

    fields = [
        "Award ID",
        "Transaction Description",
        "Action Type",
        "Recipient UEI",
        "Recipient Location",
        "Primary Place of Performance",
        "NAICS",
        "PSC",
        "Assistance Listing",
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
    assert len(resp.json().get("results")) > 0
