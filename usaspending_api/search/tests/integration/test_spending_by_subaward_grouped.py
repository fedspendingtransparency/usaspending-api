import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.search_filters_test_data import legacy_filters, non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

ENDPOINT = "/api/v2/search/spending_by_subaward_grouped"


@pytest.fixture
def subaward_grouped_data_fixture(db):
    baker.make(
        "search.AwardSearch",
        action_date="2022-01-01",
        award_id=1,
        generated_unique_award_id="CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        type="B",
        display_award_id="N6247318F4101",
        total_subaward_amount=6912,
        award_amount=10000,
        subaward_count=2,
    )
    baker.make(
        "search.AwardSearch",
        action_date="2022-01-01",
        award_id=2,
        generated_unique_award_id="CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
        type="A",
        display_award_id="N6247318F4102",
        total_subaward_amount=1500,
        award_amount=3000,
        subaward_count=1,
    )

    baker.make(
        "search.AwardSearch",
        action_date="2022-01-01",
        award_id=3,
        generated_unique_award_id="CONT_AWD_N6247318F4138_9700_N6247316D1884_9003",
        type="C",
        display_award_id="N6247318F4103",
        total_subaward_amount=250,
        award_amount=5000,
        subaward_count=1,
    )


EXPECTED_F4101 = {
    "award_id": "N6247318F4101",
    "subaward_count": 2,
    "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
    "subaward_obligation": 6912.0,
    "award_obligation": 10000.0,
    "subaward_to_award_ratio": 0.6912,
}
EXPECTED_F4102 = {
    "award_id": "N6247318F4102",
    "subaward_count": 1,
    "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
    "subaward_obligation": 1500.0,
    "award_obligation": 3000.0,
    "subaward_to_award_ratio": 0.5,
}
EXPECTED_F4103 = {
    "award_id": "N6247318F4103",
    "subaward_count": 1,
    "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9003",
    "subaward_obligation": 250.0,
    "award_obligation": 5000.0,
    "subaward_to_award_ratio": 0.05,
}


def test_spending_by_subaward_grouped_success(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 3, "sort": "award_id", "filters": {}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp_json = resp.json()
    resp_results = resp_json["results"]
    assert resp_json["page_metadata"]["page"] == 1
    assert resp_json["limit"] == 3
    assert len(resp_results) == 3
    assert resp_results[0] == EXPECTED_F4103
    assert resp_results[1] == EXPECTED_F4102
    assert resp_results[2] == EXPECTED_F4101
    assert resp_json["messages"][0] == "This endpoint is under active development and subject to change"


@pytest.mark.django_db
def test_spending_by_subaward_grouped_legacy_filter(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 3, "sort": "award_id", "filters": legacy_filters()}),
    )

    print(legacy_filters())
    print(resp.json())

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["limit"] == 3
    assert len(resp.json()["results"]) == 2
    assert resp.json()["results"][0] == EXPECTED_F4102
    assert resp.json()["results"][1] == EXPECTED_F4101


@pytest.mark.django_db
def test_spending_by_subaward_grouped_non_legacy_filter(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 3, "sort": "award_id", "filters": non_legacy_filters()}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["limit"] == 3
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_spending_by_subaward_grouped_award_type_filter(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 3, "sort": "award_id", "filters": {"award_type_codes": ["A"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["limit"] == 3
    assert len(resp.json()["results"]) == 1
    assert resp.json()["results"][0] == EXPECTED_F4102


@pytest.mark.django_db
def test_spending_by_subaward_grouped_sort_by_subaward_to_award_ratio(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 3, "sort": "subaward_to_award_ratio", "order": "desc", "filters": {}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()["results"]
    assert len(results) == 3
    assert results[0] == EXPECTED_F4101
    assert results[1] == EXPECTED_F4102
    assert results[2] == EXPECTED_F4103

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 3, "sort": "subaward_to_award_ratio", "order": "asc", "filters": {}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()["results"]
    assert len(results) == 3
    assert results[0] == EXPECTED_F4103
    assert results[1] == EXPECTED_F4102
    assert results[2] == EXPECTED_F4101


@pytest.mark.django_db
def test_spending_by_subaward_grouped_zero_award_obligation_ratio(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    baker.make(
        "search.AwardSearch",
        action_date="2022-01-01",
        award_id=99,
        generated_unique_award_id="CONT_AWD_ZERO_AWARD_AMOUNT",
        type="A",
        display_award_id="ZERO-AWARD",
        total_subaward_amount=500,
        award_amount=0,
        subaward_count=1,
    )
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 1, "sort": "award_id", "filters": {}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()["results"]
    assert results[0] == {
        "award_id": "ZERO-AWARD",
        "subaward_count": 1,
        "award_generated_internal_id": "CONT_AWD_ZERO_AWARD_AMOUNT",
        "subaward_obligation": 500.0,
        "award_obligation": 0.0,
        "subaward_to_award_ratio": 0.0,
    }


@pytest.mark.django_db
def test_spending_by_subaward_grouped_award_type_invalid_sort(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 2, "sort": "invalid_sorting_field"}),
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert (
        resp.json().get("detail") == "Field 'sort' is outside valid values ['award_id', 'subaward_count', "
        "'award_generated_internal_id', 'subaward_obligation', 'subaward_to_award_ratio']"
    )
