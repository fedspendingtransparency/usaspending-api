import json

import pytest
from model_bakery import baker
from rest_framework import status

from usaspending_api.search.tests.data.search_filters_test_data import legacy_filters, non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


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
        subaward_count=1,
    )


def test_spending_by_subaward_grouped_success(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 2, "sort": "award_id", "filters": {}}),
    )
    assert resp.status_code == status.HTTP_200_OK

    resp_json = resp.json()
    resp_results = resp_json["results"]
    assert resp_json["page_metadata"]["page"] == 1
    assert resp_json["limit"] == 2
    assert len(resp_results) == 2
    assert resp_results[0] == {
        "award_id": "N6247318F4102",
        "subaward_count": 1,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
        "subaward_obligation": 1500,
    }
    assert resp_results[1] == {
        "award_id": "N6247318F4101",
        "subaward_count": 2,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        "subaward_obligation": 6912,
    }
    assert resp_json["messages"][0] == "This endpoint is under active development and subject to change"


@pytest.mark.django_db
def test_spending_by_subaward_grouped_legacy_filter(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 2, "sort": "award_id", "filters": legacy_filters()}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["limit"] == 2
    assert len(resp.json()["results"]) == 2
    assert resp.json()["results"][0] == {
        "award_id": "N6247318F4102",
        "subaward_count": 1,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
        "subaward_obligation": 1500,
    }
    assert resp.json()["results"][1] == {
        "award_id": "N6247318F4101",
        "subaward_count": 2,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        "subaward_obligation": 6912,
    }


@pytest.mark.django_db
def test_spending_by_subaward_grouped_non_legacy_filter(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 2, "sort": "award_id", "filters": non_legacy_filters()}),
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["limit"] == 2
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_spending_by_subaward_grouped_award_type_filter(
    client, monkeypatch, elasticsearch_award_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_award_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 2, "sort": "award_id", "filters": {"award_type_codes": ["A"]}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["limit"] == 2
    assert len(resp.json()["results"]) == 1
    assert resp.json()["results"][0] == {
        "award_id": "N6247318F4102",
        "subaward_count": 1,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
        "subaward_obligation": 1500,
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
        resp.json().get("detail")
        == "Field 'sort' is outside valid values ['award_id', 'subaward_count', 'award_generated_internal_id', 'subaward_obligation']"
    )
