import json

import pytest
from model_bakery import baker
from rest_framework import status
from usaspending_api.search.tests.data.search_filters_test_data import legacy_filters, non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test


@pytest.fixture
def subaward_grouped_data_fixture(db):
    award_search1 = baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        type="B",
    )
    award_search2 = baker.make(
        "search.AwardSearch",
        award_id=2,
        generated_unique_award_id="CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
        type="A",
    )

    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=1,
        award=award_search1,
        sub_action_date="2023-01-01",
        prime_award_group="grant",
        prime_award_type="A",
        subaward_number=99999,
        action_date="2023-01-01",
        subaward_amount=1234,
        unique_award_key="CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        award_piid_fain="N6247318F4101",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=3,
        award=award_search1,
        sub_action_date="2023-01-01",
        prime_award_group="grant",
        prime_award_type="C",
        subaward_number=99999,
        action_date="2023-01-01",
        subaward_amount=5678,
        unique_award_key="CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        award_piid_fain="N6247318F4101",
    )
    baker.make(
        "search.SubawardSearch",
        broker_subaward_id=2,
        award=award_search2,
        sub_action_date="2023-01-01",
        prime_award_group="procurement",
        prime_award_type="B",
        subaward_number=99998,
        action_date="2023-01-01",
        subaward_amount=1500,
        unique_award_key="CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
        award_piid_fain="N6247318F4102",
    )


def test_spending_by_subaward_grouped_success(
    client, monkeypatch, elasticsearch_subaward_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps({"page": 1, "limit": 2, "sort": "award_id", "filters": {}}),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["limit"] == 2
    assert len(resp.json()["results"]) == 2
    assert resp.json()["results"][0] == {
        "award_id": "N6247318F4101",
        "subaward_count": 2,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        "subaward_obligation": 6912,
    }
    assert resp.json()["results"][1] == {
        "award_id": "N6247318F4102",
        "subaward_count": 1,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
        "subaward_obligation": 1500,
    }


@pytest.mark.django_db
def test_spending_by_subaward_grouped_legacy_filter(
    client, monkeypatch, elasticsearch_subaward_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

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
        "award_id": "N6247318F4101",
        "subaward_count": 1,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        "subaward_obligation": 1234,
    }
    assert resp.json()["results"][1] == {
        "award_id": "N6247318F4102",
        "subaward_count": 1,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9002",
        "subaward_obligation": 1500,
    }


@pytest.mark.django_db
def test_spending_by_subaward_grouped_non_legacy_filter(
    client, monkeypatch, elasticsearch_subaward_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

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
    client, monkeypatch, elasticsearch_subaward_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

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
        "award_id": "N6247318F4101",
        "subaward_count": 1,
        "award_generated_internal_id": "CONT_AWD_N6247318F4138_9700_N6247316D1884_9001",
        "subaward_obligation": 1234,
    }


@pytest.mark.django_db
def test_spending_by_subaward_grouped_award_type_invalid_sort(
    client, monkeypatch, elasticsearch_subaward_index, subaward_grouped_data_fixture
):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

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
