import json

import pytest
from model_bakery import baker
from rest_framework import status
from usaspending_api.search.tests.data.search_filters_test_data import legacy_filters, non_legacy_filters
from usaspending_api.search.tests.data.utilities import setup_elasticsearch_test

@pytest.mark.django_db
def test_spending_by_subaward_grouped_success(client, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    resp = client.post(
        "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps(
            {"page": 1, "limit": 2, "sort": "award_id", "filters": {"award_type_codes": ["A", "B", "C"]}}
        ),
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["page_metadata"]["page"] == 1
    assert resp.json()["limit"] == 2
    print(resp.json())
    assert len(resp.json()["results"]) == 2

@pytest.mark.django_db
def test_spending_by_subaward_grouped_legacy_filter(client, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    resp = client.post(
          "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps(
            {"page": 1, "limit": 2, "sort": "award_id", "filters": legacy_filters()}
        ),
    )
    
    assert resp.status_code == status.HTTP_200_OK

@pytest.mark.django_db
def test_spending_by_subaward_grouped_non_legacy_filter(client, monkeypatch, elasticsearch_subaward_index):
    setup_elasticsearch_test(monkeypatch, elasticsearch_subaward_index)

    resp = client.post(
         "/api/v2/search/spending_by_subaward_grouped",
        content_type="application/json",
        data=json.dumps(
            {"page": 1, "limit": 2, "sort": "award_id", "filters": legacy_filters()}
        ),
    )
    
    assert resp.status_code == status.HTTP_200_OK