import pytest
import json
from rest_framework import status

from usaspending_api.search.tests.integration.hierarchical_filters.es_search_test_helpers import (
    _setup_es,
    _query_by_tas,
)
from usaspending_api.search.tests.integration.hierarchical_filters.fixtures import TAS_DICTIONARIES
from usaspending_api.search.elasticsearch.filters.tas import TasCodes


@pytest.mark.django_db
def test_tas_filter_not_object_or_list(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, "This shouldn't be a string")

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_too_long(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [["011", "011-0990", "3-4-2-5-3/5-6-3-4"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_too_short(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [["011", "011-0990", "3-4-2"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_no_ata(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [["011", "011-0990", "2000/2000-0990-000"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_no_sub(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [["011", "011-0990", "011-2000/2000-0990"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_no_main(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [["011", "011-0990", "011-2000/2000-000"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_filter_is_legacy(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, [{"main": TAS_DICTIONARIES[0]["main"], "aid": TAS_DICTIONARIES[0]["aid"]}])

    assert len(resp.json()["results"]) == 1


@pytest.mark.django_db
def test_not_using_es_header(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "subawards": False,
                "fields": ["Award ID"],
                "sort": "Award ID",
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    TasCodes.underscore_name: {"require": [["011"]]},
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
    )

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_filter_inappropriate_characters(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [["011", "[abc]"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"
