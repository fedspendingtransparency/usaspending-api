import pytest
from rest_framework import status

from usaspending_api.search.tests.integration.hierarchical_filters.tas_search_test_helpers import (
    _setup_es,
    query_by_tas_subaward,
    query_by_treasury_account_components,
)
from usaspending_api.search.tests.integration.hierarchical_filters.tas_fixtures import TAS_DICTIONARIES


@pytest.mark.django_db
def test_tas_filter_not_object_or_list(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, "This shouldn't be a string")

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_too_long(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [["011", "011-0990", "3-4-2-5-3/5-6-3-4"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_too_short(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [["011", "011-0990", "3-4-2"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_no_ata(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [["011", "011-0990", "2000/2000-0990-000"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_no_sub(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [["011", "011-0990", "011-2000/2000-0990"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_unparsable_no_main(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [["011", "011-0990", "011-2000/2000-000"]]})

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"


@pytest.mark.django_db
def test_tas_filter_is_legacy(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, [{"main": TAS_DICTIONARIES[0]["main"], "aid": TAS_DICTIONARIES[0]["aid"]}])

    assert len(resp.json()["results"]) == 1


@pytest.mark.django_db
def test_treasury_account_component_filter_appropriate_characters(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    # "R" doesn't represent anything special here, it just makes sure the code is ok with any capital letter
    resp = query_by_treasury_account_components(client, [{"a": "R"}], None)

    assert resp.status_code == status.HTTP_200_OK, "Failed to return 422 Response"


@pytest.mark.django_db
def test_treasury_account_component_filter_inappropriate_characters(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_treasury_account_components(client, {"aid": "020", "main": "SELECT"}, None)

    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY, "Failed to return 422 Response"
