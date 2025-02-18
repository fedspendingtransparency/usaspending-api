import pytest

from usaspending_api.search.tests.integration.hierarchical_filters.tas_fixtures import (
    BASIC_TAS,
    ATA_TAS,
    TAS_DICTIONARIES,
)
from usaspending_api.search.tests.integration.hierarchical_filters.tas_search_test_helpers import (
    _setup_es,
    query_by_treasury_account_components_subaward,
)


@pytest.mark.django_db
def test_match_from_code_filter_only(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_treasury_account_components_subaward(client, {"require": [_agency_path(BASIC_TAS)]}, None)

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_match_from_component_filter_only(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_treasury_account_components_subaward(client, None, [component_dictionary(BASIC_TAS)])

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_match_from_component_both_filters(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_treasury_account_components_subaward(
        client, {"require": [_agency_path(BASIC_TAS)]}, [component_dictionary(BASIC_TAS)]
    )

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_non_match_from_component_both_filters(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_treasury_account_components_subaward(
        client, {"require": [_agency_path(ATA_TAS)]}, [component_dictionary(ATA_TAS)]
    )

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_different_matches_with_each_filter(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, multiple_subawards_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_treasury_account_components_subaward(
        client, {"require": [_agency_path(BASIC_TAS)]}, [component_dictionary(ATA_TAS)]
    )

    assert resp.json()["results"].sort(key=lambda elem: elem["internal_id"]) == [_subaward1(), _subaward2()].sort(
        key=lambda elem: elem["internal_id"]
    )


@pytest.mark.django_db
def test_match_from_component_filter_despite_exclusion(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, multiple_subawards_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_treasury_account_components_subaward(
        client,
        {"require": [_agency_path(BASIC_TAS)], "exclude": [_agency_path(BASIC_TAS)]},
        [component_dictionary(BASIC_TAS)],
    )

    assert resp.json()["results"] == [_subaward1()]


def _subaward1():
    return {
        "Sub-Award ID": "11111",
        "internal_id": "11111",
        "prime_award_generated_internal_id": "AWARD_1",
        "prime_award_internal_id": 1,
    }


def _subaward2():
    return {
        "internal_id": "11111",
        "prime_award_internal_id": 2,
        "Sub-Award ID": "11111",
        "prime_award_generated_internal_id": "AWARD_2",
    }


def _agency_path(index):
    return [_agency(index)]


def _agency(index):
    return TAS_DICTIONARIES[index]["aid"]


def component_dictionary(index):
    return TAS_DICTIONARIES[index]
