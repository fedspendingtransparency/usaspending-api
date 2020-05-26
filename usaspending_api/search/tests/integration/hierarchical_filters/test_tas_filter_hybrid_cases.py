import pytest

from usaspending_api.search.tests.integration.hierarchical_filters.tas_fixtures import (
    BASIC_TAS,
    ATA_TAS,
    TAS_DICTIONARIES,
)
from usaspending_api.search.tests.integration.hierarchical_filters.tas_search_test_helpers import (
    _setup_es,
    query_by_treasury_account_components,
)


@pytest.mark.django_db
def test_match_from_code_filter_only(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_treasury_account_components(client, {"require": [_agency_path(BASIC_TAS)]}, None)

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_from_component_filter_only(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_treasury_account_components(client, None, [component_dictionary(BASIC_TAS)])

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_from_component_both_filters(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_treasury_account_components(
        client, {"require": [_agency_path(BASIC_TAS)]}, [component_dictionary(BASIC_TAS)]
    )

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_non_match_from_component_both_filters(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_treasury_account_components(
        client, {"require": [_agency_path(ATA_TAS)]}, [component_dictionary(ATA_TAS)]
    )

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_different_matches_with_each_filter(client, monkeypatch, elasticsearch_award_index, multiple_awards_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_treasury_account_components(
        client, {"require": [_agency_path(BASIC_TAS)]}, [component_dictionary(ATA_TAS)]
    )

    assert resp.json()["results"].sort(key=lambda elem: elem["internal_id"]) == [_award1(), _award2()].sort(
        key=lambda elem: elem["internal_id"]
    )


@pytest.mark.django_db
def test_match_from_component_filter_despite_exclusion(
    client, monkeypatch, elasticsearch_award_index, multiple_awards_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_treasury_account_components(
        client,
        {"require": [_agency_path(BASIC_TAS)], "exclude": [_agency_path(BASIC_TAS)]},
        [component_dictionary(BASIC_TAS)],
    )

    assert resp.json()["results"] == [_award1()]


def _award1():
    return {"internal_id": 1, "Award ID": "abcdefg", "generated_internal_id": "AWARD_1"}


def _award2():
    return {"internal_id": 2, "Award ID": "abcdefg", "generated_internal_id": "AWARD_2"}


def _agency_path(index):
    return [_agency(index)]


def _agency(index):
    return TAS_DICTIONARIES[index]["aid"]


def component_dictionary(index):
    return TAS_DICTIONARIES[index]
