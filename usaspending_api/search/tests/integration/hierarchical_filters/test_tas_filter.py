import json
import pytest
from django.conf import settings

from usaspending_api.search.elasticsearch.filters.tas import TasCodes
from usaspending_api.common.experimental_api_flags import EXPERIMENTAL_API_HEADER, ELASTICSEARCH_HEADER_VALUE
from usaspending_api.search.tests.integration.hierarchical_filters.fixtures import TAS_DICTIONARIES, TAS_STRINGS


@pytest.mark.django_db
def test_match_from_agency(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_agency_path(0)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_from_fa(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_fa_path(0)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_from_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_tas_path(0)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_non_match_from_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_agency_path(0) + [TAS_STRINGS[1]]]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_search_on_multiple_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_tas_path(0), _tas_path(1)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_non_match_search_on_multiple_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_tas_path(1), _tas_path(2)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_search_multi_tas_award(client, monkeypatch, elasticsearch_award_index, award_with_multiple_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_tas_path(0)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_double_match_search_multi_tas_award(client, monkeypatch, elasticsearch_award_index, award_with_multiple_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_tas_path(0), _tas_path(1)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_non_match_search_multi_tas_award(client, monkeypatch, elasticsearch_award_index, award_with_multiple_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_tas_path(2)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_only_awards_with_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas, award_without_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_tas_path(0)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_on_multiple_awards(client, monkeypatch, elasticsearch_award_index, multiple_awards_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = _query_by_tas(client, {"require": [_tas_path(0), _tas_path(1)]})

    assert resp.json()["results"].sort(key=lambda elem: elem["internal_id"]) == [_award1(), _award2()].sort(
        key=lambda elem: elem["internal_id"]
    )


def _setup_es(client, monkeypatch, elasticsearch_award_index):
    monkeypatch.setattr(
        "usaspending_api.common.elasticsearch.search_wrappers.AwardSearch._index_name",
        settings.ES_AWARDS_QUERY_ALIAS_PREFIX,
    )
    elasticsearch_award_index.update_index()


def _query_by_tas(client, tas):
    return client.post(
        "/api/v2/search/spending_by_award",
        content_type="application/json",
        data=json.dumps(
            {
                "subawards": False,
                "fields": ["Award ID"],
                "sort": "Award ID",
                "filters": {
                    "award_type_codes": ["A", "B", "C", "D"],
                    TasCodes.underscore_name: tas,
                    "time_period": [{"start_date": "2007-10-01", "end_date": "2020-09-30"}],
                },
            }
        ),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )


def _award1():
    return {"internal_id": 1, "Award ID": "abcdefg", "generated_internal_id": "AWARD_1"}


def _award2():
    return {"internal_id": 2, "Award ID": "abcdefg", "generated_internal_id": "AWARD_2"}


def _agency_path(index):
    return [_agency(index)]


def _fa_path(index):
    return [_agency(index), _fa(index)]


def _tas_path(index):
    return [_agency(index), _fa(index), _tas(index)]


def _agency(index):
    return TAS_DICTIONARIES[index]["aid"]


def _fa(index):
    return f"{TAS_DICTIONARIES[index]['aid']}-{TAS_DICTIONARIES[index]['main']}"


def _tas(index):
    return TAS_STRINGS[index]


def _sort_by_id(dictionary):
    dictionary["internal_id"]
