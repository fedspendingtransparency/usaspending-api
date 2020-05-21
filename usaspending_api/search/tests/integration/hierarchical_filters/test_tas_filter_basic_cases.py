import pytest

from usaspending_api.search.tests.integration.hierarchical_filters.tas_fixtures import (
    BASIC_TAS,
    ATA_TAS,
    BPOA_TAS,
    TAS_DICTIONARIES,
    TAS_STRINGS,
    UNINTUITIVE_AGENCY,
)
from usaspending_api.search.tests.integration.hierarchical_filters.tas_search_test_helpers import (
    _setup_es,
    query_by_tas,
)


@pytest.mark.django_db
def test_match_from_agency(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_from_fa(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_fa_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_from_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_non_match_from_agency(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_agency_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_non_match_from_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_agency_path(BASIC_TAS) + [TAS_STRINGS[ATA_TAS]]]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_from_ata_tas(client, monkeypatch, elasticsearch_award_index, award_with_ata_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(ATA_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_from_bpoa_tas(client, monkeypatch, elasticsearch_award_index, award_with_bpoa_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BPOA_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_unintuitive_tas(client, monkeypatch, elasticsearch_award_index, tas_with_nonintuitive_agency):
    # ensure that api can find a TAS that is under an agency not implied by the tas code
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [[UNINTUITIVE_AGENCY, _fa(BASIC_TAS), _tas(BASIC_TAS)]]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_non_match_unintuitive_tas_from_agency(
    client, monkeypatch, elasticsearch_award_index, tas_with_nonintuitive_agency
):
    # ensure that api CANNOT find a TAS from the agency in the TAS code's aid, because it's actually linked under a
    # different agency
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_non_match_unintuitive_tas_from_tas(
    client, monkeypatch, elasticsearch_award_index, tas_with_nonintuitive_agency
):
    # ensure that api CANNOT find a TAS from the agency in the TAS code's aid, because it's actually linked under a
    # different agency
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_search_on_multiple_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS), _tas_path(ATA_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_non_match_search_on_multiple_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(ATA_TAS), _tas_path(BPOA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_search_multi_tas_award(client, monkeypatch, elasticsearch_award_index, award_with_multiple_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_double_match_search_multi_tas_award(client, monkeypatch, elasticsearch_award_index, award_with_multiple_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS), _tas_path(ATA_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_non_match_search_multi_tas_award(client, monkeypatch, elasticsearch_award_index, award_with_multiple_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BPOA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_only_awards_with_tas(client, monkeypatch, elasticsearch_award_index, award_with_tas, award_without_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_match_on_multiple_awards(client, monkeypatch, elasticsearch_award_index, multiple_awards_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS), _tas_path(1)]})

    assert resp.json()["results"].sort(key=lambda elem: elem["internal_id"]) == [_award1(), _award2()].sort(
        key=lambda elem: elem["internal_id"]
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
