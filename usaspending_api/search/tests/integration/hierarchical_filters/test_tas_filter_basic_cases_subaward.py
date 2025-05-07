import pytest

from usaspending_api.search.tests.integration.hierarchical_filters.tas_fixtures import (
    ATA_BPOA_TAS,
    ATA_TAS,
    BASIC_TAS,
    BPOA_TAS,
    TAS_DICTIONARIES,
    TAS_STRINGS,
    UNINTUITIVE_AGENCY,
)
from usaspending_api.search.tests.integration.hierarchical_filters.tas_search_test_helpers import (
    _setup_es,
    query_by_tas_subaward,
)


@pytest.mark.django_db
def test_match_from_agency(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_match_from_fa(client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_fa_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_match_from_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_non_match_from_agency(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_agency_path(ATA_TAS)]})
    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_non_match_from_fa(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_fa_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_non_match_from_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_from_ata_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_ata_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(ATA_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_match_from_bpoa_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_bpoa_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BPOA_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_match_from_unintuitive_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_unintuitive_agency
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [[UNINTUITIVE_AGENCY, _fa(BASIC_TAS), _tas(BASIC_TAS)]]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_non_match_from_unintuitive_tas_from_agency(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_unintuitive_agency
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    # ensure that api CANNOT find a TAS from the agency in the TAS code's aid, because it's actually linked under a
    # different agency
    resp = query_by_tas_subaward(client, {"require": [_agency_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_non_match_from_unintuitive_tas_from_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_unintuitive_agency
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    # ensure that api CANNOT find a TAS from the agency in the TAS code's aid, because it's actually linked under a
    # different agency
    resp = query_by_tas_subaward(client, {"require": [_tas_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_search_on_multiple_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_multiple_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BASIC_TAS), _tas_path(ATA_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_non_match_search_on_multiple_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_multiple_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(ATA_BPOA_TAS), _tas_path(BPOA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_search_multi_tas_award(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_multiple_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_double_match_search_multi_tas_award(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_multiple_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BASIC_TAS), _tas_path(ATA_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_match_only_awards_with_tas(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_no_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_match_on_multiple_awards(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_no_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


def _subaward1():
    return {
        "Sub-Award ID": "11111",
        "internal_id": "11111",
        "prime_award_generated_internal_id": "AWARD_1",
        "prime_award_internal_id": 1,
    }


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
