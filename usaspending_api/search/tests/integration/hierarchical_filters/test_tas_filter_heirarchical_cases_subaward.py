import pytest

from usaspending_api.search.tests.integration.hierarchical_filters.tas_fixtures import (
    BASIC_TAS,
    ATA_TAS,
    SISTER_TAS,
    TAS_DICTIONARIES,
    TAS_STRINGS,
)
from usaspending_api.search.tests.integration.hierarchical_filters.tas_search_test_helpers import (
    _setup_es,
    query_by_tas_subaward,
)


@pytest.mark.django_db
def test_agency_level_require_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_fa_level_require_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_fa_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_tas_level_require_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_agency_level_exclude_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"exclude": [_agency_path(ATA_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_fa_level_exclude_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"exclude": [_fa_path(ATA_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_tas_level_exclude_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"exclude": [_tas_path(ATA_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_agency_level_require_non_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_agency_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_fa_level_require_non_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_fa_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_tas_level_require_non_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_agency_level_exclude_non_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"exclude": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_fa_level_exclude_non_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"exclude": [_fa_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_tas_level_exclude_non_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"exclude": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_double_require(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_fa_path(BASIC_TAS), _tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_double_exclude(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"exclude": [_fa_path(BASIC_TAS), _tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_exclude_overrides_require(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(BASIC_TAS)], "exclude": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_exclude_eclipsing_require(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_agency_path(BASIC_TAS)], "exclude": [_fa_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_require_eclipsing_exclude(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_fa_path(BASIC_TAS)], "exclude": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_double_eclipsing_filters(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(
        client, {"require": [_agency_path(BASIC_TAS), _tas_path(BASIC_TAS)], "exclude": [_fa_path(BASIC_TAS)]}
    )

    assert resp.json()["results"] == [_subaward1()]


@pytest.mark.django_db
def test_double_eclipsing_filters2(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, subaward_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(
        client, {"require": [_fa_path(BASIC_TAS)], "exclude": [_agency_path(BASIC_TAS), _tas_path(BASIC_TAS)]}
    )

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_sibling_eclipsing_filters(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, multiple_subawards_with_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(
        client,
        {
            "require": [_agency_path(BASIC_TAS), _tas_path(ATA_TAS)],
            "exclude": [_agency_path(ATA_TAS), _tas_path(BASIC_TAS)],
        },
    )

    assert resp.json()["results"] == [_subaward2()]


@pytest.mark.django_db
def test_sibling_filters_one_match(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, multiple_subawards_with_sibling_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(SISTER_TAS[1])]})

    assert resp.json()["results"] == [_subaward2()]


@pytest.mark.django_db
def test_sibling_filters_two_matchs(
    client, monkeypatch, elasticsearch_award_index, elasticsearch_subaward_index, multiple_subawards_with_sibling_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    _setup_es(client, monkeypatch, elasticsearch_subaward_index)
    resp = query_by_tas_subaward(client, {"require": [_tas_path(SISTER_TAS[1]), _tas_path(SISTER_TAS[0])]})

    assert resp.json()["results"].sort(key=lambda elem: elem["internal_id"]) == [_subaward1(), _subaward2()].sort(
        key=lambda elem: elem["internal_id"]
    )


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
