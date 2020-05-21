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
    query_by_tas,
)


@pytest.mark.django_db
def test_agency_level_require_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_fa_level_require_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_fa_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_tas_level_require_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_agency_level_exclude_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"exclude": [_agency_path(ATA_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_fa_level_exclude_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"exclude": [_fa_path(ATA_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_tas_level_exclude_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"exclude": [_tas_path(ATA_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_agency_level_require_non_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_agency_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_fa_level_require_non_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_fa_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_tas_level_require_non_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(ATA_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_agency_level_exclude_non_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"exclude": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_fa_level_exclude_non_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"exclude": [_fa_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_tas_level_exclude_non_match(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"exclude": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_double_require(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_fa_path(BASIC_TAS), _tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_double_exclude(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"exclude": [_fa_path(BASIC_TAS), _tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_exclude_overrides_require(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(BASIC_TAS)], "exclude": [_tas_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_exclude_eclipsing_require(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_agency_path(BASIC_TAS)], "exclude": [_fa_path(BASIC_TAS)]})

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_require_eclipsing_exclude(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_fa_path(BASIC_TAS)], "exclude": [_agency_path(BASIC_TAS)]})

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_double_eclipsing_filters(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(
        client, {"require": [_agency_path(BASIC_TAS), _tas_path(BASIC_TAS)], "exclude": [_fa_path(BASIC_TAS)]}
    )

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_double_eclipsing_filters2(client, monkeypatch, elasticsearch_award_index, award_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(
        client, {"require": [_fa_path(BASIC_TAS)], "exclude": [_agency_path(BASIC_TAS), _tas_path(BASIC_TAS)]}
    )

    assert resp.json()["results"] == []


@pytest.mark.django_db
def test_sibling_eclipsing_filters(client, monkeypatch, elasticsearch_award_index, multiple_awards_with_tas):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(
        client,
        {
            "require": [_agency_path(BASIC_TAS), _tas_path(ATA_TAS)],
            "exclude": [_agency_path(ATA_TAS), _tas_path(BASIC_TAS)],
        },
    )

    assert resp.json()["results"] == [_award2()]


@pytest.mark.django_db
def test_sibling_filters_on_one_sibling(
    client, monkeypatch, elasticsearch_award_index, multiple_awards_with_sibling_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(SISTER_TAS[1])]})

    assert resp.json()["results"] == [_award2()]


@pytest.mark.django_db
def test_sibling_filters_on_both_siblings(
    client, monkeypatch, elasticsearch_award_index, multiple_awards_with_sibling_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(client, {"require": [_tas_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[1])]})

    assert resp.json()["results"].sort(key=lambda elem: elem["internal_id"]) == [_award1(), _award2()].sort(
        key=lambda elem: elem["internal_id"]
    )


@pytest.mark.django_db
def test_sibling_filters_excluding_one_sibling(
    client, monkeypatch, elasticsearch_award_index, multiple_awards_with_sibling_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(
        client, {"require": [_tas_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[2])], "exclude": [_tas_path(SISTER_TAS[2])]}
    )

    assert resp.json()["results"] == [_award1()]


@pytest.mark.django_db
def test_sibling_filters_excluding_two_siblings(
    client, monkeypatch, elasticsearch_award_index, multiple_awards_with_sibling_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(
        client,
        {
            "require": [_tas_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[1])],
            "exclude": [_tas_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[2])],
        },
    )

    assert resp.json()["results"] == [_award2()]


@pytest.mark.django_db
def test_sibling_filters_with_fa_excluding_one_sibling(
    client, monkeypatch, elasticsearch_award_index, multiple_awards_with_sibling_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(
        client,
        {
            "require": [_fa_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[2])],
            "exclude": [_tas_path(SISTER_TAS[2])],
        },
    )

    assert resp.json()["results"].sort(key=lambda elem: elem["internal_id"]) == [_award1(), _award2()].sort(
        key=lambda elem: elem["internal_id"]
    )


@pytest.mark.django_db
def test_sibling_filters_with_fa_excluding_two_siblings(
    client, monkeypatch, elasticsearch_award_index, multiple_awards_with_sibling_tas
):
    _setup_es(client, monkeypatch, elasticsearch_award_index)
    resp = query_by_tas(
        client,
        {
            "require": [_fa_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[1])],
            "exclude": [_tas_path(SISTER_TAS[0]), _tas_path(SISTER_TAS[2])],
        },
    )

    assert resp.json()["results"] == [_award2()]


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
