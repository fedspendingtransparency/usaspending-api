import pytest

from model_mommy import mommy
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.models_matviews import UniversalAwardView
from usaspending_api.awards.v2.filters.location_filter_geocode import (
    create_city_name_queryset,
    create_nested_object,
    elasticsearch_results,
    geocode_filter_locations,
    get_award_ids_by_city,
    get_fields_list,
    location_error_handling,
    return_query_string,
    validate_location_keys,
)
from usaspending_api.common.exceptions import InvalidParameterException


@pytest.fixture
def award_data_fixture(db):
    mommy.make("references.Location", location_id=1, location_country_code="USA", state_code="TX")
    mommy.make("references.LegalEntity", legal_entity_id=1)
    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        action_date="2010-10-01",
        award_id=1,
        is_fpds=True,
        place_of_performance_id=1,
        type="A",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        legal_entity_city_name="Burbank",
        legal_entity_country_code="USA",
        legal_entity_state_code="CA",
        piid="piiiiid",
        place_of_perform_city_name="Houston",
    )
    mommy.make(
        "awards.Award", id=1, is_fpds=True, latest_transaction_id=1, piid="piiiiid", recipient_id=1, type="A",
        place_of_performance_id=1
    )


def test_geocode_filter_locations(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()

    to = UniversalAwardView.objects

    values = [
        {"city": "Houston", "state": "TX", "country": "USA"},
        {"city": "Burbank", "state": "CA", "country": "USA"},
    ]

    assert to.filter(geocode_filter_locations("nothing", [], True)).count() == 1
    assert to.filter(geocode_filter_locations("pop", values, True)).count() == 1
    assert to.filter(geocode_filter_locations("recipient_location", values, True)).count() == 1

    values = [
        {"city": "Austin", "state": "TX", "country": "USA"},
        {"city": "Burbank", "state": "TX", "country": "USA"},
    ]

    assert to.filter(geocode_filter_locations("pop", values, True)).count() == 0
    assert to.filter(geocode_filter_locations("recipient_location", values, True)).count() == 0


def test_validate_location_keys():
    assert validate_location_keys([]) is None
    with pytest.raises(InvalidParameterException):
        assert validate_location_keys([{}]) is None
    with pytest.raises(InvalidParameterException):
        assert validate_location_keys([{"district": ""}]) is None
    with pytest.raises(InvalidParameterException):
        assert validate_location_keys([{"county": ""}]) is None
    assert validate_location_keys([{"country": "", "state": ""}]) is None
    assert validate_location_keys([{"country": "", "state": "", "feet": ""}]) is None
    assert (
        validate_location_keys(
            [
                {
                    "country": "USA",
                    "zip": "12345",
                    "city": "Chicago",
                    "state": "IL",
                    "county": "Yes",
                    "district": "Also Yes",
                },
                {"country": "USA", "zip": "12345", "city": "Chicago"},
            ]
        )
        is None
    )


def test_create_nested_object():
    with pytest.raises(InvalidParameterException):
        location_error_handling([])
    with pytest.raises(InvalidParameterException):
        location_error_handling([{"country": "", "state": ""}])
    with pytest.raises(InvalidParameterException):
        location_error_handling([{"country": "", "state": "", "feet": ""}])
    assert create_nested_object(
        [
            {
                "country": "USA",
                "zip": "12345",
                "city": "Chicago",
                "state": "IL",
                "county": "Yes",
                "district": "Also Yes",
            },
            {"country": "USA", "zip": "12345", "city": "Chicago"},
        ]
    ) == {
        "USA": {
            "city": ["Chicago"],
            "zip": ["12345", "12345"],
            "IL": {"county": ["Yes"], "district": ["Also Yes"], "city": ["Chicago"]},
        }
    }


def test_location_error_handling():
    with pytest.raises(InvalidParameterException):
        location_error_handling({})
    with pytest.raises(InvalidParameterException):
        location_error_handling({"country": "", "county": ""})
    with pytest.raises(InvalidParameterException):
        location_error_handling({"country": "", "district": ""})
    assert location_error_handling({"country": "", "state": "", "county": ""}) is None
    assert location_error_handling({"country": "", "state": "", "district": ""}) is None
    assert location_error_handling({"country": "", "state": "", "county": "", "district": ""}) is None
    assert (
        location_error_handling({"country": "", "state": "", "county": "", "district": "", "feet": ""})
        is None
    )


def test_get_fields_list():
    assert get_fields_list("congressional_code", "01") == ["1", "01", "1.0"]
    assert get_fields_list("county_code", "01") == ["1", "01", "1.0"]
    assert get_fields_list("feet", "01") == ["01"]
    assert get_fields_list("congressional_code", "abc") == ["abc"]


def test_return_query_string():
    assert return_query_string(True) == ("{0}_{1}", "country_code")
    assert return_query_string(False) == ("{0}__{1}", "location_country_code")


def test_create_city_name_queryset(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()

    to = TransactionNormalized.objects

    assert to.filter(create_city_name_queryset("nothing", [], "nothing", "nothing")).count() == 0

    assert to.filter(create_city_name_queryset("pop", ["Houston"], "USA", None)).count() == 1
    assert to.filter(create_city_name_queryset("pop", ["Houston"], "USA", "TX")).count() == 1
    assert to.filter(create_city_name_queryset("pop", ["Houston"], "USA", "VA")).count() == 0
    assert to.filter(create_city_name_queryset("pop", ["Houston", "Burbank"], "USA", "TX")).count() == 1
    assert to.filter(create_city_name_queryset("pop", ["Burbank"], "USA", "CA")).count() == 0

    assert to.filter(create_city_name_queryset("recipient_location", ["Burbank"], "USA", None)).count() == 1
    assert to.filter(create_city_name_queryset("recipient_location", ["Burbank"], "USA", "CA")).count() == 1
    assert to.filter(create_city_name_queryset("recipient_location", ["Burbank"], "USA", "VA")).count() == 0
    assert (
        to.filter(
            create_city_name_queryset("recipient_location", ["Burbank", "Houston"], "USA", "CA")
        ).count()
        == 1
    )
    assert to.filter(create_city_name_queryset("recipient_location", ["Houston"], "USA", "TX")).count() == 0


def test_get_award_ids_by_city(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()

    assert len(get_award_ids_by_city("nothing", "nothing", "nothing", "nothing")) == 0

    assert len(get_award_ids_by_city("pop", "Houston", "USA", None)) == 1
    assert len(get_award_ids_by_city("pop", "Houston", "USA", "TX")) == 1
    assert len(get_award_ids_by_city("pop", "Houston", "USA", "VA")) == 0
    assert len(get_award_ids_by_city("pop", "Burbank", "USA", "CA")) == 0

    assert len(get_award_ids_by_city("recipient_location", "Burbank", "USA", None)) == 1
    assert len(get_award_ids_by_city("recipient_location", "Burbank", "USA", "CA")) == 1
    assert len(get_award_ids_by_city("recipient_location", "Burbank", "USA", "VA")) == 0
    assert len(get_award_ids_by_city("recipient_location", "Houston", "USA", "TX")) == 0


def test_elasticsearch_results(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()

    query = {
        "_source": ["award_id"],
        "size": 0,
        "query": {"match_all": {}},
        "aggs": {"award_ids": {"terms": {"field": "award_id", "size": 5}}},
    }
    results = elasticsearch_results(query)
    assert results is not None
    assert len(results) == 1
    assert results[0] == 1

    query = {
        "_source": ["award_id"],
        "size": 0,
        "query": {"match": {"pop_country": "XXXXX"}},
        "aggs": {"award_ids": {"terms": {"field": "award_id", "size": 5}}},
    }
    results = elasticsearch_results(query)
    assert results is not None
    assert len(results) == 0
