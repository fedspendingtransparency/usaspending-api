import pytest

from model_mommy import mommy
from usaspending_api.awards.models_matviews import UniversalAwardView, UniversalTransactionView
from usaspending_api.awards.v2.filters.location_filter_geocode import (
    create_city_name_queryset,
    create_nested_object,
    elasticsearch_results,
    geocode_filter_locations,
    get_record_ids_by_city,
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
        legal_entity_city_name="BURBANK",
        legal_entity_country_code="USA",
        legal_entity_state_code="CA",
        piid="piiiiid",
        place_of_perform_city_name="HOUSTON",
    )
    mommy.make(
        "awards.Award",
        id=1,
        is_fpds=True,
        latest_transaction_id=1,
        piid="piiiiid",
        recipient_id=1,
        type="A",
        place_of_performance_id=1,
    )

    mommy.make("references.Location", location_id=2, location_country_code="USA", state_code="NE")
    mommy.make("references.LegalEntity", legal_entity_id=2)
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        action_date="2010-10-01",
        award_id=2,
        is_fpds=True,
        place_of_performance_id=1,
        type="A",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        legal_entity_city_name="BRISTOL",
        legal_entity_country_code="GBR",
        piid="piiiiid",
        place_of_perform_city_name="Mccool Junction",
    )
    mommy.make(
        "awards.Award", id=2, is_fpds=True, latest_transaction_id=2, piid="0001", recipient_id=2, type="A",
        place_of_performance_id=2
    )

    mommy.make("references.LegalEntity", legal_entity_id=3)
    mommy.make(
        "awards.TransactionNormalized",
        id=3,
        action_date="2010-10-01",
        award_id=3,
        is_fpds=True,
        place_of_performance_id=2,
        type="A",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=3,
        legal_entity_city_name="BRISBANE",
        piid="0002",
        place_of_perform_city_name="BRISBANE",
    )
    mommy.make(
        "awards.Award", id=3, is_fpds=True, latest_transaction_id=3, piid="0002", recipient_id=2, type="A",
        place_of_performance_id=2
    )

    mommy.make("references.LegalEntity", legal_entity_id=4)
    mommy.make(
        "awards.TransactionNormalized",
        id=4,
        action_date="2010-10-01",
        award_id=4,
        is_fpds=True,
        place_of_performance_id=2,
        type="A",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=4,
        legal_entity_city_name="NEW YORK",
        legal_entity_country_code="USA",
        piid="0003",
        place_of_perform_city_name="NEW YORK",
    )
    mommy.make(
        "awards.Award", id=4, is_fpds=True, latest_transaction_id=4, piid="0003", recipient_id=2, type="A",
        place_of_performance_id=2
    )
    mommy.make("references.LegalEntity", legal_entity_id=4)
    mommy.make(
        "awards.TransactionNormalized",
        id=5,
        action_date="2010-10-01",
        award_id=5,
        is_fpds=True,
        place_of_performance_id=2,
        type="A",
    )
    mommy.make(
        "awards.TransactionFPDS",
        transaction_id=5,
        legal_entity_city_name="NEW AMSTERDAM",
        legal_entity_country_code="USA",
        piid="0004",
        place_of_perform_city_name="NEW AMSTERDAM",
    )
    mommy.make(
        "awards.Award", id=5, is_fpds=True, latest_transaction_id=5, piid="0004", recipient_id=2, type="A",
        place_of_performance_id=2
    )


def test_geocode_filter_locations(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()

    to = UniversalAwardView.objects

    values = [
        {"city": "HOUSTON", "state": "TX", "country": "USA"},
        {"city": "BURBANK", "state": "CA", "country": "USA"},
    ]

    assert to.filter(geocode_filter_locations("nothing", [], True)).count() == 5
    assert to.filter(geocode_filter_locations("pop", values, True)).count() == 1
    assert to.filter(geocode_filter_locations("recipient_location", values, True)).count() == 1

    values = [
        {"city": "AUSTIN", "state": "TX", "country": "USA"},
        {"city": "BURBANK", "state": "TX", "country": "USA"},
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
    assert location_error_handling({"country": "", "state": "", "county": "", "district": "", "feet": ""}) is None


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

    to = UniversalTransactionView.objects

    assert to.filter(create_city_name_queryset("nothing", [], "nothing", "nothing")).count() == 0

    assert to.filter(create_city_name_queryset("pop", "transaction_id", ["Houston"], "USA", None)).count() == 1
    assert to.filter(create_city_name_queryset("pop", "transaction_id", ["Houston"], "USA", "TX")).count() == 1
    assert to.filter(create_city_name_queryset("pop", "transaction_id", ["Houston"], "USA", "VA")).count() == 0
    assert (
        to.filter(create_city_name_queryset("pop", "transaction_id", ["Houston", "Burbank"], "USA", "TX")).count() == 1
    )
    assert to.filter(create_city_name_queryset("pop", "transaction_id", ["Burbank"], "USA", "CA")).count() == 0

    assert (
        to.filter(create_city_name_queryset("recipient_location", "transaction_id", ["Burbank"], "USA", None)).count()
        == 1
    )
    assert (
        to.filter(create_city_name_queryset("recipient_location", "transaction_id", ["Burbank"], "USA", "CA")).count()
        == 1
    )
    assert (
        to.filter(create_city_name_queryset("recipient_location", "transaction_id", ["Burbank"], "USA", "VA")).count()
        == 0
    )
    assert (
        to.filter(
            create_city_name_queryset("recipient_location", "transaction_id", ["Burbank", "Houston"], "USA", "CA")
        ).count()
        == 1
    )
    assert to.filter(create_city_name_queryset("recipient_location", "award_id", ["Houston"], "USA", "TX")).count() == 0
    assert to.filter(create_city_name_queryset("recipient_location", "award_id", ["BRISTOL"], "GBR")).count() == 1
    assert to.filter(create_city_name_queryset("recipient_location", "award_id", ["BRISTOL"], "FOREIGN")).count() == 1
    assert to.filter(create_city_name_queryset("recipient_location", "award_id", ["BRISTOL"], "USA")).count() == 0
    assert to.filter(create_city_name_queryset("recipient_location", "award_id", ["BRISBANE"], "USA")).count() == 1
    assert to.filter(create_city_name_queryset("recipient_location", "award_id", ["BRISBANE"], "FOREIGN")).count() == 0
    assert to.filter(create_city_name_queryset("recipient_location", "award_id", ["NEW YORK"], "USA")).count() == 1
    assert to.filter(create_city_name_queryset("recipient_location", "award_id", ["NEW AMSTERDAM"], "USA")).count() == 1
    assert (
        to.filter(create_city_name_queryset("recipient_location", "transaction_id", ["Houston"], "USA", "TX")).count()
        == 0
    )


def test_get_award_ids_by_city(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()

    # assert len(get_record_ids_by_city("nothing", "nothing", "nothing", "nothing", "nothing")) == 0

    assert len(get_record_ids_by_city("pop", "award_id", "Houston", "USA", None)) == 1
    assert len(get_record_ids_by_city("pop", "award_id", "Houston", "USA", "TX")) == 1
    assert len(get_record_ids_by_city("pop", "award_id", "Houston", "USA", "VA")) == 0
    assert len(get_record_ids_by_city("pop", "award_id", "Burbank", "USA", "CA")) == 0

    assert len(get_record_ids_by_city("recipient_location", "award_id", "Burbank", "USA", None)) == 1
    assert len(get_record_ids_by_city("recipient_location", "award_id", "Burbank", "USA", "CA")) == 1
    assert len(get_record_ids_by_city("recipient_location", "award_id", "Burbank", "USA", "VA")) == 0
    assert len(get_record_ids_by_city("recipient_location", "award_id", "Houston", "USA", "TX")) == 0
    assert len(get_record_ids_by_city("recipient_location", "award_id", "BRISTOL", "GBR")) == 1
    assert len(get_record_ids_by_city("recipient_location", "award_id", "BRISTOL", "FOREIGN")) == 1
    assert len(get_record_ids_by_city("recipient_location", "award_id", "BRISTOL", "USA")) == 0
    assert len(get_record_ids_by_city("recipient_location", "award_id", "BRISBANE", "FOREIGN")) == 0
    assert len(get_record_ids_by_city("recipient_location", "award_id", "BRISBANE", "USA")) == 1
    assert len(get_record_ids_by_city("recipient_location", "award_id", "Burbank", "USA", None)) == 1
    assert len(get_record_ids_by_city("recipient_location", "award_id", "Burbank", "USA", "CA")) == 1
    assert len(get_record_ids_by_city("recipient_location", "award_id", "Burbank", "USA", "VA")) == 0
    assert len(get_record_ids_by_city("recipient_location", "award_id", "Houston", "USA", "TX")) == 0


def test_elasticsearch_results(award_data_fixture, elasticsearch_transaction_index):
    elasticsearch_transaction_index.update_index()

    query = {
        "_source": ["award_id"],
        "size": 0,
        "query": {"match_all": {}},
        "aggs": {"id_groups": {"terms": {"field": "award_id", "size": 5}}},
    }
    results = elasticsearch_results(query)
    assert results is not None
    assert len(results) == 5
    assert results[0] == 1
    assert results[1] == 2
    assert results[2] == 3
    assert results[3] == 4
    assert results[4] == 5

    query = {
        "_source": ["award_id"],
        "size": 0,
        "query": {"match": {"pop_country": "XXXXX"}},
        "aggs": {"id_groups": {"terms": {"field": "award_id", "size": 5}}},
    }
    results = elasticsearch_results(query)
    assert results is not None
    assert len(results) == 0
