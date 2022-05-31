import pytest

from model_bakery import baker
from usaspending_api.search.models import ContractAwardSearchMatview
from usaspending_api.awards.v2.filters.location_filter_geocode import (
    create_nested_object,
    geocode_filter_locations,
    get_fields_list,
    location_error_handling,
    validate_location_keys,
)
from usaspending_api.common.exceptions import InvalidParameterException


@pytest.fixture
def award_data_fixture(db):
    baker.make("awards.TransactionNormalized", id=1, action_date="2010-10-01", award_id=1, is_fpds=True, type="A")
    baker.make(
        "awards.TransactionFPDS",
        transaction_id=1,
        legal_entity_city_name="BURBANK",
        legal_entity_country_code="USA",
        legal_entity_state_code="CA",
        piid="piiiiid",
        place_of_perform_city_name="AUSTIN",
        place_of_performance_state="TX",
        place_of_perform_country_c="USA",
    )
    baker.make("awards.Award", id=1, is_fpds=True, latest_transaction_id=1, piid="piiiiid", type="A")

    baker.make("awards.TransactionNormalized", id=2, action_date="2010-10-01", award_id=2, is_fpds=True, type="A")
    baker.make(
        "awards.TransactionFPDS",
        transaction_id=2,
        legal_entity_city_name="BRISTOL",
        legal_entity_country_code="GBR",
        piid="piiiiid",
        place_of_perform_city_name="MCCOOL JUNCTION",
        place_of_performance_state="TX",
        place_of_perform_country_c="USA",
    )
    baker.make("awards.Award", id=2, is_fpds=True, latest_transaction_id=2, piid="0001", type="A")

    baker.make("awards.TransactionNormalized", id=3, action_date="2010-10-01", award_id=3, is_fpds=True, type="A")
    baker.make(
        "awards.TransactionFPDS",
        transaction_id=3,
        legal_entity_city_name="BRISBANE",
        piid="0002",
        place_of_perform_city_name="BRISBANE",
        place_of_performance_state="NE",
        place_of_perform_country_c="USA",
    )
    baker.make("awards.Award", id=3, is_fpds=True, latest_transaction_id=3, piid="0002", type="A")

    baker.make("awards.TransactionNormalized", id=4, action_date="2010-10-01", award_id=4, is_fpds=True, type="A")
    baker.make(
        "awards.TransactionFPDS",
        transaction_id=4,
        legal_entity_city_name="NEW YORK",
        legal_entity_country_code="USA",
        piid="0003",
        place_of_perform_city_name="NEW YORK",
        place_of_performance_state="NE",
        place_of_perform_country_c="USA",
    )
    baker.make("awards.Award", id=4, is_fpds=True, latest_transaction_id=4, piid="0003", type="A")
    baker.make("awards.TransactionNormalized", id=5, action_date="2010-10-01", award_id=5, is_fpds=True, type="A")
    baker.make(
        "awards.TransactionFPDS",
        transaction_id=5,
        legal_entity_city_name="NEW AMSTERDAM",
        legal_entity_country_code="USA",
        piid="0004",
        place_of_perform_city_name="NEW AMSTERDAM",
        place_of_performance_state="NE",
        place_of_perform_country_c="USA",
    )
    baker.make("awards.Award", id=5, is_fpds=True, latest_transaction_id=5, piid="0004", type="A")

    baker.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")


def test_geocode_filter_locations(award_data_fixture):

    to = ContractAwardSearchMatview.objects

    values = [
        {"city": "McCool Junction", "state": "TX", "country": "USA"},
        {"city": "Burbank", "state": "CA", "country": "USA"},
    ]

    assert to.filter(geocode_filter_locations("nothing", [])).count() == 5
    assert to.filter(geocode_filter_locations("pop", values)).count() == 1
    assert to.filter(geocode_filter_locations("recipient_location", values)).count() == 1

    values = [
        {"city": "Houston", "state": "TX", "country": "USA"},
        {"city": "McCool Junction", "state": "TX", "country": "USA"},
    ]

    assert to.filter(geocode_filter_locations("pop", values)).count() == 1
    assert to.filter(geocode_filter_locations("recipient_location", values)).count() == 0


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
            "city": ["CHICAGO"],
            "zip": ["12345", "12345"],
            "IL": {"county": ["YES"], "district": ["ALSO YES"], "city": ["CHICAGO"]},
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
