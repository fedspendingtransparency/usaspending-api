import pytest

from usaspending_api.awards.v2.filters.location_filter_geocode import (
    create_nested_object,
    get_fields_list,
    location_error_handling,
    validate_location_keys,
)
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import (
    INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS,
    DUPLICATE_DISTRICT_LOCATION_PARAMETERS,
)


def test_validate_location_keys():
    assert validate_location_keys([]) is None
    with pytest.raises(InvalidParameterException):
        assert validate_location_keys([{}]) is None
    with pytest.raises(InvalidParameterException):
        assert validate_location_keys([{"district": ""}]) is None
    with pytest.raises(InvalidParameterException):
        assert validate_location_keys([{"county": ""}]) is None
    with pytest.raises(InvalidParameterException, match=INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS):
        validate_location_keys([{"country": "CANADA", "district_original": "01", "state": "WA"}])
    with pytest.raises(InvalidParameterException, match=INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS):
        validate_location_keys([{"country": "USA", "district_original": "01"}])
    with pytest.raises(InvalidParameterException, match=INCOMPATIBLE_DISTRICT_LOCATION_PARAMETERS):
        validate_location_keys([{"country": "USA", "district_original": "01", "state": "WA", "county": "WHATCOM"}])
    with pytest.raises(InvalidParameterException, match=DUPLICATE_DISTRICT_LOCATION_PARAMETERS):
        validate_location_keys([{"country": "USA", "district_original": "01", "state": "WA", "district_current": "99"}])
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
            {
                "country": "USA",
                "zip": "12346",
                "city": "Springfield",
                "state": "IL",
                "district_original": "02",
            },
            {
                "country": "USA",
                "zip": "12346",
                "city": "Springfield",
                "state": "IL",
                "district_current": "02",
            },
            {"country": "USA", "zip": "12345", "city": "Chicago"},
        ]
    ) == {
        "USA": {
            "city": ["CHICAGO"],
            "zip": ["12345", "12346", "12346", "12345"],
            "IL": {
                "county": ["YES"],
                "district": ["ALSO YES"],
                "district_current": ["02"],
                "district_original": ["02"],
                "city": ["CHICAGO", "SPRINGFIELD", "SPRINGFIELD"],
            },
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
