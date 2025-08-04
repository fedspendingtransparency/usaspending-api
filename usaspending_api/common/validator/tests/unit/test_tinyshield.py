import copy
from mock import patch
import pytest

from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.helpers import validate_array
from usaspending_api.common.validator.helpers import validate_boolean
from usaspending_api.common.validator.helpers import validate_datetime
from usaspending_api.common.validator.helpers import validate_enum
from usaspending_api.common.validator.helpers import validate_float
from usaspending_api.common.validator.helpers import validate_integer
from usaspending_api.common.validator.helpers import validate_object
from usaspending_api.common.validator.helpers import validate_text
from usaspending_api.common.validator.tinyshield import TinyShield


ARRAY_RULE = {
    "name": "test",
    "type": "array",
    "key": "filters|test",
    "array_type": "integer",
    "optional": True,
    "array_min": 1,
    "array_max": 3,
    "value": [1, 2, 3],
}
BOOLEAN_RULE = {"name": "test", "type": "boolean", "key": "filters|test", "optional": True, "value": True}
DATETIME_RULE = {
    "name": "test",
    "type": "datetime",
    "key": "filters|test",
    "optional": True,
    "value": "1984-09-16T4:05:00",
}
ENUM_RULE = {
    "name": "test",
    "type": "enum",
    "key": "filters|test",
    "enum_values": ["foo", "bar"],
    "optional": True,
    "value": "foo",
}
FLOAT_RULE = {
    "name": "test",
    "type": "float",
    "key": "filters|test",
    "optional": True,
    "value": 3.14,
    "min": 2,
    "max": 4,
}
TEXT_RULE = {
    "name": "test",
    "type": "text",
    "key": "filters|test",
    "optional": True,
    "value": "hello world",
    "text_type": "search",
}
INTEGER_RULE = {
    "name": "test",
    "type": "integer",
    "key": "filters|test",
    "optional": True,
    "value": 3,
    "min": 2,
    "max": 4,
}
OBJECT_RULE = {
    "name": "test",
    "type": "object",
    "key": "filters|test",
    "object_keys": {"foo": {"type": "string", "optional": False}, "hello": {"type": "integer", "optional": False}},
    "value": {"foo": "bar", "hello": 1},
}

FILTER_OBJ = {
    "filters": {
        "keywords": ["grumpy", "bungle"],
        "description": "Test Description",
        "award_type_codes": ["A", "B", "C", "D"],
        "time_period": [{"date_type": "action_date", "start_date": "2008-01-01", "end_date": "2011-01-31"}],
        "place_of_performance_scope": "domestic",
        "place_of_performance_locations": [{"country": "USA", "state": "VA", "county": "059"}],
        "agencies": [
            {"type": "funding", "tier": "toptier", "name": "Office of Pizza"},
            {"type": "awarding", "tier": "subtier", "name": "Personal Pizza"},
        ],
        "recipient_search_text": ["D12345678"],
        "recipient_scope": "domestic",
        "recipient_locations": [{"country": "USA", "state": "VA", "county": "059"}],
        "recipient_type_names": ["Small Business", "Alaskan Native Corporation Owned Firm"],
        "award_ids": ["1605SS17F00018"],
        "award_amounts": [
            {"lower_bound": 1000000.00, "upper_bound": 25000000.00},
            {"upper_bound": 1000000.00},
            {"lower_bound": 500000000.00},
        ],
        "program_numbers": ["10.553"],
        "naics_codes": {"require": [336411]},
        "psc_codes": ["1510"],
        "contract_pricing_type_codes": ["SAMPLECODE123"],
        "set_aside_type_codes": ["SAMPLECODE123"],
        "extent_competed_type_codes": ["SAMPLECODE123"],
    }
}


@pytest.fixture()
def tinyshield():
    """We want this test to fail if either AWARD_FILTERS has an invalid model,
    OR if the logic of the check_models function has been corrupted.
    It will fail if an exception is raised. Otherwise it will define the ts fixture object
    so we can use it in the remaining tests."""
    ts = TinyShield(copy.deepcopy(AWARD_FILTER))
    yield ts


"""
Because these functions all raise Exceptions on failure, all we need to do to write the unit tests is call the function.
If an exception is raised, the test will fail
"""


def test_validate_array():
    validate_array(ARRAY_RULE)


def test_validate_boolean():
    validate_boolean(BOOLEAN_RULE)


def test_validate_datetime():
    validate_datetime(DATETIME_RULE)


def test_validate_enum():
    validate_enum(ENUM_RULE)


def test_validate_float():
    validate_float(FLOAT_RULE)


def test_validate_integer():
    validate_integer(INTEGER_RULE)


def test_validate_text():
    validate_text(TEXT_RULE)


def test_validate_object():
    validate_object(OBJECT_RULE)


def test_recurse_appendt(tinyshield):
    mydict = {}
    struct = ["level1", "level2"]
    data = "foobar"
    tinyshield.recurse_append(struct, mydict, data)
    assert mydict == {"level1": {"level2": "foobar"}}


def test_parse_request(tinyshield):
    request = FILTER_OBJ
    tinyshield.parse_request(request)
    assert all("value" in item for item in tinyshield.rules)


def test_enforce_rules(tinyshield):
    request = FILTER_OBJ
    tinyshield.parse_request(request)
    tinyshield.enforce_rules()
    assert tinyshield.data == FILTER_OBJ


# Test the "any" rule.
def test_any_rule():
    models = [
        {
            "name": "value",
            "key": "value",
            "type": "any",
            "models": [{"type": "integer"}, {"type": "text", "text_type": "search"}],
        }
    ]

    # Test integer and random other key.
    ts = TinyShield(models).block({"value": 1, "another_value": 2})
    assert ts["value"] == 1
    assert ts.get("another_value") is None

    # Test integer masquerading as a string.
    ts = TinyShield(models).block({"value": "1"})
    assert ts["value"] == 1

    # Test string.
    ts = TinyShield(models).block({"value": "XYZ"})
    assert ts["value"] == "XYZ"

    # Test list (which should blow up).
    with pytest.raises(UnprocessableEntityException):
        TinyShield(models).block({"value": ["XYZ"]})

    # Test with optional 'value' missing.
    ts = TinyShield(models).block({"another_value": 2})
    assert ts.get("value") is None
    assert ts.get("another_value") is None

    # Make 'value' required then run the same tests as above.
    models[0]["optional"] = False

    # Test integer and random other key.
    ts = TinyShield(models).block({"value": 1, "another_value": 2})
    assert ts["value"] == 1
    assert ts.get("another_value") is None

    # Test integer masquerading as a string.
    ts = TinyShield(models).block({"value": "1"})
    assert ts["value"] == 1

    # Test string.
    ts = TinyShield(models).block({"value": "XYZ"})
    assert ts["value"] == "XYZ"

    # Test list (which should blow up).
    with pytest.raises(UnprocessableEntityException):
        TinyShield(models).block({"value": ["XYZ"]})

    # Test with required 'value' missing.
    with pytest.raises(UnprocessableEntityException):
        TinyShield(models).block({"another_value": 2})


@patch("usaspending_api.common.validator.tinyshield.TinyShield.check_models")
def test_enforce_object_keys_min(check_models_patch):
    model = {
        "name": "program_activities",
        "type": "array",
        "array_type": "object",
        "object_keys_min": 1,
        "object_keys": {
            "name": {
                "type": "text",
            },
            "code": {
                "type": "integer",
            },
        },
    }
    tiny_shield = TinyShield(model)

    # Test success
    data = {"filters": {"program_activities": [{"code": "123"}]}}
    tiny_shield.enforce_object_keys_min(data, model)

    data = {"filters": {"program_activities": [{"code": "123"}, {"name": "desmond"}]}}
    tiny_shield.enforce_object_keys_min(data, model)

    # Test failure
    with pytest.raises(
        UnprocessableEntityException,
        match="Required number of minimum object keys is not met.",
    ):
        data = {"filters": {"program_activities": [{"code": "123"}, {}]}}
        tiny_shield.enforce_object_keys_min(data, model)

    with pytest.raises(
        UnprocessableEntityException,
        match="The object provided has no children. If the object is used it needs at least one child.",
    ):
        data = {"filters": {"program_activities": []}}
        tiny_shield.enforce_object_keys_min(data, model)
