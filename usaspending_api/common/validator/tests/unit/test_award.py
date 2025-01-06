"""
Some sanity checks for the pre-built TinyShield Awards rules.
"""

import pytest

from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.common.validator.award import (
    get_generated_award_id_model,
    get_internal_award_id_model,
    get_internal_or_generated_award_id_model,
)
from usaspending_api.common.validator.helpers import MAX_INT, MAX_ITEMS, MIN_INT


def test_get_generate_award_id_rule():
    models = [get_generated_award_id_model()]
    r = TinyShield(models).block({"award_id": "abcd"})
    assert r == {"award_id": "abcd"}

    models = [get_generated_award_id_model()]
    r = TinyShield(models).block({"award_id": "A" * MAX_ITEMS})
    assert r == {"award_id": "A" * MAX_ITEMS}

    models = [get_generated_award_id_model(key="my_award_id")]
    r = TinyShield(models).block({"my_award_id": "abcd"})
    assert r == {"my_award_id": "abcd"}

    models = [get_generated_award_id_model(key="my_award_id", name="your_award_id")]
    r = TinyShield(models).block({"my_award_id": "abcd"})
    assert r == {"my_award_id": "abcd"}

    models = [get_generated_award_id_model(key="my_award_id", name="your_award_id", optional=True)]
    r = TinyShield(models).block({"my_award_id": "abcd"})
    assert r == {"my_award_id": "abcd"}

    models = [get_generated_award_id_model(key="my_award_id", name="your_award_id", optional=True)]
    r = TinyShield(models).block({})
    assert r == {}

    # Rule violations.
    ts = TinyShield([get_generated_award_id_model()])

    with pytest.raises(UnprocessableEntityException):
        ts.block({})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"award_id": "A" * (MAX_ITEMS + 1)})
    with pytest.raises(InvalidParameterException):
        ts.block({"award_id": 1.1})
    with pytest.raises(InvalidParameterException):
        ts.block({"award_id": [1, 2]})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"id": "abcd"})


def test_get_internal_award_id_rule():
    models = [get_internal_award_id_model()]
    r = TinyShield(models).block({"award_id": 12345})
    assert r == {"award_id": 12345}

    models = [get_internal_award_id_model()]
    r = TinyShield(models).block({"award_id": "12345"})
    assert r == {"award_id": 12345}

    models = [get_internal_award_id_model()]
    r = TinyShield(models).block({"award_id": MAX_INT})
    assert r == {"award_id": MAX_INT}

    models = [get_internal_award_id_model()]
    r = TinyShield(models).block({"award_id": MIN_INT})
    assert r == {"award_id": MIN_INT}

    models = [get_internal_award_id_model(key="my_award_id")]
    r = TinyShield(models).block({"my_award_id": 12345})
    assert r == {"my_award_id": 12345}

    models = [get_internal_award_id_model(key="my_award_id", name="your_award_id")]
    r = TinyShield(models).block({"my_award_id": 12345})
    assert r == {"my_award_id": 12345}

    models = [get_internal_award_id_model(key="my_award_id", name="your_award_id", optional=True)]
    r = TinyShield(models).block({"my_award_id": 12345})
    assert r == {"my_award_id": 12345}

    models = [get_internal_award_id_model(key="my_award_id", name="your_award_id", optional=True)]
    r = TinyShield(models).block({})
    assert r == {}

    # Rule violations.
    ts = TinyShield([get_internal_award_id_model()])

    with pytest.raises(UnprocessableEntityException):
        ts.block({})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"award_id": MAX_INT + 1})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"award_id": MIN_INT - 1})
    with pytest.raises(InvalidParameterException):
        ts.block({"award_id": 1.1})
    with pytest.raises(InvalidParameterException):
        ts.block({"award_id": [1, 2]})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"id": "abcde"})


def test_get_internal_or_generated_award_id_rule():
    models = [get_internal_or_generated_award_id_model()]
    r = TinyShield(models).block({"award_id": "abcd"})
    assert r == {"award_id": "abcd"}

    models = [get_internal_or_generated_award_id_model()]
    r = TinyShield(models).block({"award_id": "A" * MAX_ITEMS})
    assert r == {"award_id": "A" * MAX_ITEMS}

    models = [get_internal_or_generated_award_id_model(key="my_award_id")]
    r = TinyShield(models).block({"my_award_id": "abcd"})
    assert r == {"my_award_id": "abcd"}

    models = [get_internal_or_generated_award_id_model(key="my_award_id", name="your_award_id")]
    r = TinyShield(models).block({"my_award_id": "abcd"})
    assert r == {"my_award_id": "abcd"}

    models = [get_internal_or_generated_award_id_model(key="my_award_id", name="your_award_id", optional=True)]
    r = TinyShield(models).block({"my_award_id": "abcd"})
    assert r == {"my_award_id": "abcd"}

    models = [get_internal_or_generated_award_id_model()]
    r = TinyShield(models).block({"award_id": 12345})
    assert r == {"award_id": 12345}

    models = [get_internal_or_generated_award_id_model()]
    r = TinyShield(models).block({"award_id": "12345"})
    assert r == {"award_id": 12345}

    models = [get_internal_or_generated_award_id_model()]
    r = TinyShield(models).block({"award_id": MAX_INT})
    assert r == {"award_id": MAX_INT}

    models = [get_internal_or_generated_award_id_model()]
    r = TinyShield(models).block({"award_id": MIN_INT})
    assert r == {"award_id": MIN_INT}

    models = [get_internal_or_generated_award_id_model(key="my_award_id")]
    r = TinyShield(models).block({"my_award_id": 12345})
    assert r == {"my_award_id": 12345}

    models = [get_internal_or_generated_award_id_model(key="my_award_id", name="your_award_id")]
    r = TinyShield(models).block({"my_award_id": 12345})
    assert r == {"my_award_id": 12345}

    models = [get_internal_or_generated_award_id_model(key="my_award_id", name="your_award_id", optional=True)]
    r = TinyShield(models).block({"my_award_id": 12345})
    assert r == {"my_award_id": 12345}

    models = [get_internal_or_generated_award_id_model(key="my_award_id", name="your_award_id", optional=True)]
    r = TinyShield(models).block({})
    assert r == {}


def test_get_internal_or_generated_award_id_rule_bad():

    # Rule violations.
    ts = TinyShield([get_internal_or_generated_award_id_model()])

    with pytest.raises(UnprocessableEntityException):
        ts.block({})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"award_id": "B" * (MAX_ITEMS + 1)})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"award_id": MAX_INT + 1})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"award_id": MIN_INT - 1})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"award_id": 1.1})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"award_id": [1, 2]})
    with pytest.raises(UnprocessableEntityException):
        ts.block({"id": "abcde"})
