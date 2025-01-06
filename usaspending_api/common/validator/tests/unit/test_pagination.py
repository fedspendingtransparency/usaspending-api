"""
Some sanity checks for the pre-built TinyShield PAGINATION rules.
"""

import pytest

from copy import deepcopy

from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns, PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


def test_default_pagination():
    pagination = deepcopy(PAGINATION)

    r = TinyShield(pagination).block({})
    assert r == {"page": 1, "limit": 10, "order": "desc"}


def test_non_default_overridden_pagination():
    pagination = deepcopy(PAGINATION)

    r = TinyShield(pagination).block({"page": 2, "limit": 11, "sort": "whatever", "order": "asc"})
    assert r == {"page": 2, "limit": 11, "sort": "whatever", "order": "asc"}


def test_invalid_pagination_values():
    pagination = deepcopy(PAGINATION)

    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"page": "minus one"})
    with pytest.raises(UnprocessableEntityException):
        TinyShield(pagination).block({"page": -1})
    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"page": ["test"]})

    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"limit": "minus one"})
    with pytest.raises(UnprocessableEntityException):
        TinyShield(pagination).block({"limit": -1})
    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"limit": ["test"]})

    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"sort": -1})
    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"sort": ["test"]})

    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"order": "whatever"})
    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"order": -1})
    with pytest.raises(InvalidParameterException):
        TinyShield(pagination).block({"order": ["test"]})


def test_customized_pagination():
    pagination = customize_pagination_with_sort_columns(["field1", "fieldA", "fieldOne"])

    r = TinyShield(pagination).block({})
    assert r == {"page": 1, "limit": 10, "order": "desc"}


def test_customized_pagination_with_a_default_sort_column():
    pagination = customize_pagination_with_sort_columns(["field1", "fieldA", "fieldOne"], "fieldOne")

    r = TinyShield(pagination).block({})
    assert r == {"page": 1, "limit": 10, "order": "desc", "sort": "fieldOne"}


def test_invalid_customized_pagination_values():
    with pytest.raises(TypeError):
        customize_pagination_with_sort_columns(None)
    with pytest.raises(TypeError):
        customize_pagination_with_sort_columns(1)
    with pytest.raises(TypeError):
        customize_pagination_with_sort_columns([1, 2, 3])
    with pytest.raises(ValueError):
        customize_pagination_with_sort_columns(["field1", "fieldA", "fieldOne"], 1)
    with pytest.raises(ValueError):
        customize_pagination_with_sort_columns(["field1", "fieldA", "fieldOne"], "fieldTwo")

    # This should work.  Last second sanity check.
    customize_pagination_with_sort_columns(["field1", "fieldA", "fieldOne"], "fieldOne")
