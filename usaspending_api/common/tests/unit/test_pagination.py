from types import SimpleNamespace

import pytest

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.pagination import UsaspendingPagination


def _request(**query_params):
    # GET query params arrive as lists; a JSON body would arrive as scalars in .data
    return SimpleNamespace(data={}, query_params={k: [v] for k, v in query_params.items()})


def test_non_numeric_limit_is_a_client_error_not_a_500():
    with pytest.raises(InvalidParameterException):
        UsaspendingPagination().get_limit(_request(limit="abc"))


def test_non_numeric_page_is_a_client_error_not_a_500():
    with pytest.raises(InvalidParameterException):
        UsaspendingPagination().get_page(_request(page="1e9"))


def test_page_beyond_bigint_offset_is_rejected():
    with pytest.raises(InvalidParameterException):
        UsaspendingPagination().get_page(_request(page="99999999999999999999"))


def test_in_range_values_keep_the_old_clamping_behaviour():
    pagination = UsaspendingPagination()
    assert pagination.get_limit(_request(limit="0")) == 1
    assert pagination.get_limit(_request(limit="9999")) == pagination.max_page_size
    assert pagination.get_page(_request(page="0")) == 1
    assert pagination.get_page(_request(page="7")) == 7
    assert pagination.get_limit(SimpleNamespace(data={"limit": 25}, query_params={})) == 25
