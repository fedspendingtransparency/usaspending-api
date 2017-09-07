import datetime as dt

import pytest

from usaspending_api.common.helpers import fy, get_pagination

legal_dates = {
    dt.datetime(2017, 2, 2, 16, 43, 28, 377373): 2017,
    dt.date(2017, 2, 2): 2017,
    dt.datetime(2017, 10, 2, 16, 43, 28, 377373): 2018,
    dt.date(2017, 10, 2): 2018,
}

not_dates = (0, 2017.2, 'forthwith')


def test_pagination():
    # Testing for if anything breaks for the special case of an empty list
    results = []
    empty_next_previous_metadata = {"next":None, "previous": None, "hasNext": False, "hasPrevious": False}
    assert get_pagination(results, 1, 1) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 1, 4) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 3, 1) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 3, 2) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 1, 6) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 5, 2) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 1000, 1) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 1000, 2) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 0, 1) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 10, 0) == ([], empty_next_previous_metadata)

    # Normal tests
    results = ["A", "B", "C", "D", "E"]
    assert get_pagination(results, 1, 1) == (["A"], {**empty_next_previous_metadata, **{"next":2, "hasNext": True}})
    assert get_pagination(results, 1, 4) == (["D"], {**empty_next_previous_metadata, **{"next":5, "hasNext": True,
                                                                                        "previous": 3, "hasPrevious": True}})
    assert get_pagination(results, 3, 1) == (["A", "B", "C"], {**empty_next_previous_metadata, **{"next":2, "hasNext": True}})
    assert get_pagination(results, 3, 2) == (["D", "E"], {**empty_next_previous_metadata, **{"previous":1, "hasPrevious": True}})
    # Testing special cases
    assert get_pagination(results, 1, 6) == ([], {**empty_next_previous_metadata, **{"previous":5, "hasPrevious": True}})
    assert get_pagination(results, 5, 2) == ([], {**empty_next_previous_metadata, **{"previous":1, "hasPrevious": True}})
    assert get_pagination(results, 1000, 1) == (["A", "B", "C", "D", "E"], empty_next_previous_metadata)
    assert get_pagination(results, 1000, 2) == ([], {**empty_next_previous_metadata, **{"previous":1, "hasPrevious": True}})
    assert get_pagination(results, 0, 1) == ([], empty_next_previous_metadata)
    assert get_pagination(results, 10, 0) == ([], empty_next_previous_metadata)


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_integer(raw_date, expected_fy):
    assert isinstance(fy(raw_date), int)


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_correct(raw_date, expected_fy):
    assert fy(raw_date) == expected_fy


@pytest.mark.parametrize("not_date", not_dates)
def test_fy_type_exceptions(not_date):
    with pytest.raises(TypeError):
        fy(not_date)


def test_fy_none():
    assert fy(None) is None
