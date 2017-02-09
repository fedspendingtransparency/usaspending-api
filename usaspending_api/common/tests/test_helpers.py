import datetime as dt

import pytest

from usaspending_api.common.helpers import fy

legal_dates = {
    dt.datetime(2017, 2, 2, 16, 43, 28, 377373): 2017,
    dt.date(2017, 2, 2): 2017,
    dt.datetime(2017, 10, 2, 16, 43, 28, 377373): 2018,
    dt.date(2017, 10, 2): 2018,
}

not_dates = (0, 2017.2, 'forthwith')


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
