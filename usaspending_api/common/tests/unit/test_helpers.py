import datetime as dt
import re
import time
from datetime import datetime

import pytest

from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_month, generate_fiscal_year
from usaspending_api.common.helpers.generic_helper import get_pagination
from usaspending_api.common.helpers.timing_helpers import timer

legal_dates = {
    dt.datetime(2017, 2, 2, 16, 43, 28, 377373): 2017,
    dt.date(2017, 2, 2): 2017,
    dt.datetime(2017, 10, 2, 16, 43, 28, 377373): 2018,
    dt.date(2017, 10, 2): 2018,
}

not_dates = (0, 2017.2, "forthwith")


def test_generate_fiscal_period_beginning_of_fiscal_year():
    date = datetime.strptime("10/01/2018", "%m/%d/%Y")
    expected = 1
    actual = generate_fiscal_month(date)
    assert actual == expected


def test_generate_fiscal_period_end_of_fiscal_year():
    date = datetime.strptime("09/30/2019", "%m/%d/%Y")
    expected = 12
    actual = generate_fiscal_month(date)
    assert actual == expected


def test_generate_fiscal_period_middle_of_fiscal_year():
    date = datetime.strptime("01/01/2019", "%m/%d/%Y")
    expected = 4
    actual = generate_fiscal_month(date)
    assert actual == expected


def test_generate_fiscal_period_incorrect_data_type_string():
    with pytest.raises(TypeError):
        generate_fiscal_month("2019")


def test_generate_fiscal_period_incorrect_data_type_int():
    with pytest.raises(TypeError):
        generate_fiscal_month(2019)


def test_generate_fiscal_period_malformed_date_month_year():
    date = datetime.strptime("10/2018", "%m/%Y").date
    with pytest.raises(Exception):
        generate_fiscal_month(date)


# example of a simple unit test
def test_beginning_of_fiscal_year():
    date = datetime.strptime("10/01/2018", "%m/%d/%Y")
    expected = 2019
    actual = generate_fiscal_year(date)
    assert actual == expected


def test_end_of_fiscal_year():
    date = datetime.strptime("09/30/2019", "%m/%d/%Y")
    expected = 2019
    actual = generate_fiscal_year(date)
    assert actual == expected


def test_middle_of_fiscal_year():
    date = datetime.strptime("01/01/2019", "%m/%d/%Y")
    expected = 2019
    actual = generate_fiscal_year(date)
    assert actual == expected


def test_incorrect_data_type_string():
    with pytest.raises(TypeError):
        generate_fiscal_year("2019")


def test_incorrect_data_type_int():
    with pytest.raises(TypeError):
        generate_fiscal_year(2019)


def test_malformed_date_month_year():
    date = datetime.strptime("10/2018", "%m/%Y").date
    with pytest.raises(Exception):
        generate_fiscal_year(date)


def test_pagination():
    # Testing for if anything breaks for the special case of an empty list
    results = []
    empty_page_metadata = {"next": None, "previous": None, "hasNext": False, "hasPrevious": False, "count": 0}
    assert get_pagination(results, 1, 1) == ([], {**empty_page_metadata, **{"page": 1}})
    assert get_pagination(results, 1, 4) == ([], {**empty_page_metadata, **{"page": 4}})
    assert get_pagination(results, 3, 1) == ([], {**empty_page_metadata, **{"page": 1}})
    assert get_pagination(results, 3, 2) == ([], {**empty_page_metadata, **{"page": 2}})
    assert get_pagination(results, 1, 6) == ([], {**empty_page_metadata, **{"page": 6}})
    assert get_pagination(results, 5, 2) == ([], {**empty_page_metadata, **{"page": 2}})
    assert get_pagination(results, 1000, 1) == ([], {**empty_page_metadata, **{"page": 1}})
    assert get_pagination(results, 1000, 2) == ([], {**empty_page_metadata, **{"page": 2}})
    assert get_pagination(results, 0, 1) == ([], {**empty_page_metadata, **{"page": 1}})
    assert get_pagination(results, 10, 0) == ([], {**empty_page_metadata, **{"page": 0}})

    # Normal tests
    results = ["A", "B", "C", "D", "E"]
    populated_page_metadata = {"next": 2, "hasNext": True, "count": 5, "page": 1}
    assert get_pagination(results, 1, 1) == (["A"], {**empty_page_metadata, **populated_page_metadata})
    populated_page_metadata = {"next": 5, "hasNext": True, "previous": 3, "hasPrevious": True, "count": 5, "page": 4}
    assert get_pagination(results, 1, 4) == (["D"], {**empty_page_metadata, **populated_page_metadata})
    populated_page_metadata = {"next": 2, "hasNext": True, "count": 5, "page": 1}
    assert get_pagination(results, 3, 1) == (["A", "B", "C"], {**empty_page_metadata, **populated_page_metadata})
    populated_page_metadata = {"previous": 1, "hasPrevious": True, "count": 5, "page": 2}
    assert get_pagination(results, 3, 2) == (["D", "E"], {**empty_page_metadata, **populated_page_metadata})
    # Testing special cases
    populated_page_metadata = {"previous": 5, "hasPrevious": True, "count": 5, "page": 6}
    assert get_pagination(results, 1, 6) == ([], {**empty_page_metadata, **populated_page_metadata})
    populated_page_metadata = {"previous": 1, "hasPrevious": True, "count": 5, "page": 2}
    assert get_pagination(results, 5, 2) == ([], {**empty_page_metadata, **populated_page_metadata})
    populated_page_metadata = {"page": 1, "count": 5}
    assert get_pagination(results, 1000, 1) == (
        ["A", "B", "C", "D", "E"],
        {**empty_page_metadata, **populated_page_metadata},
    )
    populated_page_metadata = {"previous": 1, "hasPrevious": True, "page": 2, "count": 5}
    assert get_pagination(results, 1000, 2) == ([], {**empty_page_metadata, **populated_page_metadata})
    populated_page_metadata = {"page": 1, "count": 5}
    assert get_pagination(results, 0, 1) == ([], {**empty_page_metadata, **populated_page_metadata})
    populated_page_metadata = {"page": 0, "count": 5}
    assert get_pagination(results, 10, 0) == ([], {**empty_page_metadata, **populated_page_metadata})


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_integer(raw_date, expected_fy):
    assert isinstance(fy(raw_date), int)


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_correct(raw_date, expected_fy):
    assert fy(raw_date) == expected_fy


@pytest.mark.parametrize("not_date", not_dates)
def test_fy_type_exceptions(not_date):
    with pytest.raises(Exception):
        fy(not_date)


def test_timer(capsys):
    """Verify that timer helper executes without error"""

    with timer():
        print("Doing a thing")
    output = capsys.readouterr()[0]
    assert "Beginning" in output
    assert "finished" in output


def test_timer_times(capsys):
    """Verify that timer shows longer times for slower operations"""

    pattern = re.compile(r"([\d\.e\-]+)s")

    with timer():
        print("Doing a thing")
    output0 = capsys.readouterr()[0]
    time0 = float(pattern.search(output0).group(1))

    with timer():
        print("Doing a slower thing")
        time.sleep(0.1)
    output1 = capsys.readouterr()[0]
    time1 = float(pattern.search(output1).group(1))

    assert time1 > time0


def test_fy_none():
    assert fy(None) is None
