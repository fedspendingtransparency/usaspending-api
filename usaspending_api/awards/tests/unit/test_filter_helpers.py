from datetime import datetime
from django.conf import settings

from usaspending_api.awards.v2.filters.filter_helpers import merge_date_ranges

MIN = settings.API_SEARCH_MIN_DATE
MAX = settings.API_MAX_DATE
DATE_RANGE_1 = {"start_date": "2010-05-05", "end_date": "2010-05-25"}
DATE_RANGE_2 = {"start_date": "2010-05-27", "end_date": "2014-05-25"}
DATE_RANGE_3 = {"start_date": "2009-01-04", "end_date": "2015-12-21"}
DATE_RANGE_4 = {"start_date": "2015-12-23", "end_date": "2018-03-11"}
FY_2010 = {"start_date": "2009-10-01", "end_date": "2010-09-30"}
FY_2011 = {"start_date": "2010-10-01", "end_date": "2011-09-30"}
FY_2012 = {"start_date": "2011-10-01", "end_date": "2012-09-30"}


# Helper functions for the tests
def _str_to_datetime(dt, fmt="%Y-%m-%d"):
    return datetime.strptime(dt, fmt)


def _convert_ranges_to_dt_list(*args):
    return [(_str_to_datetime(v.get("start_date")), _str_to_datetime(v.get("end_date"))) for v in args]


# Begin test fixtures here
def test__merge_date_ranges():
    merge_date_ranges_test_one_range()
    merge_date_ranges_test_one_range_contained_in_another()
    merge_date_ranges_test_two_sequential_fiscal_year_ranges()
    merge_date_ranges_test_three_sequential_fiscal_year_ranges()
    merge_date_ranges_test_two_apart_fiscal_year_ranges()
    keep_ranges_two_days_apart_separate()


def merge_date_ranges_test_one_range():
    expected = [(_str_to_datetime(DATE_RANGE_1["start_date"]), _str_to_datetime(DATE_RANGE_1["end_date"]))]
    input_dates = _convert_ranges_to_dt_list(DATE_RANGE_1)
    results = list(merge_date_ranges(input_dates))
    assert results == expected

    # switch order of start and end date in tuple, result should be the same
    reversed_input_dates = [(input_dates[0][1], input_dates[0][0])]
    results = list(merge_date_ranges(reversed_input_dates))
    assert results == expected


def merge_date_ranges_test_one_range_contained_in_another():
    expected = [(_str_to_datetime(DATE_RANGE_3["start_date"]), _str_to_datetime(DATE_RANGE_3["end_date"]))]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(DATE_RANGE_2, DATE_RANGE_3)))
    assert results == expected

    # switch to order of provided date ranges, it shouldn't impact the results
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(DATE_RANGE_3, DATE_RANGE_2)))
    assert results == expected


def merge_date_ranges_test_two_sequential_fiscal_year_ranges():
    expected = [(_str_to_datetime(FY_2010["start_date"]), _str_to_datetime(FY_2011["end_date"]))]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(FY_2010, FY_2011)))
    assert results == expected


def merge_date_ranges_test_three_sequential_fiscal_year_ranges():
    expected = [(_str_to_datetime(FY_2010["start_date"]), _str_to_datetime(FY_2012["end_date"]))]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(FY_2010, FY_2011, FY_2012)))
    assert results == expected


def merge_date_ranges_test_two_apart_fiscal_year_ranges():
    expected = [
        (_str_to_datetime(FY_2010["start_date"]), _str_to_datetime(FY_2010["end_date"])),
        (_str_to_datetime(FY_2012["start_date"]), _str_to_datetime(FY_2012["end_date"])),
    ]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(FY_2010, FY_2012)))
    assert results == expected


def keep_ranges_two_days_apart_separate():
    expected = [
        (_str_to_datetime(DATE_RANGE_3["start_date"]), _str_to_datetime(DATE_RANGE_3["end_date"])),
        (_str_to_datetime(DATE_RANGE_4["start_date"]), _str_to_datetime(DATE_RANGE_4["end_date"])),
    ]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(DATE_RANGE_3, DATE_RANGE_4)))
    assert results == expected
