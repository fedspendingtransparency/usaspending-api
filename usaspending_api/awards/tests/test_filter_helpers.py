from datetime import datetime
from django.conf import settings
from django.db import models

from model_mommy import mommy
from django.db.models.base import ModelBase
from usaspending_api.awards.v2.filters.filter_helpers import combine_date_range_queryset
from usaspending_api.awards.v2.filters.filter_helpers import merge_date_ranges

# TABLE = models.Model()
# TABLE = ModelBase('__TestModel__', (), {'__module__': '__module__'})
# TABLE = mommy.make(
#     'references.Location',
#     location_country_code='ABC',
#     state_code="AA", county_code='001',
#     congressional_code='01',
#     zip5='12345',
# )
DATE_COL = 'dummy_col'
MIN = settings.API_SEARCH_MIN_DATE
MAX = settings.API_MAX_DATE

date_range_1 = {'start_date': '2010-05-05', 'end_date': '2010-05-25'}
date_range_2 = {'start_date': '2010-05-27', 'end_date': '2014-05-25'}
date_range_3 = {'start_date': '2009-01-04', 'end_date': '2015-12-23'}
date_range_4 = {'start_date': '2008-02-19', 'end_date': '2018-03-11'}
fy_2010 = {'start_date': '2009-10-01', 'end_date': '2010-09-30'}
fy_2011 = {'start_date': '2010-10-01', 'end_date': '2011-09-30'}
fy_2012 = {'start_date': '2011-10-01', 'end_date': '2012-09-30'}


# Helper functions for the tests
def _str_to_datetime(dt, fmt='%Y-%m-%d'):
    return datetime.strptime(dt, fmt)


def _convert_ranges_to_dt_list(*args):
    return [
        (
            _str_to_datetime(v.get('start_date')),
            _str_to_datetime(v.get('end_date'))
        ) for v in args]


# Begin test fixtures here
def test_one_range_contained_in_another():
    expected = [(_str_to_datetime(date_range_3['start_date']), _str_to_datetime(date_range_3['end_date']))]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(date_range_2, date_range_3)))
    assert results == expected


def test_two_sequential_fiscal_year_ranges():
    expected = [(_str_to_datetime(fy_2010['start_date']), _str_to_datetime(fy_2011['end_date']))]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(fy_2010, fy_2011)))
    assert results == expected


def test_three_sequential_fiscal_year_ranges():
    expected = [(_str_to_datetime(fy_2010['start_date']), _str_to_datetime(fy_2012['end_date']))]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(fy_2010, fy_2011, fy_2012)))
    assert results == expected


def test_two_apart_fiscal_year_ranges():
    expected = [
        (_str_to_datetime(fy_2010['start_date']), _str_to_datetime(fy_2010['end_date'])),
        (_str_to_datetime(fy_2012['start_date']), _str_to_datetime(fy_2012['end_date']))]
    results = list(merge_date_ranges(_convert_ranges_to_dt_list(fy_2010, fy_2012)))
    assert results == expected


# def test_combine_range_1():
    # expected = {'start_date': '2009-10-01', 'end_date': '2011-09-30'}
    # results = combine_date_range_queryset([fy_2010, fy_2011], TABLE, DATE_COL, MIN, MAX)
