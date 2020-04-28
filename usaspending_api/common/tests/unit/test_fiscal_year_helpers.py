from fiscalyear import FiscalDate
from datetime import date

from usaspending_api.common.helpers.fiscal_year_helpers import *


def test_all_fiscal_years():
    range_list = list(range(2001))
    assert range_list == create_fiscal_year_list(start_year=0, end_year=2001)


def test_current_fiscal_year():
    current_fiscal_year = FiscalDate.today().fiscal_year
    fiscal_year_list = create_fiscal_year_list(start_year=2010)
    assert fiscal_year_list[0] == 2010
    assert fiscal_year_list[-1] == current_fiscal_year


def test_create_fiscal_year_list():
    assert create_fiscal_year_list(start_year=2004, end_year=2008) == [2004, 2005, 2006, 2007]
    years = [x for x in range(2000, FiscalDate.today().next_fiscal_year.fiscal_year)]
    assert create_fiscal_year_list() == years


def test_calculate_fiscal_years():
    assert generate_fiscal_year(date(2000, 9, 30)) == 2000
    assert generate_fiscal_year(date(2001, 10, 1)) == 2002
    assert generate_fiscal_year(date(2020, 3, 2)) == 2020
    assert generate_fiscal_year(date(2017, 5, 30)) == 2017
    assert generate_fiscal_year(date(2019, 10, 30)) == 2020


def test_generate_fiscal_month():
    assert generate_fiscal_month(date(2000, 9, 30)) == 12
    assert generate_fiscal_month(date(2001, 10, 1)) == 1
    assert generate_fiscal_month(date(2020, 3, 2)) == 6
    assert generate_fiscal_month(date(2017, 5, 30)) == 8
    assert generate_fiscal_month(date(2019, 10, 30)) == 1


def test_generate_fiscal_quarter():
    assert generate_fiscal_quarter(date(2000, 9, 30)) == 4
    assert generate_fiscal_quarter(date(2001, 10, 1)) == 1
    assert generate_fiscal_quarter(date(2020, 3, 2)) == 2
    assert generate_fiscal_quarter(date(2017, 5, 30)) == 3
    assert generate_fiscal_quarter(date(2019, 10, 30)) == 1


def test_generate_fiscal_year_and_quarter():
    assert generate_fiscal_year_and_quarter(date(2000, 9, 30)) == "2000-Q4"
    assert generate_fiscal_year_and_quarter(date(2001, 10, 1)) == "2002-Q1"
    assert generate_fiscal_year_and_quarter(date(2020, 3, 2)) == "2020-Q2"
    assert generate_fiscal_year_and_quarter(date(2017, 5, 30)) == "2017-Q3"
    assert generate_fiscal_year_and_quarter(date(2019, 10, 30)) == "2020-Q1"


def test_dates_are_fiscal_year_bookends():
    date_1 = date(2000, 9, 30)
    date_2 = date(2001, 10, 1)
    date_3 = date(2020, 3, 2)
    date_4 = date(2017, 5, 30)
    date_5 = date(2019, 10, 30)
    date_6 = date(1998, 10, 1)

    assert dates_are_fiscal_year_bookends(date_1, date_2) is False
    assert dates_are_fiscal_year_bookends(date_1, date_3) is False
    assert dates_are_fiscal_year_bookends(date_2, date_4) is False
    assert dates_are_fiscal_year_bookends(date_1, date_5) is False
    assert dates_are_fiscal_year_bookends(date_6, date_1) is True
