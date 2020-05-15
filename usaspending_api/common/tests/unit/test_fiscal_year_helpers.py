from fiscalyear import FiscalDate
from datetime import date

import usaspending_api.common.helpers.fiscal_year_helpers as fyh


def test_all_fiscal_years():
    range_list = list(range(2001))
    assert range_list == fyh.create_fiscal_year_list(start_year=0, end_year=2001)


def test_current_fiscal_year():
    current_fiscal_year = FiscalDate.today().fiscal_year
    fiscal_year_list = fyh.create_fiscal_year_list(start_year=2010)
    assert fiscal_year_list[0] == 2010
    assert fiscal_year_list[-1] == current_fiscal_year


def test_create_fiscal_year_list():
    assert fyh.create_fiscal_year_list(start_year=2004, end_year=2008) == [2004, 2005, 2006, 2007]
    years = [x for x in range(2000, FiscalDate.today().next_fiscal_year.fiscal_year)]
    assert fyh.create_fiscal_year_list() == years


def test_calculate_fiscal_years():
    assert fyh.generate_fiscal_year(date(2000, 9, 30)) == 2000
    assert fyh.generate_fiscal_year(date(2001, 10, 1)) == 2002
    assert fyh.generate_fiscal_year(date(2020, 3, 2)) == 2020
    assert fyh.generate_fiscal_year(date(2017, 5, 30)) == 2017
    assert fyh.generate_fiscal_year(date(2019, 10, 30)) == 2020


def test_generate_fiscal_month():
    assert fyh.generate_fiscal_month(date(2000, 9, 30)) == 12
    assert fyh.generate_fiscal_month(date(2001, 10, 1)) == 1
    assert fyh.generate_fiscal_month(date(2020, 3, 2)) == 6
    assert fyh.generate_fiscal_month(date(2017, 5, 30)) == 8
    assert fyh.generate_fiscal_month(date(2019, 10, 30)) == 1


def test_generate_fiscal_quarter():
    assert fyh.generate_fiscal_quarter(date(2000, 9, 30)) == 4
    assert fyh.generate_fiscal_quarter(date(2001, 10, 1)) == 1
    assert fyh.generate_fiscal_quarter(date(2020, 3, 2)) == 2
    assert fyh.generate_fiscal_quarter(date(2017, 5, 30)) == 3
    assert fyh.generate_fiscal_quarter(date(2019, 10, 30)) == 1


def test_generate_fiscal_year_and_quarter():
    assert fyh.generate_fiscal_year_and_quarter(date(2000, 9, 30)) == "2000-Q4"
    assert fyh.generate_fiscal_year_and_quarter(date(2001, 10, 1)) == "2002-Q1"
    assert fyh.generate_fiscal_year_and_quarter(date(2020, 3, 2)) == "2020-Q2"
    assert fyh.generate_fiscal_year_and_quarter(date(2017, 5, 30)) == "2017-Q3"
    assert fyh.generate_fiscal_year_and_quarter(date(2019, 10, 30)) == "2020-Q1"


def test_dates_are_fiscal_year_bookends():
    date_1 = date(2000, 9, 30)
    date_2 = date(2001, 10, 1)
    date_3 = date(2020, 3, 2)
    date_4 = date(2017, 5, 30)
    date_5 = date(2019, 10, 30)
    date_6 = date(1998, 10, 1)

    assert fyh.dates_are_fiscal_year_bookends(date_1, date_2) is False
    assert fyh.dates_are_fiscal_year_bookends(date_1, date_3) is False
    assert fyh.dates_are_fiscal_year_bookends(date_2, date_4) is False
    assert fyh.dates_are_fiscal_year_bookends(date_1, date_5) is False
    assert fyh.dates_are_fiscal_year_bookends(date_6, date_1) is True


def test_calculate_last_completed_fiscal_quarter():
    """
    FY2000 Q1 == 1999-10-01 - 1999-12-31 available after 2000-02-14
    FY2000 Q2 == 2000-01-01 - 2000-03-31 available after 2000-05-15
    FY2000 Q3 == 2000-04-01 - 2000-06-30 available after 2000-08-14
    FY2000 Q4 == 2000-07-01 - 2000-09-30 available after 2000-11-14
    """
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(1999, 10, 1)) is None
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(1999, 11, 1)) is None
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(1999, 12, 1)) is None
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 1, 1)) is None
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 2, 1)) is None
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 2, 14)) is None
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 2, 15)) == 1
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 3, 1)) == 1
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 4, 1)) == 1
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 5, 1)) == 1
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 5, 15)) == 1
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 5, 16)) == 2
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 6, 1)) == 2
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 7, 1)) == 2
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 8, 1)) == 2
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 9, 1)) == 3
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 10, 1)) == 3
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 11, 1)) == 3
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2000, 12, 1)) == 4
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2001, 1, 1)) == 4
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2001, 2, 1)) == 4
    assert fyh.calculate_last_completed_fiscal_quarter(2000, date(2001, 3, 1)) == 4

    # Test a fiscal year WAY in the future.  We should definitely have flying cars by then.
    assert fyh.calculate_last_completed_fiscal_quarter(2010, date(2001, 3, 1)) is None

    # And one test using the default as_of_date just to make sure nothing catches on fire.
    assert fyh.calculate_last_completed_fiscal_quarter(2000) == 4


def test_convert_fiscal_quarter_to_fiscal_period():
    assert fyh.convert_fiscal_quarter_to_fiscal_period(1) == 3
    assert fyh.convert_fiscal_quarter_to_fiscal_period(2) == 6
    assert fyh.convert_fiscal_quarter_to_fiscal_period(3) == 9
    assert fyh.convert_fiscal_quarter_to_fiscal_period(4) == 12
    assert fyh.convert_fiscal_quarter_to_fiscal_period(None) is None
    assert fyh.convert_fiscal_quarter_to_fiscal_period("1") is None
