from datetime import date, MINYEAR, MAXYEAR

from fiscalyear import FiscalDate

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


def test_is_valid_period():
    assert fyh.is_valid_period(2) is True
    assert fyh.is_valid_period(3) is True
    assert fyh.is_valid_period(4) is True
    assert fyh.is_valid_period(5) is True
    assert fyh.is_valid_period(6) is True
    assert fyh.is_valid_period(7) is True
    assert fyh.is_valid_period(8) is True
    assert fyh.is_valid_period(9) is True
    assert fyh.is_valid_period(10) is True
    assert fyh.is_valid_period(11) is True
    assert fyh.is_valid_period(12) is True

    assert fyh.is_valid_period(1) is False
    assert fyh.is_valid_period(13) is False
    assert fyh.is_valid_period(None) is False
    assert fyh.is_valid_period("1") is False
    assert fyh.is_valid_period("a") is False
    assert fyh.is_valid_period({"hello": "there"}) is False


def test_is_valid_quarter():
    assert fyh.is_valid_quarter(1) is True
    assert fyh.is_valid_quarter(2) is True
    assert fyh.is_valid_quarter(3) is True
    assert fyh.is_valid_quarter(4) is True

    assert fyh.is_valid_quarter(0) is False
    assert fyh.is_valid_quarter(5) is False
    assert fyh.is_valid_quarter(None) is False
    assert fyh.is_valid_quarter("1") is False
    assert fyh.is_valid_quarter("a") is False
    assert fyh.is_valid_quarter({"hello": "there"}) is False


def test_is_valid_year():
    assert fyh.is_valid_year(MINYEAR) is True
    assert fyh.is_valid_year(MINYEAR + 1) is True
    assert fyh.is_valid_year(MAXYEAR) is True
    assert fyh.is_valid_year(MAXYEAR - 1) is True
    assert fyh.is_valid_year(1999) is True

    assert fyh.is_valid_year(MINYEAR - 1) is False
    assert fyh.is_valid_year(MAXYEAR + 1) is False
    assert fyh.is_valid_year(None) is False
    assert fyh.is_valid_year("1") is False
    assert fyh.is_valid_year("a") is False
    assert fyh.is_valid_year({"hello": "there"}) is False


def test_is_final_period_of_quarter():
    assert fyh.is_final_period_of_quarter(3, 1) is True
    assert fyh.is_final_period_of_quarter(6, 2) is True
    assert fyh.is_final_period_of_quarter(9, 3) is True
    assert fyh.is_final_period_of_quarter(12, 4) is True

    assert fyh.is_final_period_of_quarter(1, 1) is False
    assert fyh.is_final_period_of_quarter(2, 1) is False
    assert fyh.is_final_period_of_quarter(11, 4) is False
    assert fyh.is_final_period_of_quarter(0, 1) is False
    assert fyh.is_final_period_of_quarter(3, 0) is False
    assert fyh.is_final_period_of_quarter(None, 1) is False
    assert fyh.is_final_period_of_quarter(3, None) is False
    assert fyh.is_final_period_of_quarter("a", "b") is False
    assert fyh.is_final_period_of_quarter([1], {2: 3}) is False


def test_get_final_period_of_quarter():
    assert fyh.get_final_period_of_quarter(1) == 3
    assert fyh.get_final_period_of_quarter(2) == 6
    assert fyh.get_final_period_of_quarter(3) == 9
    assert fyh.get_final_period_of_quarter(4) == 12

    assert fyh.get_final_period_of_quarter(None) is None
    assert fyh.get_final_period_of_quarter("1") is None
    assert fyh.get_final_period_of_quarter("a") is None
    assert fyh.get_final_period_of_quarter({"hello": "there"}) is None


def test_get_periods_in_quarter():
    assert fyh.get_periods_in_quarter(1) == (2, 3)
    assert fyh.get_periods_in_quarter(2) == (4, 5, 6)
    assert fyh.get_periods_in_quarter(3) == (7, 8, 9)
    assert fyh.get_periods_in_quarter(4) == (10, 11, 12)

    assert fyh.get_periods_in_quarter(None) is None
    assert fyh.get_periods_in_quarter("1") is None
    assert fyh.get_periods_in_quarter("a") is None
    assert fyh.get_periods_in_quarter({"hello": "there"}) is None


def test_get_quarter_from_period():
    assert fyh.get_quarter_from_period(2) == 1
    assert fyh.get_quarter_from_period(3) == 1
    assert fyh.get_quarter_from_period(4) == 2
    assert fyh.get_quarter_from_period(5) == 2
    assert fyh.get_quarter_from_period(6) == 2
    assert fyh.get_quarter_from_period(7) == 3
    assert fyh.get_quarter_from_period(8) == 3
    assert fyh.get_quarter_from_period(9) == 3
    assert fyh.get_quarter_from_period(10) == 4
    assert fyh.get_quarter_from_period(11) == 4
    assert fyh.get_quarter_from_period(12) == 4

    assert fyh.get_quarter_from_period(1) is None
    assert fyh.get_quarter_from_period(13) is None
    assert fyh.get_quarter_from_period(None) is None
    assert fyh.get_quarter_from_period("1") is None
    assert fyh.get_quarter_from_period("a") is None
    assert fyh.get_quarter_from_period({"hello": "there"}) is None


def test_generate_date_range():
    # 2-day range that crosses all boundaries
    start = date(2020, 9, 30)
    end = date(2020, 10, 1)
    expected = [
        {"fiscal_year": 2020, "fiscal_quarter": 4, "fiscal_month": 12},
        {"fiscal_year": 2021, "fiscal_quarter": 1, "fiscal_month": 1},
    ]
    assert fyh.generate_date_range(start, end, "fiscal_year") == expected
    assert fyh.generate_date_range(start, end, "quarter") == expected
    assert fyh.generate_date_range(start, end, "anything") == expected

    # check within FY
    start = date(2019, 10, 2)
    end = date(2020, 9, 30)
    expected = [
        {"fiscal_year": 2020, "fiscal_quarter": 1, "fiscal_month": 1},
    ]
    assert fyh.generate_date_range(start, end, "fiscal_year") == expected

    expected.append({"fiscal_year": 2020, "fiscal_quarter": 2, "fiscal_month": 4})
    expected.append({"fiscal_year": 2020, "fiscal_quarter": 3, "fiscal_month": 7})
    expected.append({"fiscal_year": 2020, "fiscal_quarter": 4, "fiscal_month": 10})
    assert fyh.generate_date_range(start, end, "quarter") == expected

    # 1-day period
    start = end = date(2021, 6, 23)
    expected = [{"fiscal_year": 2021, "fiscal_quarter": 3, "fiscal_month": 9}]
    assert fyh.generate_date_range(start, end, "fiscal_year") == expected
    assert fyh.generate_date_range(start, end, "quarter") == expected
    assert fyh.generate_date_range(start, end, "anything") == expected


def test_create_full_time_periods():
    # NOTE: not checking aggregations, only the time periods
    # 2-day range that crosses all boundaries
    start = date(2020, 9, 30)
    end = date(2020, 10, 1)

    years = fyh.create_full_time_periods(start, end, "fy", {})
    assert len(years) == 2
    assert years[0]["time_period"] == {"fy": "2020"}
    assert years[1]["time_period"] == {"fy": "2021"}

    quarters = fyh.create_full_time_periods(start, end, "quarter", {})
    assert len(quarters) == 2
    assert quarters[0]["time_period"] == {"fy": "2020", "quarter": "4"}
    assert quarters[1]["time_period"] == {"fy": "2021", "quarter": "1"}

    months = fyh.create_full_time_periods(start, end, "month", {})
    assert len(months) == 2
    assert months[0]["time_period"] == {"fy": "2020", "month": "12"}
    assert months[1]["time_period"] == {"fy": "2021", "month": "1"}
