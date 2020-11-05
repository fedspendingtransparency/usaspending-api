import logging

from datetime import datetime, MAXYEAR, MINYEAR, timedelta
from dateutil.relativedelta import relativedelta
from fiscalyear import FiscalDate, FiscalDateTime
from typing import Optional, Tuple
from usaspending_api.common.helpers.generic_helper import validate_date, min_and_max_from_date_ranges


logger = logging.getLogger(__name__)


SUBMISSION_WINDOW_DAYS = 45


def current_fiscal_date() -> FiscalDateTime:
    return FiscalDateTime.today()


def current_fiscal_year() -> int:
    return current_fiscal_date().year


def create_fiscal_year_list(start_year=2000, end_year=None):
    """
    return the list of fiscal year as integers
        start_year: int default 2000 FY to start at (inclusive)
        end_year: int default None: FY to end at (exclusive)
            if no end_date is provided, use the current FY
    """
    if end_year is None:
        # to return the current FY, we add 1 here for the range generator below
        end_year = FiscalDate.today().next_fiscal_year.fiscal_year

    if start_year is None or start_year >= end_year:
        raise Exception("Invalid start_year and end_year values")

    return [year for year in range(start_year, end_year)]


def generate_fiscal_year(date):
    """ Generate fiscal year based on the date provided """
    validate_date(date)

    year = date.year
    if date.month in [10, 11, 12]:
        year += 1
    return year


def generate_fiscal_month(date):
    """ Generate fiscal period based on the date provided """
    validate_date(date)

    if date.month in [10, 11, 12]:
        return date.month - 9
    return date.month + 3


def generate_fiscal_quarter(date):
    """ Generate fiscal quarter based on the date provided """
    validate_date(date)
    return FiscalDate(date.year, date.month, date.day).quarter


def generate_fiscal_year_and_month(date):
    validate_date(date)
    year = generate_fiscal_year(date)
    month = generate_fiscal_month(date)
    return year, month


def generate_fiscal_year_and_quarter(date):
    validate_date(date)
    quarter = FiscalDate(date.year, date.month, date.day).quarter
    year = generate_fiscal_year(date)
    return "{}-Q{}".format(year, quarter)


def dates_are_fiscal_year_bookends(start, end):
    """ Returns true if the start and end dates fall on fiscal year(s) start and end date """
    try:
        if start.month == 10 and start.day == 1 and end.month == 9 and end.day == 30 and start.year < end.year:
            return True
    except Exception as e:
        logger.error(str(e))
    return False


def generate_fiscal_date_range(min_date: datetime, max_date: datetime, frequency: str) -> list:
    """
    Using a min date, max date, and a frequency indicator generates a list of dictionaries that contain
    the fiscal year, fiscal quarter, and fiscal month.
    """
    if frequency == "fiscal_year":
        interval = 12
    elif frequency == "quarter":
        interval = 3
    else:  # month
        interval = 1

    date_range = []
    current_date = min_date
    while current_date <= max_date:
        date_range.append(
            {
                "fiscal_year": generate_fiscal_year(current_date),
                "fiscal_quarter": generate_fiscal_quarter(current_date),
                "fiscal_month": generate_fiscal_month(current_date),
            }
        )
        current_date = current_date + relativedelta(months=interval)

    # check if max_date is also in new period
    if current_date > max_date:
        final_period = {
            "fiscal_year": generate_fiscal_year(max_date),
            "fiscal_quarter": generate_fiscal_quarter(max_date),
            "fiscal_month": generate_fiscal_month(max_date),
        }
        if final_period != date_range[-1]:
            date_range.append(final_period)

    return date_range


def create_full_time_periods(min_date, max_date, group, columns):
    cols = {col: 0 for col in columns.keys()}
    fiscal_years = [int(fy) for fy in range(generate_fiscal_year(min_date), generate_fiscal_year(max_date) + 1)]
    if group == "fy":
        return [{**cols, **{"time_period": {"fy": str(fy)}}} for fy in fiscal_years]

    if group == "month":
        period = generate_fiscal_month(min_date)
        ending = generate_fiscal_month(max_date)
        rollover = 12
    else:  # quarter
        period = generate_fiscal_quarter(min_date)
        ending = generate_fiscal_quarter(max_date)
        rollover = 4

    results = []
    for fy in fiscal_years:
        while period <= rollover and not (period > ending and fy == fiscal_years[-1]):
            results.append({**cols, **{"time_period": {"fy": str(fy), group: str(period)}}})
            period += 1
        period = 1

    return results


def bolster_missing_time_periods(filter_time_periods, queryset, date_range_type, columns):
    """ Given the following, generate a list of dict results split by fiscal years/quarters/months

        Args:
            filter_time_periods: list of time_period objects usually provided by filters
                - {'start_date':..., 'end_date':...}
            queryset: the resulting data to split into these results
            date_range_type: how the results are split
                - 'fy', 'quarter', or 'month'
            columns: dictionary of columns to include from the queryset
                - {'name of field to be included in the resulting dict': 'column to be pulled from the queryset'}
        Returns:
            list of dict results split by fiscal years/quarters/months
    """
    min_date, max_date = min_and_max_from_date_ranges(filter_time_periods)
    results = create_full_time_periods(min_date, max_date, date_range_type, columns)

    for row in queryset:
        for item in results:
            same_year = str(item["time_period"]["fy"]) == str(row["fy"])
            same_period = str(item["time_period"][date_range_type]) == str(row[date_range_type])
            if same_year and same_period:
                for column_name, column_in_queryset in columns.items():
                    item[column_name] = row[column_in_queryset]

    for result in results:
        result["time_period"]["fiscal_year"] = result["time_period"]["fy"]
        del result["time_period"]["fy"]
    return results


def calculate_last_completed_fiscal_quarter(fiscal_year, as_of_date=current_fiscal_date()):
    """
    ENABLE_CARES_ACT_FEATURES TECH DEBT:  Make this work with new standardized, yet-to-be-named
    function.  There are currently several flavors flying around.  Waiting for one to land rather
    than creating another.

    Returns either the most recently completed fiscal quarter or None if it's too early in the
    fiscal year for the first quarter to be considered "completed".  Should always return None for
    future fiscal years.  as_of_date was intended for unit testing purposes, but who knows, maybe
    you'll find another use for it being the unquestionable genius that you are.
    """

    # Get the current fiscal year so that it can be compared against the FY in the request.
    day_difference = as_of_date - timedelta(days=SUBMISSION_WINDOW_DAYS)
    current_fiscal_date_adjusted = FiscalDateTime(day_difference.year, day_difference.month, day_difference.day)

    if fiscal_year == current_fiscal_date_adjusted.fiscal_year:
        current_fiscal_quarter = current_fiscal_date_adjusted.quarter
        # If it's currently the first quarter (or within SUBMISSION_WINDOW_DAYS days of the
        # first quarter), return None because it's too soon for there to be a completed quarter
        # for fiscal_year.
        if current_fiscal_quarter == 1:
            fiscal_quarter = None
        else:
            fiscal_quarter = current_fiscal_quarter - 1
    elif fiscal_year < current_fiscal_date_adjusted.fiscal_year:
        fiscal_quarter = 4
    else:
        # The future cannot have completed quarters unless you're into shady accounting.
        fiscal_quarter = None

    return fiscal_quarter


def is_valid_period(period: int) -> bool:
    """ There is no period 1. """
    return isinstance(period, int) and 2 <= period <= 12


def is_valid_quarter(quarter: int) -> bool:
    return isinstance(quarter, int) and 1 <= quarter <= 4


def is_valid_year(year: int) -> bool:
    return isinstance(year, int) and MINYEAR <= year <= MAXYEAR


def is_final_period_of_quarter(period: int, quarter: int) -> bool:
    return is_valid_period(period) and is_valid_quarter(quarter) and period == get_final_period_of_quarter(quarter)


def is_final_quarter(quarter: int) -> bool:
    return quarter == 4


def is_final_period(period: int) -> bool:
    return period == 12


def get_final_period_of_quarter(quarter: int) -> Optional[int]:
    return get_periods_in_quarter(quarter)[-1] if is_valid_quarter(quarter) else None


def get_periods_in_quarter(quarter: int) -> Optional[Tuple[int]]:
    """ There is no period 1. """
    return {1: (2, 3), 2: (4, 5, 6), 3: (7, 8, 9), 4: (10, 11, 12)}[quarter] if is_valid_quarter(quarter) else None


def get_quarter_from_period(period: int) -> Optional[int]:
    return (period + 2) // 3 if is_valid_period(period) else None
