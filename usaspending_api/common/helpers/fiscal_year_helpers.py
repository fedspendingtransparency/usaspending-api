import logging
from datetime import MAXYEAR, MINYEAR, datetime
from typing import List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from django.db.models import Max, Q
from fiscalyear import FiscalDate, FiscalDateTime, FiscalYear

from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.generic_helper import min_and_max_from_date_ranges, validate_date
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

logger = logging.getLogger(__name__)


def get_fiscal_year_end_datetime(fiscal_year: int) -> FiscalDateTime:
    """Provides a fiscal year date time end date given a fiscal year

    Args:
        fiscal_year: The fiscal year in which you want to
        obtain a end date for.

    Returns:
        FiscalDateTime: The datetime representing the end of the fiscal year
        provided.
    """
    fiscal_year = FiscalYear(fiscal_year)
    return fiscal_year.end


def get_fiscal_year_start_datetime(fiscal_year: int) -> FiscalDateTime:
    """Provides a fiscal year date time start date given a fiscal year

    Args:
        fiscal_year: The fiscal year in which you want to
        obtain a start date for.

    Returns:
        FiscalDateTime: The datetime representing the start of the fiscal year
        provided.
    """
    fiscal_year = FiscalYear(fiscal_year)
    return fiscal_year.start


def current_fiscal_date() -> FiscalDateTime:
    """FiscalDateTime.today() returns calendar date! Add 3 months to convert to fiscal"""
    return FiscalDateTime.today() + relativedelta(months=3)


def current_fiscal_year() -> int:
    """Return the year from the fiscal date"""
    return current_fiscal_date().year  # Don't use `.fiscal_year` since the datetime is already offset


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
    """Generate fiscal year based on the date provided"""
    validate_date(date)

    year = date.year
    if date.month in [10, 11, 12]:
        year += 1
    return year


def generate_fiscal_month(date):
    """Generate fiscal period based on the date provided"""
    validate_date(date)

    if date.month in [10, 11, 12]:
        return date.month - 9
    return date.month + 3


def generate_fiscal_quarter(date):
    """Generate fiscal quarter based on the date provided"""
    validate_date(date)
    return FiscalDate(date.year, date.month, date.day).fiscal_quarter


def generate_fiscal_year_and_month(date):
    validate_date(date)
    year = generate_fiscal_year(date)
    month = generate_fiscal_month(date)
    return year, month


def generate_fiscal_year_and_quarter(date):
    validate_date(date)
    quarter = FiscalDate(date.year, date.month, date.day).fiscal_quarter
    year = generate_fiscal_year(date)
    return "{}-Q{}".format(year, quarter)


def dates_are_fiscal_year_bookends(start, end):
    """Returns true if the start and end dates fall on fiscal year(s) start and end date"""
    try:
        if start.month == 10 and start.day == 1 and end.month == 9 and end.day == 30 and start.year < end.year:
            return True
    except Exception as e:
        logger.error(str(e))
    return False


def generate_date_range(min_date: datetime, max_date: datetime, frequency: str) -> list:
    """
    Using a min date, max date, and a frequency indicator generates a list of dictionaries that contain
    the fiscal year, fiscal quarter, and fiscal month, or calendar year.
    """
    interval = {"fiscal_year": 12, "quarter": 3, "calendar_year": 12, "month": 1}.get(frequency, 1)
    date_range = []
    current_date = min_date

    while current_date <= max_date:
        if frequency == "calendar_year":
            date_range.append({"calendar_year": current_date.year})
        else:
            date_range.append(
                {
                    "fiscal_year": generate_fiscal_year(current_date),
                    "fiscal_quarter": generate_fiscal_quarter(current_date),
                    "fiscal_month": generate_fiscal_month(current_date),
                }
            )

        current_date += relativedelta(months=interval)

    # Check if max_date is in new period
    final_period = {}

    if frequency == "calendar_year":
        final_period["calendar_year"] = current_date.year
        if (
            frequency == "calendar_year"
            and final_period["calendar_year"] != date_range[-1].get("calendar_year")
            and final_period["calendar_year"] <= max_date.year
        ):
            date_range.append(final_period)
    else:
        final_period = {
            "fiscal_year": generate_fiscal_year(max_date),
            "fiscal_quarter": generate_fiscal_quarter(max_date),
            "fiscal_month": generate_fiscal_month(max_date),
        }
        if final_period["fiscal_year"] > date_range[-1]["fiscal_year"]:
            date_range.append(final_period)
        elif interval == 3 and final_period["fiscal_quarter"] != date_range[-1]["fiscal_quarter"]:
            date_range.append(final_period)
        elif interval == 1 and final_period != date_range[-1]:
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


def clean_subaward_spending_over_time_results(subaward_results: List[dict], date_range_type: str) -> List[dict]:
    """Clean up the spending_over_time Subaward results

    Args:
        subaward_results: Subaward spending_over_time result for a given time period
        date_range_type: how the results are split
            - 'fy', 'quarter', or 'month'

    Returns:
        List of cleaned Subaward results
    """

    for result in subaward_results:
        result["time_period"]["fiscal_year"] = result["time_period"]["fy"]
        result["aggregated_amount"] = result.get("sub-contract", 0) + result.get("sub-grant", 0)

        result["Contract_Obligations"] = result.get("sub-contract", 0)
        result["Grant_Obligations"] = result.get("sub-grant", 0)

        result["total_outlays"] = None
        result["Contract_Outlays"] = None
        result["Grant_Outlays"] = None

        del result["time_period"]["fy"]
        del result["subaward_type"]
        del result["obligation_amount"]

        if "sub-contract" in result.keys():
            del result["sub-contract"]
        if "sub-grant" in result.keys():
            del result["sub-grant"]

    return subaward_results


def bolster_missing_time_periods(filter_time_periods, queryset, date_range_type, columns):
    """Given the following, generate a list of dict results split by fiscal years/quarters/months

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
                if "subaward_type" in row.keys():
                    item[row["subaward_type"]] = row.get("obligation_amount", 0)

                for column_name, column_in_queryset in columns.items():
                    item[column_name] = row[column_in_queryset]

    return results


def calculate_last_completed_fiscal_quarter(fiscal_year):
    """
    Returns either the most recently completed fiscal quarter or None if it's too early in the
    fiscal year for the first quarter to be considered "completed".  Should always return None for
    future fiscal years.
    """

    row = DABSSubmissionWindowSchedule.objects.filter(
        Q(submission_reveal_date__lte=now()) & Q(submission_fiscal_year=fiscal_year) & Q(is_quarter=True)
    ).aggregate(Max("submission_fiscal_quarter"))
    return row["submission_fiscal_quarter__max"]


def is_valid_period(period: int) -> bool:
    """There is no period 1."""
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
    """There is no period 1."""
    return {1: (2, 3), 2: (4, 5, 6), 3: (7, 8, 9), 4: (10, 11, 12)}[quarter] if is_valid_quarter(quarter) else None


def get_quarter_from_period(period: int) -> Optional[int]:
    return (period + 2) // 3 if is_valid_period(period) else None
