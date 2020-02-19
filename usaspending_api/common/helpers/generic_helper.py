import logging
import subprocess
import re
import time

from calendar import monthrange, isleap
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.db import connection
from fiscalyear import FiscalDateTime, FiscalQuarter, datetime, FiscalDate
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.matview_manager import (
    OVERLAY_VIEWS,
    DEPENDENCY_FILEPATH,
    MATERIALIZED_VIEWS,
    MATVIEW_GENERATOR_FILE,
    DEFAULT_MATIVEW_DIR,
)
from usaspending_api.references.models import Agency


logger = logging.getLogger(__name__)
TEMP_SQL_FILES = [DEFAULT_MATIVEW_DIR / val["sql_filename"] for val in MATERIALIZED_VIEWS.values()]


def read_text_file(filepath):
    with open(filepath, "r") as plaintext_file:
        file_content_str = plaintext_file.read()
    return file_content_str


def validate_date(date):
    if not isinstance(date, (datetime.datetime, datetime.date)):
        raise TypeError("Incorrect parameter type provided")

    if not (date.day or date.month or date.year):
        raise Exception("Malformed date object provided")


def check_valid_toptier_agency(agency_id):
    """ Check if the ID provided (corresponding to Agency.id) is a valid toptier agency """
    agency = Agency.objects.filter(id=agency_id, toptier_flag=True).first()
    return agency is not None


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


def generate_fiscal_year_and_quarter(date):
    validate_date(date)
    quarter = FiscalDate(date.year, date.month, date.day).quarter
    year = generate_fiscal_year(date)
    return "{}-Q{}".format(year, quarter)


# aliasing the generate_fiscal_month function to the more appropriate function name
generate_fiscal_period = generate_fiscal_month


def generate_date_from_string(date_str):
    """ Expects a string with format YYYY-MM-DD. returns datetime.date """
    try:
        return datetime.date(*[int(x) for x in date_str.split("-")])
    except Exception as e:
        logger.error(str(e))
    return None


def dates_are_fiscal_year_bookends(start, end):
    """ Returns true if the start and end dates fall on fiscal year(s) start and end date """
    try:
        if start.month == 10 and start.day == 1 and end.month == 9 and end.day == 30 and start.year < end.year:
            return True
    except Exception as e:
        logger.error(str(e))
    return False


def dates_are_month_bookends(start, end):
    try:
        last_day_of_month = monthrange(end.year, end.month)[1]
        if start.day == 1 and end.day == last_day_of_month:
            return True
    except Exception as e:
        logger.error(str(e))
    return False


def min_and_max_from_date_ranges(filter_time_periods: list) -> tuple:
    min_date = min([t.get("start_date", settings.API_MAX_DATE) for t in filter_time_periods])
    max_date = max([t.get("end_date", settings.API_SEARCH_MIN_DATE) for t in filter_time_periods])
    return dt.strptime(min_date, "%Y-%m-%d"), dt.strptime(max_date, "%Y-%m-%d")


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


def generate_fiscal_date_range(min_date: datetime, max_date: datetime, frequency: str) -> list:
    """
    Using a min date, max date, and a frequency indicator generates a list of dictionaries that contain
    the fiscal_year, quarter, and month.
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

    return date_range


def within_one_year(d1, d2):
    """ includes leap years """
    year_range = list(range(d1.year, d2.year + 1))
    if len(year_range) > 2:
        return False
    days_diff = abs((d2 - d1).days)
    for leap_year in [year for year in year_range if isleap(year)]:
        leap_date = datetime.datetime(leap_year, 2, 29)
        if d1 <= leap_date <= d2:
            days_diff -= 1
    return days_diff <= 365


EXTRACT_MATVIEW_SQL = re.compile(r"^.*?CREATE MATERIALIZED VIEW (.*?)_temp\b(.*?) (?:NO )?WITH DATA;.*?$", re.DOTALL)
REPLACE_VIEW_SQL = r"CREATE OR REPLACE VIEW \1\2;"


def convert_matview_to_view(matview_sql):
    sql = EXTRACT_MATVIEW_SQL.sub(REPLACE_VIEW_SQL, matview_sql)
    if sql == matview_sql:
        raise RuntimeError(
            "Error converting materialized view to traditional view.  Perhaps the structure of matviews has changed?"
        )
    return sql


def generate_matviews(materialized_views_as_traditional_views=False):
    with connection.cursor() as cursor:
        cursor.execute(CREATE_READONLY_SQL)
        cursor.execute(DEPENDENCY_FILEPATH.read_text())
        subprocess.call("python {} --quiet".format(MATVIEW_GENERATOR_FILE), shell=True)
        for matview_sql_file in TEMP_SQL_FILES:
            sql = matview_sql_file.read_text()
            if materialized_views_as_traditional_views:
                sql = convert_matview_to_view(sql)
            cursor.execute(sql)
        for view_sql_file in OVERLAY_VIEWS:
            cursor.execute(view_sql_file.read_text())


def generate_last_completed_fiscal_quarter(fiscal_year, fiscal_quarter=None):
    """ Generate the most recently completed fiscal quarter """

    # Get the current fiscal year so that it can be compared against the FY in the request
    current_fiscal_date = FiscalDateTime.today()
    day_difference = current_fiscal_date - datetime.timedelta(days=45)
    current_fiscal_date_adjusted = FiscalDateTime(day_difference.year, day_difference.month, day_difference.day)

    # Attempting to get data for current fiscal year (minus 45 days)
    if fiscal_year == current_fiscal_date_adjusted.fiscal_year:
        current_fiscal_quarter = current_fiscal_date_adjusted.quarter
        # If a fiscal quarter has been requested
        if fiscal_quarter:
            # If the fiscal quarter requested is not yet completed (or within 45 days of being completed), error out
            if current_fiscal_quarter <= fiscal_quarter:
                raise InvalidParameterException(
                    "Requested fiscal year and quarter must have been completed over 45 "
                    "days prior to the current date."
                )
        # If no fiscal quarter has been requested
        else:
            # If it's currently the first quarter (or within 45 days of the first quarter), throw an error
            if current_fiscal_quarter == 1:
                raise InvalidParameterException(
                    "Cannot obtain data for current fiscal year. At least one quarter must "
                    "be completed for over 45 days."
                )
            # roll back to the last completed fiscal quarter if it's any other quarter
            else:
                fiscal_quarter = current_fiscal_quarter - 1
    # Attempting to get data for any fiscal year before the current one (minus 45 days)
    elif fiscal_year < current_fiscal_date_adjusted.fiscal_year:
        # If no fiscal quarter has been requested, give the fourth quarter of the year requested
        if not fiscal_quarter:
            fiscal_quarter = 4
    else:
        raise InvalidParameterException(
            "Cannot obtain data for future fiscal years or fiscal years that have not been active for over 45 days."
        )

    # get the fiscal date
    fiscal_date = FiscalQuarter(fiscal_year, fiscal_quarter).end
    fiscal_date = datetime.datetime.strftime(fiscal_date, "%Y-%m-%d")

    return fiscal_date, fiscal_quarter


def get_pagination(results, limit, page, benchmarks=False):
    if benchmarks:
        start_pagination = time.time()
    page_metadata = {
        "page": page,
        "count": len(results),
        "next": None,
        "previous": None,
        "hasNext": False,
        "hasPrevious": False,
    }
    if limit < 1 or page < 1:
        return [], page_metadata

    page_metadata["hasNext"] = limit * page < len(results)
    page_metadata["hasPrevious"] = page > 1 and limit * (page - 2) < len(results)

    if not page_metadata["hasNext"]:
        paginated_results = results[limit * (page - 1) :]
    else:
        paginated_results = results[limit * (page - 1) : limit * page]

    page_metadata["next"] = page + 1 if page_metadata["hasNext"] else None
    page_metadata["previous"] = page - 1 if page_metadata["hasPrevious"] else None
    if benchmarks:
        logger.info("get_pagination took {} seconds".format(time.time() - start_pagination))
    return paginated_results, page_metadata


def get_pagination_metadata(total_return_count, limit, page):
    page_metadata = {
        "page": page,
        "total": total_return_count,
        "limit": limit,
        "next": None,
        "previous": None,
        "hasNext": False,
        "hasPrevious": False,
    }
    if limit < 1 or page < 1:
        return page_metadata

    page_metadata["hasNext"] = limit * page < total_return_count
    page_metadata["hasPrevious"] = page > 1 and limit * (page - 2) < total_return_count
    page_metadata["next"] = page + 1 if page_metadata["hasNext"] else None
    page_metadata["previous"] = page - 1 if page_metadata["hasPrevious"] else None
    return page_metadata


def get_simple_pagination_metadata(results_plus_one, limit, page):
    has_next = results_plus_one > limit
    has_previous = page > 1

    page_metadata = {
        "page": page,
        "next": page + 1 if has_next else None,
        "previous": page - 1 if has_previous else None,
        "hasNext": has_next,
        "hasPrevious": has_previous,
    }
    return page_metadata


def get_generic_filters_message(original_filters, allowed_filters):
    retval = [get_time_period_message()]
    if original_filters.difference(allowed_filters):
        retval.append(unused_filters_message(original_filters.difference(allowed_filters)))
    return retval


def get_time_period_message():
    return (
        "For searches, time period start and end dates are currently limited to an earliest date of "
        f"{settings.API_SEARCH_MIN_DATE}.  For data going back to {settings.API_MIN_DATE}, use either the Custom "
        "Award Download feature on the website or one of our download or bulk_download API endpoints as "
        "listed on https://api.usaspending.gov/docs/endpoints."
    )


def unused_filters_message(filters):
    return f"The following filters from the request were not used: {filters}. See https://api.usaspending.gov/docs/endpoints for a list of appropriate filters"


# Raw SQL run during a migration
FY_PG_FUNCTION_DEF = """
    CREATE OR REPLACE FUNCTION fy(raw_date DATE)
    RETURNS integer AS $$
          DECLARE result INTEGER;
          DECLARE month_num INTEGER;
          BEGIN
            month_num := EXTRACT(MONTH from raw_date);
            result := EXTRACT(YEAR FROM raw_date);
            IF month_num > 9
            THEN
              result := result + 1;
            END IF;
            RETURN result;
          END;
        $$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION fy(raw_date TIMESTAMP WITH TIME ZONE)
    RETURNS integer AS $$
          DECLARE result INTEGER;
          DECLARE month_num INTEGER;
          BEGIN
            month_num := EXTRACT(MONTH from raw_date);
            result := EXTRACT(YEAR FROM raw_date);
            IF month_num > 9
            THEN
              result := result + 1;
            END IF;
            RETURN result;
          END;
        $$ LANGUAGE plpgsql;


    CREATE OR REPLACE FUNCTION fy(raw_date TIMESTAMP WITHOUT TIME ZONE)
    RETURNS integer AS $$
          DECLARE result INTEGER;
          DECLARE month_num INTEGER;
          BEGIN
            month_num := EXTRACT(MONTH from raw_date);
            result := EXTRACT(YEAR FROM raw_date);
            IF month_num > 9
            THEN
              result := result + 1;
            END IF;
            RETURN result;
          END;
        $$ LANGUAGE plpgsql;
        """

FY_FROM_TEXT_PG_FUNCTION_DEF = """
    CREATE OR REPLACE FUNCTION fy(raw_date TEXT)
    RETURNS integer AS $$
          BEGIN
            RETURN fy(raw_date::DATE);
          END;
        $$ LANGUAGE plpgsql;
        """
"""
Filtering on `field_name__fy` is present for free on all Date fields.
To add this field to the serializer:

1. Add tests that the field is present and functioning
(see awards/tests/test_awards.py)

2. Add a SerializerMethodField and `def get_field_name__fy`
to the the serializer (see awards/serializers.py)

3. add field_name`__fy` to the model's `get_default_fields`
(see awards/models.py)

Also, query performance will stink unless/until the field is indexed.

CREATE INDEX ON awards(FY(field_name))
"""

CORRECTED_CGAC_PG_FUNCTION_DEF = """
    CREATE FUNCTION corrected_cgac(text) RETURNS text AS $$
    SELECT
        CASE $1
        WHEN '016' THEN '1601' -- DOL
        WHEN '011' THEN '1100' -- EOP
        WHEN '033' THEN '3300' -- SI
        WHEN '352' THEN '7801' -- FCA
        WHEN '537' THEN '9566' -- FHFA
        ELSE $1 END;
    $$ LANGUAGE SQL;"""

REV_CORRECTED_CGAC_PG_FUNCTION_DEF = """
    DROP FUNCTION corrected_cgac(text);
"""

CREATE_READONLY_SQL = """DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly') THEN
CREATE ROLE readonly;
END IF;
END$$;"""


def generate_test_db_connection_string():
    db = connection.cursor().db.settings_dict
    return "postgres://{}:{}@{}:5432/{}".format(db["USER"], db["PASSWORD"], db["HOST"], db["NAME"])
