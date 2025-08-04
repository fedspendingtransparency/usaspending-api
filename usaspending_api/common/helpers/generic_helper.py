import logging
import re
import shutil
import subprocess
import time

from calendar import monthrange, isleap
from datetime import datetime as dt
from pathlib import Path

from dateutil import parser

from django.conf import settings
from django.db import connection
from fiscalyear import datetime
from usaspending_api.common.matview_manager import (
    DEPENDENCY_FILEPATH,
    MATERIALIZED_VIEWS,
    CHUNKED_MATERIALIZED_VIEWS,
    MATVIEW_GENERATOR_FILE,
    DEFAULT_MATIVEW_DIR,
)
from usaspending_api.references.models import Agency
from typing import List


logger = logging.getLogger(__name__)


def read_text_file(filepath):
    with open(filepath, "r") as plaintext_file:
        file_content_str = plaintext_file.read()
    return file_content_str


def convert_string_to_datetime(input: str) -> datetime.datetime:
    """Parse a string into a datetime object"""
    return parser.parse(input)


def convert_string_to_date(input: str) -> datetime.date:
    """Parse a string into a date object"""
    return convert_string_to_datetime(input).date()


def validate_date(date):
    if not isinstance(date, (datetime.datetime, datetime.date)):
        raise TypeError("Incorrect parameter type provided")

    if not (date.day or date.month or date.year):
        raise Exception("Malformed date object provided")


def check_valid_toptier_agency(agency_id):
    """Check if the ID provided (corresponding to Agency.id) is a valid toptier agency"""
    agency = Agency.objects.filter(id=agency_id, toptier_flag=True).first()
    return agency is not None


def generate_date_from_string(date_str):
    """Expects a string with format YYYY-MM-DD. returns datetime.date"""
    try:
        return datetime.date(*[int(x) for x in date_str.split("-")])
    except Exception as e:
        logger.error(str(e))
    return None


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


def within_one_year(d1, d2):
    """includes leap years"""
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


def get_temp_matview_sql_files_dict(matview_dir: Path) -> List[dict]:
    """Get mapping of matviews to SQL file definitions

    Args:
        matview_dir: Directory where matview definition SQL files were built out

    Returns:
        A List of dict objects, one for each matview, keyed by matview name pointing to their SQL file definition

    """
    temp_sql_files = [
        {"name": key, "sql_file": matview_dir / val["sql_filename"]} for key, val in MATERIALIZED_VIEWS.items()
    ]
    temp_sql_files += [
        {"name": key, "sql_file": matview_dir / val["sql_filename"]} for key, val in CHUNKED_MATERIALIZED_VIEWS.items()
    ]
    return temp_sql_files


def generate_matviews(materialized_views_as_traditional_views: bool = False, parallel_worker_id: str = None) -> None:
    """Build out matview definitions as SQL files, then create matviews in the database and materialize them.

    Args:
        materialized_views_as_traditional_views: False if they should be just DB VIEWs; True if they should be real
            MATERIALIZED VIEWs. Defaults to False.
        parallel_worker_id: Under pytest-xdist parallel test sessions, use the worker as a suffix to the temp
            directory in which the sql is built to not cause concurrent File I/O conflicts/race conditions
    """
    with connection.cursor() as cursor:
        cursor.execute(CREATE_READONLY_SQL)
        cursor.execute(DEPENDENCY_FILEPATH.read_text())

        matview_dir = DEFAULT_MATIVEW_DIR
        if parallel_worker_id:
            matview_dir = DEFAULT_MATIVEW_DIR / f"worker_{parallel_worker_id}"
        subprocess.call(f"python3 {MATVIEW_GENERATOR_FILE} --dest {matview_dir} --quiet", shell=True)

        for matview_sql_lookup in get_temp_matview_sql_files_dict(matview_dir):
            name = matview_sql_lookup["name"]
            sql = matview_sql_lookup["sql_file"].read_text()
            if materialized_views_as_traditional_views:
                sql = convert_matview_to_view(sql)
                # Put in place to help resolve issues where records are deleted from TransactionSearch;
                # it is converted to be a View for tests, but is normally a Table which allows DELETEs
                sql += (
                    f" DROP RULE IF EXISTS {name}_delete_rule ON {name};"
                    f" CREATE RULE {name}_delete_rule AS ON DELETE TO {name} DO INSTEAD NOTHING;"
                )
            cursor.execute(sql)
        shutil.rmtree(matview_dir)


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


def get_generic_filters_message(original_filters, allowed_filters) -> List[str]:
    retval = [get_time_period_message()]
    if set(original_filters).difference(allowed_filters):
        retval.append(unused_filters_message(set(original_filters).difference(allowed_filters)))
    return retval


def get_time_period_message():
    return (
        "For searches, time period start and end dates are currently limited to an earliest date of "
        f"{settings.API_SEARCH_MIN_DATE}.  For data going back to {settings.API_MIN_DATE}, use either the Custom "
        "Award Download feature on the website or one of our download or bulk_download API endpoints as "
        "listed on https://api.usaspending.gov/docs/endpoints. "
    )


def unused_filters_message(filters):
    return f"The following filters from the request were not used: {filters}. See https://api.usaspending.gov/docs/endpoints for a list of appropriate filters"


def get_account_data_time_period_message():
    return (
        f"Account data powering this endpoint were first collected in FY2017 Q2 under the DATA Act; "
        f"as such, there are no data available for prior fiscal years."
    )


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
to the serializer (see awards/serializers.py)

3. add field_name`__fy` to the model's `get_default_fields`
(see awards/models.py)

Also, query performance will stink unless/until the field is indexed.

CREATE INDEX ON awards(FY(field_name))
"""

CREATE_READONLY_SQL = """DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly') THEN
CREATE ROLE readonly;
END IF;
END$$;"""


def sort_with_null_last(to_sort, sort_key, sort_order, tie_breaker=None):
    """
    Use tuples to sort results so that None can be converted to a Boolean for comparison
    """
    if tie_breaker is None:
        tie_breaker = sort_key
    return sorted(
        to_sort,
        key=lambda x: ((x[sort_key] is None) == (sort_order == "asc"), x[sort_key], x[tie_breaker]),
        reverse=(sort_order == "desc"),
    )


def deprecated_api_endpoint_message(messages: List[str]):
    """Adds a deprecation message (when needed) indicating that the endpoint is deprecated.
    Args:
        message: The existing message list to add additional messages to
    """
    deprecation_message = f"This endpoint is DEPRECATED. Please refer to the api contracts."
    messages.append(deprecation_message)
