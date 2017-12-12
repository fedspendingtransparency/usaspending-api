import datetime
import logging
import time
from calendar import monthrange

from fiscalyear import *

from django.db import DEFAULT_DB_ALIAS
from django.utils.dateparse import parse_date
from usaspending_api.references.models import Agency
from usaspending_api.common.exceptions import InvalidParameterException

logger = logging.getLogger(__name__)

QUOTABLE_TYPES = (str, datetime.date)


def check_valid_toptier_agency(agency_id):
    """ Check if the ID provided (corresponding to Agency.id) is a valid toptier agency """

    agency = Agency.objects.filter(id=agency_id, toptier_flag=True).first()
    return agency is not None


def generate_fiscal_year(date):
    """ Generate fiscal year based on the date provided """
    year = date.year
    if date.month in [10, 11, 12]:
        year += 1
    return year


def generate_fiscal_period(date):
    """ Generate fiscal period based on the date provided """
    return ((generate_fiscal_month(date) - 1) // 3) + 1


def generate_fiscal_month(date):
    """ Generate fiscal period based on the date provided """
    if date.month in [10, 11, 12, "10", "11", "12"]:
        return date.month - 9
    return date.month + 3


def generate_date_from_string(date_str):
    """ Expects a string with format YYYY-MM-DD. returns datetime.date """
    try:
        return datetime.date(*[int(x) for x in date_str.split('-')])
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


def generate_all_fiscal_years_in_range(start, end):
    """ For a given date-range, provide the inclusive fiscal years """
    fiscal_years = []
    temp_date = start
    while temp_date < end:
        fiscal_years.append(generate_fiscal_year(temp_date))
        temp_date = datetime.date(temp_date.year + 1, temp_date.month, temp_date.day)
    return fiscal_years


def generate_raw_quoted_query(queryset):
    """
    Generates the raw sql from a queryset with quotable types quoted.
    This function exists cause queryset.query doesn't quote some types such as
    dates and strings. If Django is updated to fix this, please use that instead.
    Note: To add types that should be in quotes in queryset.query, add it to
          QUOTABLE_TYPES above
    """
    sql, params = queryset.query.get_compiler(DEFAULT_DB_ALIAS).as_sql()
    str_fix_params = []
    for param in params:
        str_fix_param = '\'{}\''.format(param) if isinstance(param, QUOTABLE_TYPES) else param
        str_fix_params.append(str_fix_param)
    return sql % tuple(str_fix_params)


def generate_last_completed_fiscal_quarter(fiscal_year):
    """ Generate the most recently completed fiscal quarter """

    # Get the current fiscal year so that it can be compared against the FY in the request
    current_fiscal_date = FiscalDateTime.today()
    requested_fiscal_year = FiscalYear(fiscal_year)

    if requested_fiscal_year.fiscal_year == current_fiscal_date.fiscal_year:
        current_fiscal_quarter = current_fiscal_date.quarter
        # If attempting to get data for the current year and we're still in quarter 1, error out because no quarter
        # has yet completed for the current year
        if current_fiscal_quarter == 1:
            raise InvalidParameterException("Cannot obtain data for current fiscal year. At least one quarter must be "
                                            "completed.")
        else:
            # can also do: current_fiscal_date.prev_quarter.quarter
            fiscal_quarter = current_fiscal_quarter - 1
            fiscal_date = FiscalQuarter(fiscal_year, fiscal_quarter).end
    elif requested_fiscal_year.fiscal_year < current_fiscal_date.fiscal_year:
        # If the retrieving a previous FY, get the last quarter data UNLESS its the most recent quarter, then account
        # for data loading.
        if (current_fiscal_date - requested_fiscal_year.end).days <= 45:
            fiscal_quarter = requested_fiscal_year.end.quarter - 1
            fiscal_date = getattr(requested_fiscal_year, 'q' + str(fiscal_quarter)).end
        else:
            fiscal_quarter = requested_fiscal_year.end.quarter
            fiscal_date = requested_fiscal_year.end
    else:

        raise InvalidParameterException("Cannot obtain data for future fiscal years.")

    fiscal_date = datetime.datetime.strftime(fiscal_date, '%Y-%m-%d')

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
        "hasPrevious": False
    }
    if limit < 1 or page < 1:
        return [], page_metadata

    page_metadata["hasNext"] = (limit * page < len(results))
    page_metadata["hasPrevious"] = (page > 1 and limit * (page - 2) < len(results))

    if not page_metadata["hasNext"]:
        paginated_results = results[limit * (page - 1):]
    else:
        paginated_results = results[limit * (page - 1): limit * page]

    page_metadata["next"] = page + 1 if page_metadata["hasNext"] else None
    page_metadata["previous"] = page - 1 if page_metadata["hasPrevious"] else None
    if benchmarks:
        logger.info("get_pagination took {} seconds".format(time.time() - start_pagination))
    return paginated_results, page_metadata


def get_pagination_metadata(total_return_count, limit, page):
    page_metadata = {
        "page": page,
        "count": total_return_count,
        "next": None,
        "previous": None,
        "hasNext": False,
        "hasPrevious": False
    }
    if limit < 1 or page < 1:
        return page_metadata

    page_metadata["hasNext"] = (limit * page < total_return_count)
    page_metadata["hasPrevious"] = (page > 1 and limit * (page - 2) < total_return_count)
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
        "hasPrevious": has_previous
    }
    return page_metadata


def fy(raw_date):
    'Federal fiscal year corresponding to date'

    if raw_date is None:
        return None

    if isinstance(raw_date, str):
        raw_date = parse_date(raw_date)

    try:
        result = raw_date.year
        if raw_date.month > 9:
            result += 1
    except AttributeError:
        raise TypeError('{} needs year and month attributes'.format(raw_date))

    return result


# Raw SQL run during a migration
FY_PG_FUNCTION_DEF = '''
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
        '''

FY_FROM_TEXT_PG_FUNCTION_DEF = '''
    CREATE OR REPLACE FUNCTION fy(raw_date TEXT)
    RETURNS integer AS $$
          BEGIN
            RETURN fy(raw_date::DATE);
          END;
        $$ LANGUAGE plpgsql;
        '''

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
