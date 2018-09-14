import contextlib
import logging
import time
import timeit
import subprocess

from calendar import monthrange, isleap
from collections import OrderedDict
from django.db import DEFAULT_DB_ALIAS, connection
from django.utils.dateparse import parse_date
from fiscalyear import FiscalDateTime, FiscalQuarter, datetime, FiscalDate

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Agency

logger = logging.getLogger(__name__)

QUOTABLE_TYPES = (str, datetime.date)

TEMP_SQL_FILES = [
    '../matviews/universal_transaction_matview.sql',
    '../matviews/universal_award_matview.sql',
    '../matviews/summary_transaction_view.sql',
    '../matviews/summary_transaction_month_view.sql',
    '../matviews/summary_transaction_geo_view.sql',
    '../matviews/summary_state_view.sql',
    '../matviews/summary_award_recipient_view.sql',
    '../matviews/summary_award_view.sql',
    '../matviews/summary_view_cfda_number.sql',
    '../matviews/summary_view_naics_codes.sql',
    '../matviews/summary_view_psc_codes.sql',
    '../matviews/summary_view.sql',
    '../matviews/subaward_view.sql',
]
MATVIEW_GENERATOR_FILE = "usaspending_api/database_scripts/matview_generator/matview_sql_generator.py"
ENUM_FILE = ['usaspending_api/database_scripts/matviews/functions_and_enums.sql']


def read_text_file(filepath):
    with open(filepath, "r") as plaintext_file:
        file_content_str = plaintext_file.read()
    return file_content_str


def upper_case_dict_values(input_dict):
    for key in input_dict:
        if isinstance(input_dict[key], str):
            input_dict[key] = input_dict[key].upper()


def validate_date(date):
    if not isinstance(date, (datetime.datetime, datetime.date)):
        raise TypeError('Incorrect parameter type provided')

    if not (date.day or date.month or date.year):
        raise Exception('Malformed date object provided')


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

    if date.month in [10, 11, 12, "10", "11", "12"]:
        return date.month - 9
    return date.month + 3


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


def generate_raw_quoted_query(queryset):
    """
    Generates the raw sql from a queryset with quotable types quoted. This function exists cause queryset.query doesn't
    quote some types such as dates and strings. If Django is updated to fix this, please use that instead.
    Note: To add types that should be in quotes in queryset.query, add it to QUOTABLE_TYPES above
    """
    sql, params = queryset.query.get_compiler(DEFAULT_DB_ALIAS).as_sql()
    str_fix_params = []
    for param in params:
        if isinstance(param, QUOTABLE_TYPES):
            # single quotes are escaped with two '' for strings in sql
            param = param.replace('\'', '\'\'') if isinstance(param, str) else param
            str_fix_param = '\'{}\''.format(param)
        elif isinstance(param, list):
            str_fix_param = 'ARRAY{}'.format(param)
        else:
            str_fix_param = param
        str_fix_params.append(str_fix_param)
    return sql % tuple(str_fix_params)


def order_nested_object(nested_object):
    """
    Simply recursively order the item. To be used for standardizing objects for JSON dumps
    """
    if isinstance(nested_object, list):
        if len(nested_object) > 0 and isinstance(nested_object[0], dict):
            # Lists of dicts aren't handled by python's sorted(), so we handle sorting manually
            sorted_subitems = []
            sort_dict = {}
            # Create a hash using keys & values
            for subitem in nested_object:
                hash_list = ['{}{}'.format(key, subitem[key]) for key in sorted(list(subitem.keys()))]
                hash_str = '_'.join(str(hash_list))
                sort_dict[hash_str] = order_nested_object(subitem)
            # Sort by the new hash
            for sorted_hash in sorted(list(sort_dict.keys())):
                sorted_subitems.append(sort_dict[sorted_hash])
            return sorted_subitems
        else:
            return sorted([order_nested_object(subitem) for subitem in nested_object])
    elif isinstance(nested_object, dict):
        return OrderedDict([(key, order_nested_object(nested_object[key])) for key in sorted(nested_object.keys())])
    else:
        return nested_object


def generate_matviews():
    with connection.cursor() as c:
        c.execute(CREATE_READONLY_SQL)
        c.execute(get_sql(ENUM_FILE)[0])
        subprocess.call("python  " + MATVIEW_GENERATOR_FILE + " --quiet", shell=True)
        for file in get_sql(TEMP_SQL_FILES):
            c.execute(file)


def get_sql(sql_files):
    data = []
    for file in sql_files:
        with open(file, 'r') as myfile:
            data.append(myfile.read())
    return data


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
                raise InvalidParameterException("Requested fiscal year and quarter must have been completed over 45 "
                                                "days prior to the current date.")
        # If no fiscal quarter has been requested
        else:
            # If it's currently the first quarter (or within 45 days of the first quarter), throw an error
            if current_fiscal_quarter == 1:
                raise InvalidParameterException("Cannot obtain data for current fiscal year. At least one quarter must "
                                                "be completed for over 45 days.")
            # roll back to the last completed fiscal quarter if it's any other quarter
            else:
                fiscal_quarter = current_fiscal_quarter - 1
    # Attempting to get data for any fiscal year before the current one (minus 45 days)
    elif fiscal_year < current_fiscal_date_adjusted.fiscal_year:
        # If no fiscal quarter has been requested, give the fourth quarter of the year requested
        if not fiscal_quarter:
            fiscal_quarter = 4
    else:
        raise InvalidParameterException("Cannot obtain data for future fiscal years or fiscal years that have not "
                                        "been active for over 45 days.")

    # get the fiscal date
    fiscal_date = FiscalQuarter(fiscal_year, fiscal_quarter).end
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
        "total": total_return_count,
        "limit": limit,
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
    """Federal fiscal year corresponding to date"""

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


@contextlib.contextmanager
def timer(msg='', logging_func=print):
    """
    Use as a context manager or decorator to report on elapsed time.

    with timer('stuff', logger.info):
        (active code)

    """
    start = timeit.default_timer()
    logging_func('Beginning {}...'.format(msg))
    try:
        yield {}
    finally:
        elapsed = timeit.default_timer() - start
        logging_func('... finished {} in {} sec'.format(msg, elapsed))


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

CORRECTED_CGAC_PG_FUNCTION_DEF = '''
    CREATE FUNCTION corrected_cgac(text) RETURNS text AS $$
    SELECT
        CASE $1
        WHEN '016' THEN '1601' -- DOL
        WHEN '011' THEN '1100' -- EOP
        WHEN '033' THEN '3300' -- SI
        WHEN '352' THEN '7801' -- FCA
        WHEN '537' THEN '9566' -- FHFA
        ELSE $1 END;
    $$ LANGUAGE SQL;'''

REV_CORRECTED_CGAC_PG_FUNCTION_DEF = '''
    DROP FUNCTION corrected_cgac(text);
'''

CREATE_READONLY_SQL = """DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly') THEN
CREATE ROLE readonly;
END IF;
END$$;"""
