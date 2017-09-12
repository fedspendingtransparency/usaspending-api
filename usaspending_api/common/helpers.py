import datetime
import logging
import time

from django.utils.dateparse import parse_date
from usaspending_api.references.models import Agency

logger = logging.getLogger(__name__)


def check_valid_toptier_agency(agency_id):
    """ Check if the ID provided (corresponding to Agency.id) is a valid toptier agency """

    agency = Agency.objects.filter(id=agency_id, toptier_flag=True).first()
    return agency is not None


def get_params_from_req_or_request(request=None, req=None):
    """
    Uses requests and req to return a single param dictionary combining query params and data
    Prefers the data from req, if available
    """
    params = {}
    if request:
        params = dict(request.query_params)
        params.update(dict(request.data))

    if req:
        params = req.request["query_params"]
        params.update(req.request["data"])

    return params


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


def get_pagination(results, limit, page, benchmarks=False):
    if benchmarks:
        start_pagination = time.time()
    page_metadata = {"page": page, "count": len(results), "next": None, "previous": None, "hasNext": False,
                     "hasPrevious": False}
    if limit < 1 or page < 1:
        return [], page_metadata
    page_metadata["hasNext"] = (limit*page < len(results))
    page_metadata["hasPrevious"] = (page > 1 and limit*(page-2) < len(results))
    if not page_metadata["hasNext"]:
        paginated_results = results[limit*(page-1):]
    else:
        paginated_results = results[limit*(page-1):limit*page]
    page_metadata["next"] = page+1 if page_metadata["hasNext"] else None
    page_metadata["previous"] = page-1 if page_metadata["hasPrevious"] else None
    if benchmarks:
        logger.info("get_pagination took {} seconds".format(time.time() - start_pagination))
    return paginated_results, page_metadata


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
            -- Defined in usaspending_api/common/helpers.py
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
            -- Defined in usaspending_api/common/helpers.py
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
            -- Defined in usaspending_api/common/helpers.py
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
