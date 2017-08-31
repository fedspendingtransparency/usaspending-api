import datetime

from django.utils.dateparse import parse_date
from usaspending_api.references.models import Agency


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
