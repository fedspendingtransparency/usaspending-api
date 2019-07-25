import datetime

from django.db.models import Q
from psycopg2.sql import Identifier, Literal, SQL
from usaspending_api.accounts.models import TASAwardMatview
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string


TAS_COMPONENT_TO_FIELD_MAPPING = {
    "ata": "allocation_transfer_agency_id",
    "aid": "agency_id",
    "bpoa": "beginning_period_of_availability",
    "epoa": "ending_period_of_availability",
    "a": "availability_type_code",
    "main": "main_account_code",
    "sub": "sub_account_code",
}


def start_and_end_dates_from_fyq(fiscal_year, fiscal_quarter):
    if fiscal_quarter == 1:
        start_date = datetime.date(fiscal_year - 1, 10, 1)
        end_date = datetime.date(fiscal_year - 1, 12, 31)
    elif fiscal_quarter == 2:
        start_date = datetime.date(fiscal_year, 1, 1)
        end_date = datetime.date(fiscal_year, 3, 31)
    elif fiscal_quarter == 3:
        start_date = datetime.date(fiscal_year, 4, 1)
        end_date = datetime.date(fiscal_year, 6, 30)
    else:
        start_date = datetime.date(fiscal_year, 7, 1)
        end_date = datetime.date(fiscal_year, 9, 30)

    return start_date, end_date


def build_tas_codes_filter(queryset, model, value):
    """
    Build the TAS codes filter.  Because of performance issues, the normal
    trick of checking for award_id in a subquery wasn't cutting it.  To
    work around this, we're going to use the query.extra function to add SQL
    that should give us a better query plan.  This will create old school SQL
    because the extra method doesn't seem to have a way to add joins.
    """
    # Build the filtering for the tas_award_matview.
    or_queryset = Q()
    for tas in value:
        or_queryset |= Q(**{TAS_COMPONENT_TO_FIELD_MAPPING[k]: v for k, v in tas.items()})

    if or_queryset:
        """
        Now that we've built the actual filter, let's turn it into SQL that we 
        can provide to the queryset.extra method.  We do this by converting the
        queryset to raw SQL and extracting the WHERE clause.
        """
        _where = generate_raw_quoted_query(TASAwardMatview.objects.filter(or_queryset).values("award_id"))
        _where = _where[_where.find('WHERE') + 6:]
        return queryset.extra(
            tables=[TASAwardMatview._meta.db_table],
            where=[TASAwardMatview._meta.db_table + ".award_id = " + model._meta.db_table + ".award_id", _where],
        )

    return queryset
