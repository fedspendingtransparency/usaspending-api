from datetime import date
from django.db import DEFAULT_DB_ALIAS
from django.db.models import Func, IntegerField
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.awards.models_matviews import (
    MatviewAwardContracts,
    MatviewAwardDirectPayments,
    MatviewAwardGrants,
    MatviewAwardIdvs,
    MatviewAwardLoans,
    MatviewAwardOther,
)

from usaspending_api.awards.v2.lookups.lookups import (
    contract_type_mapping,
    direct_payment_type_mapping,
    grant_type_mapping,
    idv_type_mapping,
    loan_type_mapping,
    other_type_mapping,
)

TYPES_TO_QUOTE_IN_SQL = (str, date)


CATEGORY_TO_MODEL = {
    "contracts": {"model": MatviewAwardContracts, "types": set(contract_type_mapping.keys())},
    "direct_payments": {"model": MatviewAwardDirectPayments, "types": set(direct_payment_type_mapping.keys())},
    "grants": {"model": MatviewAwardGrants, "types": set(grant_type_mapping.keys())},
    "idvs": {"model": MatviewAwardIdvs, "types": set(idv_type_mapping.keys())},
    "loans": {"model": MatviewAwardLoans, "types": set(loan_type_mapping.keys())},
    "other": {"model": MatviewAwardOther, "types": set(other_type_mapping.keys())},
}


class FiscalMonth(Func):
    function = "EXTRACT"
    template = "%(function)s(MONTH from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalQuarter(Func):
    function = "EXTRACT"
    template = "%(function)s(QUARTER from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalYear(Func):
    function = "EXTRACT"
    template = "%(function)s(YEAR from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


def generate_raw_quoted_query(queryset):
    """
    Generates the raw sql from a queryset with quotable types quoted.
    This function provided benefit since the Django queryset.query doesn't quote
        some types such as dates and strings. If Django is updated to fix this,
        please use that instead.

    Note: To add new python data types that should be quoted in queryset.query output,
        add them to TYPES_TO_QUOTE_IN_SQL global
    """
    sql, params = queryset.query.get_compiler(DEFAULT_DB_ALIAS).as_sql()
    str_fix_params = []
    for param in params:
        if isinstance(param, TYPES_TO_QUOTE_IN_SQL):
            # single quotes are escaped with two '' for strings in sql
            param = param.replace("'", "''") if isinstance(param, str) else param
            str_fix_param = "'{}'".format(param)
        elif isinstance(param, list):
            str_fix_param = "ARRAY{}".format(param)
        else:
            str_fix_param = param
        str_fix_params.append(str_fix_param)
    return sql % tuple(str_fix_params)


def generate_where_clause(queryset):
    """
    Returns the SQL and params from a queryset all ready to be plugged into an
    extra method.
    """
    compiler = queryset.query.get_compiler(get_connection().alias)
    return queryset.query.where.as_sql(compiler, compiler.connection)


def obtain_view_from_award_group(type_list):
    types = set(type_list)

    for category, values in CATEGORY_TO_MODEL.items():
        if types <= values["types"]:
            return values["model"]
    else:
        raise Exception("Invalid type list. Types cross categories")


def category_to_award_materialized_views():
    return {k: v["model"] for k, v in CATEGORY_TO_MODEL.items()}
