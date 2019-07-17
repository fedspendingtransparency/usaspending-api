from datetime import date
from django.db import DEFAULT_DB_ALIAS
from django.db.models import Func, IntegerField


TYPES_TO_QUOTE_IN_SQL = (str, date)


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
