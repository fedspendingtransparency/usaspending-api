from datetime import date
from functools import reduce
from operator import add
from typing import List, Union

from django.contrib.postgres.aggregates import StringAgg
from django.db import DEFAULT_DB_ALIAS
from django.db.models import Aggregate, Case, CharField, F, Func, IntegerField, QuerySet, TextField, Value, When
from django.db.models.functions import Cast, Coalesce, Concat, LPad

from usaspending_api.awards.v2.lookups.lookups import (
    assistance_type_mapping,
    procurement_type_mapping,
    all_award_types_mappings,
)


TYPES_TO_QUOTE_IN_SQL = (str, date)


class AwardGroupsException(Exception):
    """Custom Exception for a specific event"""


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


class FiscalYearAndQuarter(Func):
    """Generates a fiscal year and quarter string along the lines of FY2019Q1."""

    function = "EXTRACT"
    template = (
        "CONCAT('FY', %(function)s(YEAR from (%(expressions)s) + INTERVAL '3 months'), "
        "'Q', %(function)s(QUARTER from (%(expressions)s) + INTERVAL '3 months'))"
    )
    output_field = CharField()


class CFDAs(Func):
    """Generates the CFDAs string from the text array of JSON strings of cfdas."""

    function = "array_to_string"
    template = (
        "%(function)s(ARRAY("
        "SELECT CONCAT(unnest_cfdas::JSON->>'cfda_number', ': ', unnest_cfdas::JSON->>'cfda_program_title')"
        " FROM unnest(%(expressions)s) AS unnest_cfdas), '; ')"
    )
    output_field = TextField()


class BoolOr(Aggregate):
    """true if at least one input value is true, otherwise false"""

    function = "BOOL_OR"
    name = "BoolOr"


class ConcatAll(Func):
    """
    Postgres only!  By default, Django's Concat function breaks concatenations down into pairs
    which, for large concatenations, can add significant time to queries.  This flavor stuffs
    all of the fields into a single CONCAT statement.
    """

    function = "CONCAT"
    output_field = TextField()


class StringAggWithDefault(StringAgg):
    """
    In Django 3.2 a change was made that now requires the output_field defined in Aggregations for mixed types.
    From the release notes:
        Value() expression now automatically resolves its output_field to the appropriate Field subclass based
        on the type of its provided value for bool, bytes, float, int, str, datetime.date, datetime.datetime,
        datetime.time, datetime.timedelta, decimal.Decimal, and uuid.UUID instances. As a consequence, resolving
        an output_field for database functions and combined expressions may now crash with mixed types when
        using Value(). You will need to explicitly set the output_field in such cases.
    In most cases we can simply add the output_field to aggregations to resolve this, however, for
    django.contrib.postgres.aggregates.StringAgg this is not an option and there is no output_field set.
    This was fixed in Django's development branch, but has not been released to any version yet:
        https://github.com/django/django/pull/14898/files#diff-7a96646614a5df088584a163e17143464f836555ec84808a2f24b587b86284dbR93

    As a result, this method sets the output_field as a workaround until either Django 3.2 is patched with the linked
    change, or we upgrade to a version which has the change.
    TODO: Remove this function and revert usages back to normal StringAgg when the above change is available
    """

    output_field = TextField()


def get_fyp_notation(relation_name=None):
    """
    Generates FYyyyyPpp syntax from submission table.  relation_name is the Django ORM
    relation name from the foreign key table to the submission table.
    """
    prefix = f"{relation_name}__" if relation_name else ""
    return Concat(
        Value("FY"),
        Cast(f"{prefix}reporting_fiscal_year", output_field=CharField()),
        Value("P"),
        LPad(Cast(f"{prefix}reporting_fiscal_period", output_field=CharField()), 2, Value("0")),
    )


def get_fyq_notation(relation_name=None):
    """
    Generates FYyyyyPpp syntax from submission table.  relation_name is the Django ORM
    relation name from the foreign key table to the submission table.
    """
    prefix = f"{relation_name}__" if relation_name else ""
    return Concat(
        Value("FY"),
        Cast(f"{prefix}reporting_fiscal_year", output_field=CharField()),
        Value("Q"),
        Cast(f"{prefix}reporting_fiscal_quarter", output_field=CharField()),
    )


def get_fyp_or_q_notation(relation_name=None):
    """
    Generates FYyyyyQq or FYyyyyPpp syntax from submission table.  relation_name is the Django ORM
    relation name from the foreign key table to the submission table.
    """
    prefix = f"{relation_name}__" if relation_name else ""
    return Case(
        When(**{f"{prefix}quarter_format_flag": True}, then=get_fyq_notation(relation_name)),
        default=get_fyp_notation(relation_name),
        output_field=TextField(),
    )


def get_gtas_fyp_notation():
    """
    Generates FYyyyyPpp syntax from gtas_sf133_balances table.
    """
    return Concat(
        Value("FY"),
        Cast("fiscal_year", output_field=CharField()),
        Value("P"),
        LPad(Cast("fiscal_period", output_field=CharField()), 2, Value("0")),
    )


def generate_raw_quoted_query(queryset: QuerySet) -> str:
    """Generates the raw sql from a queryset with quotable types quoted.

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


def obtain_category_from_award_group(type_list):
    if not type_list:
        raise AwardGroupsException("Invalid award type list: No types provided.")

    type_set = set(type_list)
    for category, category_types in all_award_types_mappings.items():
        if type_set <= set(category_types):
            return category
    else:
        raise AwardGroupsException("Invalid award type list: Types cross multiple categories.")


def award_types_are_valid_groups(type_list: list) -> bool:
    """Check to ensure the award type list is a subset of one and only one award group.

    Groups: are Contracts, Loans, Grants, etc.
    If false, the award type codes aren't a subset of any category.
    """
    is_valid = False
    try:
        obtain_category_from_award_group(type_list)
        is_valid = True
    except AwardGroupsException:
        # If this error was thrown, it means is_valid is still False.
        pass

    return is_valid


def subaward_types_are_valid_groups(type_list):
    """Check to ensure the award type list is a subset of one and only one award group.

    Groups: are "Procurement" and "Assistance"
    If false, the award type codes aren't a subset of either category.
    """
    is_procurement = set(type_list).difference(set(procurement_type_mapping.keys()))
    is_assistance = set(type_list).difference(set(assistance_type_mapping.keys()))
    return bool(is_procurement) != bool(is_assistance)  # clever XOR logic


def sum_column_list(column_list: List[Union[str, Coalesce]]) -> F:
    """Create a single expression to sum a dynamic list of columns

    While Django has a Sum() method that can be used to sum values in an aggregation vertically, it does not have
    any direct support for a sum of values horizontally across a row. This method combines a list of columns into
    a single F() object such that ["col_a", "col_b"] becomes F("col_a") + F("col_b") and is usable in a Django QuerySet.

    Additionally, there is special handling if the column is coalesced since the Coalesce() function can be
    directly combined with the created F() objects.
    """
    ignore_types = [Coalesce]
    return reduce(add, (col if type(col) in ignore_types else F(col) for col in column_list))
