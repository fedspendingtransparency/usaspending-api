from datetime import date
from django.db import DEFAULT_DB_ALIAS
from django.db.models import Aggregate, Case, CharField, Func, IntegerField, Subquery, Value, When
from django.db.models.functions import Concat, LPad, Cast

from usaspending_api.search.models import (
    ContractAwardSearchMatview,
    DirectPaymentAwardSearchMatview,
    GrantAwardSearchMatview,
    IDVAwardSearchMatview,
    LoanAwardSearchMatview,
    OtherAwardSearchMatview,
)

from usaspending_api.awards.v2.lookups.lookups import (
    assistance_type_mapping,
    contract_type_mapping,
    direct_payment_type_mapping,
    grant_type_mapping,
    idv_type_mapping,
    loan_type_mapping,
    other_type_mapping,
    procurement_type_mapping,
)


TYPES_TO_QUOTE_IN_SQL = (str, date)


CATEGORY_TO_MODEL = {
    "contracts": {"model": ContractAwardSearchMatview, "types": set(contract_type_mapping.keys())},
    "direct_payments": {"model": DirectPaymentAwardSearchMatview, "types": set(direct_payment_type_mapping.keys())},
    "grants": {"model": GrantAwardSearchMatview, "types": set(grant_type_mapping.keys())},
    "idvs": {"model": IDVAwardSearchMatview, "types": set(idv_type_mapping.keys())},
    "loans": {"model": LoanAwardSearchMatview, "types": set(loan_type_mapping.keys())},
    "other": {"model": OtherAwardSearchMatview, "types": set(other_type_mapping.keys())},
}


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
    """ Generates a fiscal year and quarter string along the lines of FY2019Q1. """

    function = "EXTRACT"
    template = (
        "CONCAT('FY', %(function)s(YEAR from (%(expressions)s) + INTERVAL '3 months'), "
        "'Q', %(function)s(QUARTER from (%(expressions)s) + INTERVAL '3 months'))"
    )
    output_field = CharField()


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


class AvoidSubqueryInGroupBy(Subquery):
    """
    Django currently adds Subqueries to the Group By clause by default when an Aggregation or Annotation is
    used. This is not always needed and for those queries where it is not needed it can hurt performance.
    The ability to avoid this was implemented in Django 3.2:
    https://github.com/django/django/commit/fb3f034f1c63160c0ff13c609acd01c18be12f80
    """

    def get_group_by_cols(self):
        return []


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


def generate_raw_quoted_query(queryset):
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


def obtain_view_from_award_group(type_list):
    if not type_list:
        raise AwardGroupsException("Invalid award type list: No types provided.")

    type_set = set(type_list)
    for category, values in CATEGORY_TO_MODEL.items():
        if type_set <= values["types"]:
            return values["model"]
    else:
        raise AwardGroupsException("Invalid award type list: Types cross multiple categories.")


def award_types_are_valid_groups(type_list: list) -> bool:
    """Check to ensure the award type list is a subset of one and only one award group.

    Groups: are Contracts, Loans, Grants, etc.
    If false, the award type codes aren't a subset of any category.
    """
    is_valid = False
    try:
        obtain_view_from_award_group(type_list)
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
