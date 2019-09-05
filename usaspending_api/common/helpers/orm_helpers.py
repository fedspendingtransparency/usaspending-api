from datetime import date
from django.db import DEFAULT_DB_ALIAS
from django.db.models import Aggregate, Func, IntegerField
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.awards.models_matviews import (
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


class BoolOr(Aggregate):
    """true if at least one input value is true, otherwise false"""
    function = "BOOL_OR"
    name = "BoolOr"


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


def generate_where_clause(queryset):
    """Returns the SQL and params from a queryset all ready to be plugged into an extra method."""
    compiler = queryset.query.get_compiler(get_connection().alias)
    return queryset.query.where.as_sql(compiler, compiler.connection)


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


def category_to_award_materialized_views():
    return {category: values["model"] for category, values in CATEGORY_TO_MODEL.items()}
