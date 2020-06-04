from django.db.models import Sum

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.fiscal_year_helpers import generate_last_completed_fiscal_quarter
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import GTASTotalObligation
from usaspending_api.disaster_spending.v2.filters.query_factory import QueryFactory
from usaspending_api.disaster_spending.v2.filters.spending_filter import spending_filter

UNREPORTED_DATA_NAME = "Unreported Data"
VALID_UNREPORTED_DATA_TYPES = ["agency", "budget_function", "object_class"]
VALID_UNREPORTED_FILTERS = ["fy", "quarter"]


def get_unreported_data_obj(
    queryset, filters, limit, spending_type, actual_total, fiscal_year, fiscal_quarter
) -> (list, float):
    """ Returns the modified list of result objects including the object corresponding to the unreported amount, only
        if applicable. If the unreported amount does not fit within the limit of results provided, it will not be added.

        Args:
            queryset: Django queryset with all necessary filters, etc already applied
            filters: filters provided in POST request to endpoint
            limit: number of results to limit to
            spending_type: spending explorer category
            actual_total: total calculated based on results in `queryset`
            fiscal_year: fiscal year from request
            fiscal_quarter: fiscal quarter from request

        Returns:
            result_set: modified (if applicable) result set as a list
            expected_total: total calculated from GTAS
    """

    queryset = queryset[:limit] if type == "award" else queryset

    result_set = []
    result_keys = ["id", "code", "type", "name", "amount"]
    if spending_type == "federal_account":
        result_keys.append("account_number")
    for entry in queryset:
        condensed_entry = {}
        for key in result_keys:
            condensed_entry[key] = entry[key] if key != "id" else str(entry[key])
        result_set.append(condensed_entry)

    expected_total = (
        GTASTotalObligation.objects.filter(fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter)
        .values_list("total_obligation", flat=True)
        .first()
    )

    if spending_type in VALID_UNREPORTED_DATA_TYPES and set(filters.keys()) == set(VALID_UNREPORTED_FILTERS):
        unreported_obj = {"id": None, "code": None, "type": spending_type, "name": UNREPORTED_DATA_NAME, "amount": None}

        # if both values are actually available, then calculate the amount, otherwise leave it as the default of None
        if not (actual_total is None or expected_total is None):
            unreported_obj["amount"] = expected_total - actual_total

            # Since the limit doesn't apply to anything except the awards category, always append the unreported object
            result_set.append(unreported_obj)

        result_set = sorted(result_set, key=lambda k: k["amount"], reverse=True)
    else:
        expected_total = actual_total

    return result_set, expected_total


def type_filter(_type, filters, limit=None):
    fiscal_year = None
    fiscal_quarter = None
    fiscal_date = None

    _types = [
        "budget_function",
        "budget_subfunction",
        "federal_account",
        "program_activity",
        "object_class",
        "recipient",
        "award",
        "award_category",
        "agency",
        "agency_type",
        "agency_sub",
    ]

    # Validate explorer _type
    if _type is None:
        raise InvalidParameterException('Missing Required Request Parameter, "type": "type"')

    elif _type not in _types:
        raise InvalidParameterException(
            "Type does not have a valid value. "
            "Valid Types: budget_function, budget_subfunction, federal_account, program_activity,"
            "object_class, recipient, award, award_category agency, agency_type, agency_sub"
        )

    if filters is None:
        raise InvalidParameterException('Missing Required Request Parameter, "filters": { "filter_options" }')

    # Get fiscal_date and fiscal_quarter
    for key, value in filters.items():
        if key == "fy":
            try:
                fiscal_year = int(value)
                if fiscal_year < 1000 or fiscal_year > 9999:
                    raise InvalidParameterException('Incorrect Fiscal Year Parameter, "fy": "YYYY"')
            except ValueError:
                raise InvalidParameterException('Incorrect or Missing Fiscal Year Parameter, "fy": "YYYY"')
        elif key == "quarter":
            if value in ("1", "2", "3", "4"):
                fiscal_quarter = int(value)
            else:
                raise InvalidParameterException(
                    "Incorrect value provided for quarter parameter. Must be a string between 1 and 4"
                )

    if fiscal_year:
        fiscal_date, fiscal_quarter = generate_last_completed_fiscal_quarter(
            fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter
        )

    # Recipient, Award Queryset
    award_queryset = (
        FinancialAccountsByAwards.objects.all()
        .exclude(transaction_obligated_amount__isnull=True)
        .exclude(transaction_obligated_amount="NaN")
        .filter(submission__reporting_fiscal_quarter=fiscal_quarter)
        .filter(submission__reporting_fiscal_year=fiscal_year)
        .annotate(amount=Sum("transaction_obligated_amount"))
    )

    # Base Queryset
    account_queryset = (
        FinancialAccountsByProgramActivityObjectClass.objects.all()
        .exclude(obligations_incurred_by_program_object_class_cpe__isnull=True)
        .filter(submission__reporting_fiscal_quarter=fiscal_quarter)
        .filter(submission__reporting_fiscal_year=fiscal_year)
        .annotate(amount=Sum("obligations_incurred_by_program_object_class_cpe"))
    )

    # Apply filters to queryset results
    award_queryset, account_queryset = spending_filter(award_queryset, account_queryset, filters, _type)

    if _type == "recipient" or _type == "award" or _type == "award_category" or _type == "agency_type":
        # Annotate and get explorer _type filtered results
        exp = QueryFactory(award_queryset, account_queryset)

        if _type == "recipient":
            award_queryset = exp.recipient()
        if _type == "award":
            award_queryset = exp.award()
        if _type == "award_category":
            award_queryset = exp.award_category()

        # Total value of filtered results
        actual_total = 0

        for award in award_queryset:
            award["id"] = str(award["id"])
            if _type in ["award", "award_category"]:
                code = None
                for code_type in ("piid", "fain", "uri"):
                    if award[code_type]:
                        code = award[code_type]
                        break
                for code_type in ("piid", "fain", "uri"):
                    del award[code_type]
                award["code"] = code
                if _type == "award":
                    award["name"] = code
            actual_total += award["total"]

        award_queryset = award_queryset[:limit] if _type == "award" else award_queryset

        results = {"total": actual_total, "end_date": fiscal_date, "results": award_queryset}

    else:
        # Annotate and get explorer _type filtered results
        exp = QueryFactory(award_queryset, account_queryset)

        if _type == "budget_function":
            account_queryset = exp.budget_function()
        if _type == "budget_subfunction":
            account_queryset = exp.budget_subfunction()
        if _type == "federal_account":
            account_queryset = exp.federal_account()
        if _type == "program_activity":
            account_queryset = exp.program_activity()
        if _type == "object_class":
            account_queryset = exp.object_class()
        if _type == "agency":
            account_queryset = exp.agency()

        # Actual total value of filtered results
        actual_total = account_queryset.aggregate(total=Sum("amount"))["total"]

        result_set, expected_total = get_unreported_data_obj(
            queryset=account_queryset,
            filters=filters,
            limit=limit,
            spending_type=_type,
            actual_total=actual_total,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
        )

        results = {"total": expected_total, "end_date": fiscal_date, "results": result_set}

    return results
