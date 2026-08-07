from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from django.db.models import QuerySet, Sum

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import GTASSF133Balances
from usaspending_api.spending_explorer.v2.filters.explorer import Explorer
from usaspending_api.spending_explorer.v2.filters.spending_filter import spending_filter
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

UNREPORTED_DATA_NAME = "Unreported Data"
VALID_UNREPORTED_DATA_TYPES = ["agency", "budget_function", "object_class"]
VALID_UNREPORTED_FILTERS = ["fy", "quarter", "period"]


@dataclass
class UnreportedDataParams:
    """
    Params:
        queryset: Django queryset with all necessary filters, etc already applied
        filters: filters provided in POST request to endpoint
        limit: number of results to limit to
        spending_type: spending explorer category
        actual_total: total calculated based on results in `queryset`
        fiscal_year: fiscal year from request
        fiscal_period: final fiscal period for fiscal quarter requested
    """
    queryset: QuerySet
    filters: dict[str, str | int]
    limit: int | None
    spending_type: str
    actual_total: float | None
    fiscal_year: int
    fiscal_period: int


def get_unreported_data_obj(params: UnreportedDataParams) \
        -> tuple[list[dict[str, Any]], float | None]:
    """Returns the modified list of result objects including the object corresponding to the unreported amount, only
    if applicable. If the unreported amount does not fit within the limit of results provided, it will not be added.

    Args:
        params: UnreportedDataParams

    Returns:
        result_set: modified (if applicable) result set as a list
        expected_total: total calculated from GTAS
    """

    queryset = params.queryset[:params.limit] if params.spending_type == "award" else params.queryset

    result_keys = ["id", "code", "type", "name", "amount"]
    if params.spending_type == "agency":
        result_keys.append("link")
    if params.spending_type == "federal_account":
        result_keys.append("account_number")

    result_set = [
        {k: (v if k != "id" else str(v)) for k, v in entry.items()}
        for entry in queryset.values(*result_keys)
    ]

    gtas = (
        GTASSF133Balances.objects.filter(
            fiscal_year=params.fiscal_year,
            fiscal_period=params.fiscal_period)
        .values("fiscal_year", "fiscal_period")
        .annotate(Sum("obligations_incurred_total_cpe"))
        .values("obligations_incurred_total_cpe__sum")
    )

    expected_total = gtas[0]["obligations_incurred_total_cpe__sum"] if gtas else None

    if (params.spending_type in VALID_UNREPORTED_DATA_TYPES and
            set(params.filters.keys()).issubset(set(VALID_UNREPORTED_FILTERS))):
        unreported_obj = {
            "id": None,
            "code": None,
            "type": params.spending_type,
            "name": UNREPORTED_DATA_NAME,
            "amount": None
        }

        # if both values are actually available, then calculate the amount, otherwise leave it as the default of None
        if not (params.actual_total is None or expected_total is None):
            unreported_obj["amount"] = expected_total - params.actual_total

            # Since the limit doesn't apply to anything except the awards category, always append the unreported object
            result_set.append(unreported_obj)

        result_set = sorted(result_set, key=lambda k: k["amount"], reverse=True)
    else:
        expected_total = params.actual_total

    return result_set, expected_total


@dataclass
class FiscalPeriodInfo:
    fiscal_year: int
    fiscal_period: int
    fiscal_date: date


def _validate_type(_type: str | None) -> str:
    """Validate the explorer type parameter."""
    valid_types = [
        "agency", "award", "award_category", "budget_function",
        "budget_subfunction", "federal_account", "object_class",
        "program_activity", "recipient",
    ]

    if _type is None:
        raise InvalidParameterException('Missing Required Request Parameter, "type": "type"')

    if _type not in valid_types:
        raise InvalidParameterException(f"Type does not have a valid value. Valid Types: {valid_types}")

    return _type


def _validate_filters(filters: dict[str, str | int] | None) -> dict[str, str | int]:
    """Validate the filters parameter."""
    if filters is None:
        raise InvalidParameterException('Missing Required Request Parameter, "filters": { "filter_options" }')

    if "fy" not in filters:
        raise InvalidParameterException('Missing required parameter "fy".')

    if "quarter" not in filters and "period" not in filters:
        raise InvalidParameterException('Missing required parameter, provide either "period" or "quarter".')

    return filters


def _validate_fiscal_year(filters: dict[str, str | int]) -> int:
    """Validate and return fiscal year."""
    try:
        fiscal_year = int(filters["fy"])
        if fiscal_year < 1000 or fiscal_year > 9999:
            raise InvalidParameterException('Incorrect Fiscal Year Parameter, "fy": "YYYY"')
        return fiscal_year
    except ValueError:
        raise InvalidParameterException('Incorrect or Missing Fiscal Year Parameter, "fy": "YYYY"') from None


def _validate_time_unit(filters: dict[str, str | int]) -> tuple[str, int]:
    """Validate and return time unit (quarter or period) and its value."""
    time_unit = "quarter" if "quarter" in filters else "period"

    if time_unit == "quarter" and filters["quarter"] not in ("1", "2", "3", "4", 1, 2, 3, 4):
        raise InvalidParameterException("Incorrect value provided for quarter parameter. Must be between 1 and 4")

    if time_unit == "period" and int(filters["period"]) not in range(1, 13):
        raise InvalidParameterException("Incorrect value provided for period parameter. Must be between 1 and 12")

    fiscal_unit = int(filters[time_unit])
    return time_unit, fiscal_unit


def _get_submission_window(fiscal_year: int, time_unit: str, fiscal_unit: int) -> DABSSubmissionWindowSchedule:
    """Get and validate submission window."""
    if time_unit == "quarter":
        submission_window = DABSSubmissionWindowSchedule.objects.filter(
            submission_fiscal_year=fiscal_year,
            submission_fiscal_quarter=fiscal_unit,
            is_quarter=True,
            submission_reveal_date__lte=datetime.now(timezone.utc),
        ).first()
    else:
        submission_window = DABSSubmissionWindowSchedule.objects.filter(
            submission_fiscal_year=fiscal_year,
            submission_fiscal_month=fiscal_unit,
            submission_reveal_date__lte=datetime.now(timezone.utc),
        ).first()

    if submission_window is None:
        raise InvalidParameterException("Fiscal parameters provided do not belong to a current submission period")

    return submission_window


def _get_base_querysets(fiscal_year: int, fiscal_period: int) -> tuple[QuerySet, QuerySet]:
    """Get base querysets for alt_set and queryset."""
    alt_set = FinancialAccountsByAwards.objects.filter(
        submission__reporting_fiscal_year=fiscal_year,
        submission__reporting_fiscal_period__lte=fiscal_period
    ).annotate(amount=Sum("transaction_obligated_amount"))

    file_b_calculations = FileBCalculations()
    queryset = FinancialAccountsByProgramActivityObjectClass.objects.filter(
        submission__reporting_fiscal_year=fiscal_year,
        submission__reporting_fiscal_period=fiscal_period
    ).annotate(amount=Sum(file_b_calculations.get_obligations()))

    return alt_set, queryset


def _process_award_types(
        _type: str,
        alt_set: QuerySet,
        queryset: QuerySet,
        limit: int | None,
        fiscal_date: date
) -> dict[str, Any]:
    """Process award, award_category, and recipient types."""
    exp = Explorer(alt_set, queryset)

    if _type == "recipient":
        alt_set = exp.recipient()
    elif _type == "award":
        alt_set = exp.award()
    elif _type == "award_category":
        alt_set = exp.award_category()

    if limit is not None:
        alt_set = alt_set[:limit]

    actual_total = 0

    for award in alt_set:
        award["id"] = str(award["id"])

        if _type in ["award", "award_category"]:
            code = next((award[code_type] for code_type in ("piid", "fain", "uri") if award[code_type]), None)
            for code_type in ("piid", "fain", "uri"):
                del award[code_type]
            award["code"] = code
            if _type == "award":
                award["name"] = code

        if award["amount"] is None:
            award["amount"] = 0
        if award["name"] is None:
            award["name"] = f"Blank {_type.capitalize().replace('_', ' ')}"

        actual_total += award["total"] or 0

    result_set = list(alt_set)
    result_set.sort(key=lambda k: k["amount"], reverse=True)

    return {"total": actual_total, "end_date": fiscal_date, "results": result_set}


@dataclass
class NonAwardTypeParams:
    _type: str
    alt_set: QuerySet
    queryset: QuerySet
    filters: dict[str, str | int]
    limit: int | None
    fiscal_year: int
    fiscal_period: int
    fiscal_date: date


def _process_non_award_types(params: NonAwardTypeParams) -> dict[str, Any]:
    """Process non-award types (budget_function, agency, etc.)."""
    exp = Explorer(params.alt_set, params.queryset)

    type_methods = {
        "budget_function": exp.budget_function,
        "budget_subfunction": exp.budget_subfunction,
        "federal_account": exp.federal_account,
        "program_activity": exp.program_activity,
        "object_class": exp.object_class,
        "agency": exp.agency,
    }

    queryset = type_methods[params._type]()
    if params.limit is not None:
        queryset = queryset[:params.limit]

    actual_total = queryset.aggregate(amount_sum=Sum("amount"))["amount_sum"] or 0

    unreported_params = UnreportedDataParams(
        queryset=queryset,
        filters=params.filters,
        limit=params.limit,
        spending_type=params._type,
        actual_total=actual_total,
        fiscal_year=params.fiscal_year,
        fiscal_period=params.fiscal_period
    )

    result_set, expected_total = get_unreported_data_obj(unreported_params)

    return {"total": expected_total, "end_date": params.fiscal_date, "results": result_set}


def type_filter(
        _type: str | None,
        filters: dict[str, str | int] | None,
        limit: int | None = None
) -> dict[str, Any]:
    """Main type filter function with reduced complexity."""
    # Validation
    _type = _validate_type(_type)
    filters = _validate_filters(filters)
    fiscal_year = _validate_fiscal_year(filters)
    time_unit, fiscal_unit = _validate_time_unit(filters)

    # Get submission window and fiscal info
    submission_window = _get_submission_window(fiscal_year, time_unit, fiscal_unit)
    fiscal_date = submission_window.period_end_date
    fiscal_period = submission_window.submission_fiscal_month

    # Get base querysets
    alt_set, queryset = _get_base_querysets(fiscal_year, fiscal_period)

    # Apply filters
    alt_set, queryset = spending_filter(alt_set, queryset, filters, _type)

    # Process based on type
    if _type in {"award", "award_category", "recipient"}:
        return _process_award_types(_type, alt_set, queryset, limit, fiscal_date)
    else:
        params = NonAwardTypeParams(
            _type=_type,
            alt_set=alt_set,
            queryset=queryset,
            filters=filters,
            limit=limit,
            fiscal_year=fiscal_year,
            fiscal_period=fiscal_period,
            fiscal_date=fiscal_date
        )
        return _process_non_award_types(params)
