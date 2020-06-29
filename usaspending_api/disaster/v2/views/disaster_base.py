from django.utils.functional import cached_property
from rest_framework.views import APIView

from django.db.models import Exists, OuterRef, Max
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.references.models.gtas_sf133_balances import GTASSF133Balances
from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.submissions.helpers import get_last_closed_submission_date, get_last_closed_submissions_of_each_FY
from usaspending_api.submissions.models import SubmissionAttributes
from django.db.models.functions import Concat
from django.db.models import Value, CharField

COVID_19_GROUP_NAME = "covid_19"


def covid_def_codes():
    return DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME)


def covid_def_code_strings():
    return [code["code"] for code in covid_def_codes().values("code")]


def latest_gtas_of_each_year_queryset():
    return GTASSF133Balances.objects.annotate(
        include=Exists(
            GTASSF133Balances.objects.values("fiscal_year")
            .annotate(fiscal_period_max=Max("fiscal_period"))
            .annotate(
                fiscal_period_and_year=Concat("fiscal_year", Value("/"), "fiscal_period", output_field=CharField())
            )
            .values("fiscal_year", "fiscal_period_max", "fiscal_period_and_year")
            .filter(
                fiscal_year=OuterRef("fiscal_year"),
                fiscal_year__gte=2020,
                fiscal_period_max=OuterRef("fiscal_period"),
                fiscal_period_and_year__in=[
                    f"{elem['submission_fiscal_year']}/{elem['submission_fiscal_month']}"
                    for elem in get_last_closed_submissions_of_each_FY(False)
                ],
                disaster_emergency_fund_code__in=covid_def_code_strings(),
            )
        )
    ).filter(include=True)


def latest_faba_of_each_year_queryset():
    return FinancialAccountsByAwards.objects.filter(submission__in=latest_submission_of_each_year_queryset())


def latest_submission_of_each_year_queryset():
    return SubmissionAttributes.objects.annotate(
        include=Exists(
            SubmissionAttributes.objects.values("reporting_fiscal_year")
            .annotate(fiscal_period_max=Max("reporting_fiscal_period"))
            .values("reporting_fiscal_year", "fiscal_period_max")
            .filter(
                reporting_fiscal_year=OuterRef("reporting_fiscal_year"),
                reporting_fiscal_year__gte=2020,
                fiscal_period_max=OuterRef("reporting_fiscal_period"),
            )
        )
    ).filter(include=True)


class DisasterBase(APIView):
    required_filters = ["def_codes"]
    reporting_period_min = "2020-04-01"

    @cached_property
    def filters(self):
        all_def_codes = list(DisasterEmergencyFundCode.objects.values_list("code", flat=True))
        object_keys_lookup = {
            "def_codes": {
                "key": "filter|def_codes",
                "name": "def_codes",
                "type": "array",
                "array_type": "enum",
                "enum_values": all_def_codes,
                "allow_nulls": False,
                "optional": False,
            },
            "query": {
                "key": "filter|query",
                "name": "query",
                "type": "text",
                "text_type": "search",
                "allow_nulls": True,
                "optional": True,
            },
            "award_type_codes": {
                "key": "filter|award_type_codes",
                "name": "award_type_codes",
                "type": "array",
                "array_type": "enum",
                "enum_values": list(award_type_mapping.keys()),
                "allow_nulls": True,
                "optional": True,
            },
        }
        model = [object_keys_lookup[key] for key in self.required_filters]
        json_request = TinyShield(model).block(self.request.data)
        return json_request["filter"]

    @property
    def def_codes(self):
        return self.filters["def_codes"]

    @cached_property
    def last_closed_monthly_submission_dates(self):
        return get_last_closed_submission_date(is_quarter=False)

    @cached_property
    def last_closed_quarterly_submission_dates(self):
        return get_last_closed_submission_date(is_quarter=True)


class SpendingMixin:
    required_filters = ["def_codes", "award_type_codes", "query"]

    @property
    def award_type_codes(self):
        return self.filters.get("award_type_codes")

    @property
    def query(self):
        return self.filters.get("query")

    @cached_property
    def spending_type(self):
        model = [
            {
                "key": "spending_type",
                "name": "spending_type",
                "type": "enum",
                "enum_values": ["total", "award"],
                "allow_nulls": False,
                "optional": False,
            }
        ]
        spending_type = TinyShield(model).block(self.request.data)
        return spending_type


class LoansMixin:
    required_filters = ["def_codes", "query"]

    @property
    def query(self):
        return self.filters.get("query")


class PaginationMixin:
    @cached_property
    def pagination(self):
        sortable_columns = ["id", "code", "description", "obligation", "outlay", "total_budgetary_resources"]
        default_sort_column = "id"
        model = customize_pagination_with_sort_columns(sortable_columns, default_sort_column)
        request_data = TinyShield(model).block(self.request.data.get("pagination", {}))
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", "obligated_amount"),
            sort_order=request_data["order"],
        )
