from datetime import datetime, timezone, date
from django.db.models import Max, Q, F
from django.utils.functional import cached_property
from rest_framework.views import APIView
from typing import List

from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_year_and_month
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.references.models.gtas_sf133_balances import GTASSF133Balances
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

COVID_19_GROUP_NAME = "covid_19"


def covid_def_codes():
    return DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME)


def covid_def_code_strings():
    return [code["code"] for code in covid_def_codes().values("code")]


def latest_gtas_of_each_year_queryset() -> GTASSF133Balances:
    q = Q()
    for sub in final_submissions_for_all_fy():
        if not sub.is_quarter:
            q |= Q(fiscal_year=sub.fiscal_year) & Q(fiscal_period=sub.fiscal_period)
    if not q:
        return GTASSF133Balances.objects.none()
    return GTASSF133Balances.objects.filter(q)


def latest_faba_of_each_year_queryset() -> FinancialAccountsByAwards:
    q = filter_by_latest_closed_periods()
    if not q:
        return FinancialAccountsByAwards.objects.none()
    return FinancialAccountsByAwards.objects.filter(q)


def filter_by_latest_closed_periods() -> Q:
    """Return Django Q for all latest closed submissions (quarterly and monthly)"""
    q = Q()
    for sub in final_submissions_for_all_fy():
        q |= (
            Q(submission__reporting_fiscal_year=sub.fiscal_year)
            & Q(submission__quarter_format_flag=sub.is_quarter)
            & Q(submission__reporting_fiscal_period=sub.fiscal_period)
        )
    return q


def final_submissions_for_all_fy() -> List[tuple]:
    """
        Returns a list the latest monthly and quarterly submission for each
        fiscal year IF it is "closed" aka ready for display on USAspending.gov
    """
    return (
        DABSSubmissionWindowSchedule.objects.filter(submission_reveal_date__lte=datetime.now(timezone.utc))
        .values("submission_fiscal_year", "is_quarter")
        .annotate(fiscal_year=F("submission_fiscal_year"), fiscal_period=Max("submission_fiscal_month"))
        .values_list("fiscal_year", "is_quarter", "fiscal_period", named=True)
    )


class DisasterBase(APIView):
    required_filters = ["def_codes"]
    reporting_period_min_date = date(2020, 4, 1)
    reporting_period_min_year, reporting_period_min_month = generate_fiscal_year_and_month(reporting_period_min_date)

    @cached_property
    def filters(self):
        all_def_codes = sorted(list(DisasterEmergencyFundCode.objects.values_list("code", flat=True)))
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
    def final_period_submission_query_filters(self):
        return filter_by_latest_closed_periods()

    @cached_property
    def all_closed_defc_submissions(self):
        """
            These filters should only be used when looking at submission data
            that includes DEF Codes, which only started appearing in submission
            for FY2020 P07 (Apr 1, 2020) and after
        """
        q = Q()
        for sub in final_submissions_for_all_fy():
            if (
                sub.fiscal_year == self.reporting_period_min_year
                and sub.fiscal_period >= self.reporting_period_min_month
            ) or sub.fiscal_year > self.reporting_period_min_year:
                q |= (
                    Q(submission__reporting_fiscal_year=sub.fiscal_year)
                    & Q(submission__quarter_format_flag=sub.is_quarter)
                    & Q(submission__reporting_fiscal_period__lte=sub.fiscal_period)
                )
        return q & Q(submission__reporting_period_start__gte=str(self.reporting_period_min_date))


class AwardTypeMixin:
    required_filters = ["def_codes", "award_type_codes"]

    @cached_property
    def award_type_codes(self):

        return self.filters.get("award_type_codes")


class SpendingMixin:
    required_filters = ["def_codes", "query"]

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

        return TinyShield(model).block(self.request.data)["spending_type"]


class LoansMixin:
    required_filters = ["def_codes", "query"]

    @property
    def query(self):
        return self.filters.get("query")


class _BasePaginationMixin:
    def pagination(self):
        """pass"""

    def run_models(self, columns):
        default_sort_column = "id"
        model = customize_pagination_with_sort_columns(columns, default_sort_column)
        request_data = TinyShield(model).block(self.request.data.get("pagination", {}))
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", "obligated_amount"),
            sort_order=request_data["order"],
        )


class PaginationMixin(_BasePaginationMixin):
    @cached_property
    def pagination(self):
        sortable_columns = ["id", "code", "description", "obligation", "outlay", "total_budgetary_resources", "count"]
        return self.run_models(sortable_columns)


class LoansPaginationMixin(_BasePaginationMixin):
    @cached_property
    def pagination(self):
        sortable_columns = ["id", "code", "description", "count", "face_value_of_loan", "obligation", "outlay"]
        return self.run_models(sortable_columns)
