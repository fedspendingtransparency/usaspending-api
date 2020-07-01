from django.utils.functional import cached_property
from rest_framework.views import APIView
from datetime import datetime, timezone

from django.db.models import Max
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.validator import customize_pagination_with_sort_columns, TinyShield
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule
from usaspending_api.references.models.gtas_sf133_balances import GTASSF133Balances
from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.submissions.helpers import get_last_closed_submission_date
from django.db.models import Q

COVID_19_GROUP_NAME = "covid_19"


def covid_def_codes():
    return DisasterEmergencyFundCode.objects.filter(group_name=COVID_19_GROUP_NAME)


def covid_def_code_strings():
    return [code["code"] for code in covid_def_codes().values("code")]


def latest_gtas_of_each_year_queryset():
    q = Q()
    for final_for_fy in finals_for_fy(False):
        q |= Q(fiscal_year=final_for_fy[0]) & Q(fiscal_period=final_for_fy[1])
    for final_for_fy in finals_for_fy(True):
        q |= Q(fiscal_year=final_for_fy[0]) & Q(fiscal_period=final_for_fy[1])
    if not q:
        return GTASSF133Balances.objects.none()
    return GTASSF133Balances.objects.filter(q)


def latest_faba_of_each_year_queryset():
    q = Q()
    for final_for_fy in finals_for_fy(False):
        q |= Q(submission__reporting_fiscal_year=final_for_fy[0]) & Q(
            submission__reporting_fiscal_period=final_for_fy[1]
        )
    for final_for_fy in finals_for_fy(True):
        q |= Q(submission__reporting_fiscal_year=final_for_fy[0]) & Q(
            submission__reporting_fiscal_period=final_for_fy[1]
        )
    if not q:
        return FinancialAccountsByAwards.objects.none()
    return FinancialAccountsByAwards.objects.filter(q)


def finals_for_fy(is_quarter):
    return (
        DABSSubmissionWindowSchedule.objects.filter(
            submission_reveal_date__lte=datetime.now(timezone.utc), is_quarter=is_quarter
        )
        .values("submission_fiscal_year")
        .annotate(max_submission_fiscal_month=Max("submission_fiscal_month"))
        .values_list("submission_fiscal_year", "max_submission_fiscal_month")
    )


class DisasterBase(APIView):
    required_filters = ["def_codes"]
    reporting_period_min = "2020-04-01"

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
    def last_closed_monthly_submission_dates(self):
        return get_last_closed_submission_date(is_quarter=False)

    @cached_property
    def last_closed_quarterly_submission_dates(self):
        return get_last_closed_submission_date(is_quarter=True)


class AwardTypeMixin:
    required_filters = ["def_codes", "award_type_codes"]

    @property
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
        sortable_columns = ["id", "code", "description", "count", "face_value_of_loan"]
        return self.run_models(sortable_columns)
