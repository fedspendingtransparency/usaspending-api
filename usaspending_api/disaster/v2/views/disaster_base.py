import json
from datetime import date
from functools import lru_cache
from typing import List, Optional

from django.db.models import Case, Count, DecimalField, F, Max, Q, QuerySet, Sum, Value, When
from django.db.models.functions import Coalesce
from django.http import HttpRequest
from django.utils.functional import cached_property
from django_cte import With
from rest_framework.views import APIView

from usaspending_api.awards.models.financial_accounts_by_awards import FinancialAccountsByAwards
from usaspending_api.awards.v2.lookups.lookups import assistance_type_mapping, award_type_mapping, loan_type_mapping
from usaspending_api.common.containers import Bunch
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.date_helper import now
from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_year_and_month
from usaspending_api.common.validator import TinyShield, customize_pagination_with_sort_columns
from usaspending_api.disaster.models import CovidFABASpending
from usaspending_api.references.helpers import get_def_codes_by_group
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.references.models.gtas_sf133_balances import GTASSF133Balances
from usaspending_api.submissions.helpers import get_last_closed_submission_date
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

COVID_19_GROUP_NAME = "covid_19"

REPORTING_PERIOD_MIN_DATE = date(2020, 4, 1)
REPORTING_PERIOD_MIN_YEAR, REPORTING_PERIOD_MIN_MONTH = generate_fiscal_year_and_month(REPORTING_PERIOD_MIN_DATE)


def latest_gtas_of_each_year_queryset():
    q = Q()
    for sub in final_submissions_for_all_fy():
        if not sub.is_quarter:
            q |= Q(fiscal_year=sub.fiscal_year) & Q(fiscal_period=sub.fiscal_period)
    if not q:
        return GTASSF133Balances.objects.none()
    return GTASSF133Balances.objects.filter(q)


def latest_faba_of_each_year_queryset() -> FinancialAccountsByAwards.objects:
    q = filter_by_latest_closed_periods()
    if not q:
        return FinancialAccountsByAwards.objects.none()
    return FinancialAccountsByAwards.objects.filter(q)


def filter_by_latest_closed_periods(submission_query_path: str = "") -> Q:
    """Return Django Q for all latest closed submissions (quarterly and monthly)"""
    return Q(**{f"{submission_query_path}submission__is_final_balances_for_fy": True})


def filter_by_defc_closed_periods() -> Q:
    """
    These filters should only be used when looking at submission data
    that includes DEF Codes, which only started appearing in submission
    for FY2020 P07 (Apr 1, 2020) and after
    """
    q = Q()
    for sub in final_submissions_for_all_fy():
        if (
            sub.fiscal_year == REPORTING_PERIOD_MIN_YEAR and sub.fiscal_period >= REPORTING_PERIOD_MIN_MONTH
        ) or sub.fiscal_year > REPORTING_PERIOD_MIN_YEAR:
            q |= (
                Q(submission__reporting_fiscal_year=sub.fiscal_year)
                & Q(submission__quarter_format_flag=sub.is_quarter)
                & Q(submission__reporting_fiscal_period__lte=sub.fiscal_period)
            )
    if not q:
        # Edgecase not expected in production. If there are no DABS between
        # FY2020 P07 (Apr 1, 2020) and now() then ensure nothing is returned
        q = Q(pk__isnull=True)
    return q & Q(submission__reporting_period_start__gte=str(REPORTING_PERIOD_MIN_DATE))


@lru_cache(maxsize=1)
def final_submissions_for_all_fy() -> List[tuple]:
    """
    Returns a list the latest monthly and quarterly submission for each
    fiscal year IF it is "closed" aka ready for display on USAspending.gov
    """
    return (
        DABSSubmissionWindowSchedule.objects.filter(submission_reveal_date__lte=now())
        .values("submission_fiscal_year", "is_quarter")
        .annotate(fiscal_year=F("submission_fiscal_year"), fiscal_period=Max("submission_fiscal_month"))
        .values_list("fiscal_year", "is_quarter", "fiscal_period", named=True)
    )


class DisasterBase(APIView):
    required_filters = ["def_codes"]

    @classmethod
    def requests_award_type_codes(cls, request: HttpRequest) -> bool:
        """Return True if an endpoint was requested with filter.award_type_codes"""

        # NOTE: The point at which this is used in the request life cycle, it has not been post-processed to include
        # a POST or data attribute. Must get payload from body
        if request and request.body:
            body_json = json.loads(request.body)
            if "filter" in body_json and "award_type_codes" in body_json["filter"]:
                return True
        return False

    @classmethod
    def requests_award_spending_type(cls, request: HttpRequest) -> bool:
        """Return True if an endpoint was requested with spending_type = award"""

        # NOTE: The point at which this is used in the request life cycle, it has not been post-processed to include
        # a POST or data attribute. Must get payload from body
        if request and request.body:
            body_json = json.loads(request.body)
            if body_json.get("spending_type", "") == "award":
                return True
        return False

    @cached_property
    def filters(self):
        all_def_codes = sorted(DisasterEmergencyFundCode.objects.values_list("code", flat=True))
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
                "enum_values": sorted(award_type_mapping.keys()),
                "allow_nulls": True,
                "optional": True,
            },
            "_loan_award_type_codes": {
                "key": "filter|award_type_codes",
                "name": "award_type_codes",
                "type": "array",
                "array_type": "enum",
                "enum_values": sorted(loan_type_mapping.keys()),
                "allow_nulls": True,
                "optional": True,
                "default": list(loan_type_mapping.keys()),
            },
            "_assistance_award_type_codes": {
                "key": "filter|award_type_codes",
                "name": "award_type_codes",
                "type": "array",
                "array_type": "enum",
                "enum_values": sorted(assistance_type_mapping.keys()),
                "allow_nulls": True,
                "optional": True,
                "default": list(assistance_type_mapping.keys()),
            },
        }
        model = [object_keys_lookup[key] for key in self.required_filters]
        json_request = TinyShield(model).block(self.request.data)
        return json_request["filter"]

    @property
    def def_codes(self):
        return self.filters["def_codes"]

    @cached_property
    def covid_def_codes(self):
        # While the filters for this method don't limit requests to only COVID DEFC, much of the logic does currently
        # limit outlay and obligations to only those relevant to COVID. A change was made that groups all DEFC
        # outlays and obligations together, but we still need to respect the limit to only COVID.
        covid_def_codes = get_def_codes_by_group(["covid_19"])["covid_19"]
        if self.def_codes:
            covid_def_codes = list(set(covid_def_codes) & set(self.def_codes))
        return covid_def_codes

    @cached_property
    def final_period_submission_query_filters(self):
        return filter_by_latest_closed_periods()

    @cached_property
    def latest_reporting_period(self):
        return get_last_closed_submission_date(False)

    @cached_property
    def all_closed_defc_submissions(self):
        return filter_by_defc_closed_periods()

    @property
    def is_in_provided_def_codes(self):
        return Q(disaster_emergency_fund__code__in=self.def_codes)

    def has_award_of_provided_type(self, should_join_awards: bool = True) -> Q:
        award_type_codes = self.filters.get("award_type_codes")
        if award_type_codes is not None:
            if should_join_awards:
                return Q(award__type__in=award_type_codes) & Q(award__isnull=False)
            else:
                return Q(type__in=award_type_codes)
        else:
            return Q()

    @property
    def has_award_of_classification(self):
        if self.filters.get("award_type"):
            # Simple check: if "procurement" then piid cannot be null, otherwise piid must be null
            return Q(piid__isnull=bool(self.filters["award_type"] == "assistance"))
        else:
            return Q()

    def construct_loan_queryset(self, faba_grouping_column, base_model, base_model_column):
        grouping_key = F(faba_grouping_column) if isinstance(faba_grouping_column, str) else faba_grouping_column

        base_values = With(
            FinancialAccountsByAwards.objects.filter(
                Q(award__type__in=loan_type_mapping), self.all_closed_defc_submissions, self.is_in_provided_def_codes
            )
            .annotate(
                grouping_key=grouping_key,
                total_loan_value=F("award__total_loan_value"),
                reporting_fiscal_year=F("submission__reporting_fiscal_year"),
                reporting_fiscal_period=F("submission__reporting_fiscal_period"),
                quarter_format_flag=F("submission__quarter_format_flag"),
            )
            .filter(grouping_key__isnull=False)
            .values(
                "grouping_key",
                "financial_accounts_by_awards_id",
                "award_id",
                "transaction_obligated_amount",
                "gross_outlay_amount_by_award_cpe",
                "reporting_fiscal_year",
                "reporting_fiscal_period",
                "quarter_format_flag",
                "total_loan_value",
            ),
            "base_values",
        )

        q = Q()
        for sub in final_submissions_for_all_fy():
            q |= (
                Q(reporting_fiscal_year=sub.fiscal_year)
                & Q(quarter_format_flag=sub.is_quarter)
                & Q(reporting_fiscal_period=sub.fiscal_period)
            )

        aggregate_faba = With(
            base_values.queryset()
            .values("grouping_key")
            .annotate(
                obligation=Coalesce(
                    Sum("transaction_obligated_amount"), 0, output_field=DecimalField(max_digits=23, decimal_places=2)
                ),
                outlay=Coalesce(
                    Sum(
                        Case(
                            When(q, then=F("gross_outlay_amount_by_award_cpe")),
                            default=Value(0),
                            output_field=DecimalField(max_digits=23, decimal_places=2),
                        )
                    ),
                    0,
                ),
            )
            .values("grouping_key", "obligation", "outlay"),
            "aggregate_faba",
        )

        distinct_awards = With(
            base_values.queryset().values("grouping_key", "award_id", "total_loan_value").distinct(), "distinct_awards"
        )

        aggregate_awards = With(
            distinct_awards.queryset()
            .values("grouping_key")
            .annotate(
                award_count=Count("award_id"),
                face_value_of_loan=Coalesce(
                    Sum("total_loan_value"), 0, output_field=DecimalField(max_digits=23, decimal_places=2)
                ),
            )
            .values("grouping_key", "award_count", "face_value_of_loan"),
            "aggregate_awards",
        )

        return Bunch(
            award_count_column=aggregate_awards.col.award_count,
            obligation_column=aggregate_faba.col.obligation,
            outlay_column=aggregate_faba.col.outlay,
            face_value_of_loan_column=aggregate_awards.col.face_value_of_loan,
            queryset=aggregate_awards.join(
                aggregate_faba.join(base_model, **{base_model_column: aggregate_faba.col.grouping_key}),
                **{base_model_column: aggregate_awards.col.grouping_key},
            )
            .with_cte(base_values)
            .with_cte(aggregate_faba)
            .with_cte(distinct_awards)
            .with_cte(aggregate_awards),
        )

    @staticmethod
    def accumulate_total_values(results: List[dict], extra_columns: List[str]) -> dict:
        totals = {"obligation": 0, "outlay": 0}

        for col in extra_columns:
            totals[col] = 0

        for res in results:
            for key in totals.keys():
                totals[key] += res.get(key) or 0

        return totals

    @staticmethod
    def sort_json_result(data_to_sort: dict, sort_key: str, sort_order: str, has_children: bool) -> dict:
        """Sort the JSON by the appropriate field and in the appropriate order before returning it.

        This assumes we're sorting the items within the `results` list within the provided dictionary.

        Args:
            data_to_sort: Unsorted JSON data.
            sort_key: The field to sort by.
            sort_order: Whether to sort the data in ascending or descending order.
            has_children: Does the JSON data contain child entries underneath a parent entry.

        Returns:
            Sorted JSON result.
        """

        if sort_key == "description":
            data_to_sort["results"] = sorted(
                data_to_sort["results"],
                key=lambda val: val.get("description", "id").lower(),
                reverse=sort_order == "desc",
            )
        else:
            data_to_sort["results"] = sorted(
                data_to_sort["results"],
                key=lambda val: val.get(sort_key, "id"),
                reverse=sort_order == "desc",
            )

        if has_children:
            for parent in data_to_sort["results"]:
                parent["children"] = sorted(
                    parent.get("children", []),
                    key=lambda val: val.get(sort_key, "id"),
                    reverse=sort_order == "desc",
                )

        return data_to_sort

    @staticmethod
    def get_covid_faba_spending(
        spending_level: str,
        def_codes: List[str],
        columns_to_return: List[str],
        award_types: Optional[List[str]] = None,
        search_query: Optional[str] = None,
        search_query_fields: Optional[List[str]] = None,
    ) -> QuerySet:
        """Query the covid_faba_spending table and return COVID-19 FABA spending grouped by the provided `spending_level`

        Args:
            spending_level: Spending level to group by
            def_codes: Disaster codes to filter by
            columns_to_return: List of columns to SELECT from the table
            award_types: Award types to filter by
            search_query: Text to search for in `search_query_fields`
            search_query_fields: Field to search for `search_query` in

        Returns:
            Matching rows from the covid_faba_spending table
        """

        queryset = (
            CovidFABASpending.objects.filter(spending_level=spending_level)
            .filter(defc__in=def_codes)
            .values(*columns_to_return)
            .annotate(
                award_count=Sum("award_count"),
                obligation_sum=Sum("obligation_sum"),
                outlay_sum=Sum("outlay_sum"),
                face_value_of_loan=Sum("face_value_of_loan"),
            )
        )

        if award_types is not None:
            queryset = queryset.filter(award_type__in=award_types)

        if search_query is not None and search_query_fields is not None:
            or_query = Q()

            for field in search_query_fields:
                or_query |= Q(**{f"{field}__icontains": search_query})

            queryset = queryset.filter(or_query)

        return queryset


class AwardTypeMixin:
    required_filters = ["def_codes", "award_type_codes"]

    @cached_property
    def award_type_codes(self):

        return self.filters.get("award_type_codes")


class FabaOutlayMixin:
    @property
    def outlay_field_annotation(self):
        return Coalesce(
            Sum(
                Case(
                    When(
                        self.final_period_submission_query_filters,
                        then=(
                            Coalesce(
                                F("gross_outlay_amount_by_award_cpe"),
                                0,
                                output_field=DecimalField(max_digits=23, decimal_places=2),
                            )
                            + Coalesce(
                                F("ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe"),
                                0,
                                output_field=DecimalField(max_digits=23, decimal_places=2),
                            )
                            + Coalesce(
                                F("ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe"),
                                0,
                                output_field=DecimalField(max_digits=23, decimal_places=2),
                            )
                        ),
                    ),
                    default=Value(0),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                )
            ),
            0,
            output_field=DecimalField(max_digits=23, decimal_places=2),
        )

    @property
    def obligated_field_annotation(self):
        return Coalesce(
            Sum("transaction_obligated_amount"), 0, output_field=DecimalField(max_digits=23, decimal_places=2)
        )


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

    def run_models(self, columns, default_sort_column="id"):
        model = customize_pagination_with_sort_columns(columns, default_sort_column)
        request_data = TinyShield(model).block(self.request.data.get("pagination", {}))
        return Pagination(
            page=request_data["page"],
            limit=request_data["limit"],
            lower_limit=(request_data["page"] - 1) * request_data["limit"],
            upper_limit=(request_data["page"] * request_data["limit"]),
            sort_key=request_data.get("sort", "obligation"),
            sort_order=request_data["order"],
            secondary_sort_key="id",
        )


class PaginationMixin(_BasePaginationMixin):
    @cached_property
    def pagination(self):
        sortable_columns = [
            "id",
            "code",
            "description",
            "award_count",
            "obligation",
            "outlay",
            "total_budgetary_resources",
        ]
        return self.run_models(sortable_columns)


class LoansPaginationMixin(_BasePaginationMixin):
    @cached_property
    def pagination(self):
        sortable_columns = ["id", "code", "description", "award_count", "obligation", "outlay", "face_value_of_loan"]
        return self.run_models(sortable_columns)
