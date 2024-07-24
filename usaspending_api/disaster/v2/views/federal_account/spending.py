from decimal import Decimal
from typing import List

from django.db.models import (
    Case,
    DecimalField,
    F,
    Func,
    IntegerField,
    OuterRef,
    Q,
    QuerySet,
    Subquery,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.models import CovidFABASpending
from usaspending_api.disaster.v2.views.disaster_base import (
    FabaOutlayMixin,
    PaginationMixin,
    SpendingMixin,
    latest_gtas_of_each_year_queryset,
)
from usaspending_api.disaster.v2.views.elasticsearch_account_base import ElasticsearchAccountDisasterBase
from usaspending_api.disaster.v2.views.federal_account.federal_account_result import TAS, FedAccount, FedAcctResults
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


def construct_response(results: list, pagination: Pagination):
    FederalAccounts = FedAcctResults()
    for row in results:
        FA = FedAccount(
            id=row.pop("fa_id"), code=row.pop("fa_code"), award_count=0, description=row.pop("fa_description")
        )
        FederalAccounts[FA].include(TAS(**row))

    return {
        "results": FederalAccounts.finalize(pagination),
        "page_metadata": get_pagination_metadata(len(FederalAccounts), pagination.limit, pagination.page),
    }


class SpendingViewSet(SpendingMixin, FabaOutlayMixin, ElasticsearchAccountDisasterBase, PaginationMixin):
    """Returns disaster spending by federal account."""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            self.has_children = True
            account_db_results = self._get_covid_faba_spending()
            json_result = self._build_json_result(account_db_results)
            sorted_json_result = self._sort_json_result(json_result)

            return Response(sorted_json_result)
        else:
            results = list(self.total_queryset)
            extra_columns = ["total_budgetary_resources"]

        response = construct_response(results, self.pagination)
        response["totals"] = self.accumulate_total_values(results, extra_columns)

        return Response(response)

    def _get_covid_faba_spending(self) -> QuerySet:
        queryset = (
            CovidFABASpending.objects.filter(spending_level="treasury_account")
            .filter(defc__in=self.filters["def_codes"])
            .values(
                "funding_federal_account_id",
                "funding_federal_account_code",
                "funding_federal_account_name",
                "funding_treasury_account_id",
                "funding_treasury_account_code",
                "funding_treasury_account_name",
            )
            .annotate(
                award_count=Sum("award_count"),
                obligation_sum=Sum("obligation_sum"),
                outlay_sum=Sum("outlay_sum"),
            )
        )

        if self.query is not None:
            queryset = queryset.filter(
                Q(funding_federal_account_name__icontains=self.query)
                | Q(funding_treasury_account_name__icontains=self.query)
            )

        return queryset

    def _build_json_result(self, queryset: List[QuerySet]) -> dict:
        """Build the JSON response that will be returned for this endpoint.

        Args:
            queryset: Database query results.

        Returns:
            Formatted JSON response.
        """

        response = {"totals": {"obligation": 0, "outlay": 0, "award_count": 0}, "results": []}

        parent_lookup = {}
        child_lookup = {}

        for row in queryset:
            parent_federal_account_id = int(row["funding_federal_account_id"])

            if parent_federal_account_id not in parent_lookup.keys():
                parent_lookup[parent_federal_account_id] = {
                    "id": parent_federal_account_id,
                    "code": row["funding_federal_account_code"],
                    "description": row["funding_federal_account_name"],
                    "award_count": 0,
                    "obligation": Decimal(0),
                    "outlay": Decimal(0),
                    # This type of response never has a TBR value
                    "total_budgetary_resources": None,
                    "children": [],
                }

            if parent_federal_account_id not in child_lookup.keys():
                child_lookup[parent_federal_account_id] = [
                    {
                        "id": int(row["funding_treasury_account_id"]),
                        "code": row["funding_treasury_account_code"],
                        "description": row["funding_treasury_account_name"],
                        "award_count": int(row["award_count"]),
                        "obligation": Decimal(row["obligation_sum"]),
                        "outlay": Decimal(row["outlay_sum"]),
                    }
                ]
            else:
                child_lookup[parent_federal_account_id].append(
                    {
                        "id": int(row["funding_treasury_account_id"]),
                        "code": row["funding_treasury_account_code"],
                        "description": row["funding_treasury_account_name"],
                        "award_count": int(row["award_count"]),
                        "obligation": Decimal(row["obligation_sum"]),
                        "outlay": Decimal(row["outlay_sum"]),
                    }
                )

            response["totals"]["obligation"] += Decimal(row["obligation_sum"])
            response["totals"]["outlay"] += Decimal(row["outlay_sum"])
            response["totals"]["award_count"] += row["award_count"]

        for parent_account_id, children in child_lookup.items():
            for child_ta_account in children:
                parent_lookup[parent_account_id]["children"].append(child_ta_account)
                parent_lookup[parent_account_id]["award_count"] += child_ta_account["award_count"]
                parent_lookup[parent_account_id]["obligation"] += child_ta_account["obligation"]
                parent_lookup[parent_account_id]["outlay"] += child_ta_account["outlay"]

        response["results"] = list(parent_lookup.values())

        response["page_metadata"] = get_pagination_metadata(
            len(response["results"]), self.pagination.limit, self.pagination.page
        )

        return response

    def _sort_json_result(self, json_result: dict) -> dict:
        """Sort the JSON by the appropriate field and in the appropriate order before returning it.

        Args:
            json_result: Unsorted JSON result.

        Returns:
            Sorted JSON result.
        """

        if self.pagination.sort_key == "description":
            sorted_parents = sorted(
                json_result["results"],
                key=lambda val: val.get("description", "id").lower(),
                reverse=self.pagination.sort_order == "desc",
            )
        else:
            sorted_parents = sorted(
                json_result["results"],
                key=lambda val: val.get(self.pagination.sort_key, "id"),
                reverse=self.pagination.sort_order == "desc",
            )

        if self.has_children:
            for parent in sorted_parents:
                parent["children"] = sorted(
                    parent.get("children", []),
                    key=lambda val: val.get(self.pagination.sort_key, "id"),
                    reverse=self.pagination.sort_order == "desc",
                )

        json_result["results"] = sorted_parents

        return json_result

    @property
    def total_queryset(self):
        filters = [
            self.is_in_provided_def_codes,
            self.is_non_zero_total_spending,
            self.all_closed_defc_submissions,
            Q(treasury_account__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
        ]

        annotations = {
            "fa_code": F("treasury_account__federal_account__federal_account_code"),
            "description": F("treasury_account__account_title"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "award_count": Value(None, output_field=IntegerField()),
            "fa_description": F("treasury_account__federal_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("obligations_incurred_by_program_object_class_cpe")
                            + F("deobligations_recoveries_refund_pri_program_object_class_cpe"),
                        ),
                        default=Value(0),
                        output_field=DecimalField(max_digits=23, decimal_places=2),
                    )
                ),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("gross_outlay_amount_by_program_object_class_cpe")
                            + F("ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe")
                            + F("ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe"),
                        ),
                        default=Value(0),
                        output_field=DecimalField(max_digits=23, decimal_places=2),
                    )
                ),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
            "total_budgetary_resources": Coalesce(
                Subquery(
                    latest_gtas_of_each_year_queryset()
                    .filter(
                        disaster_emergency_fund_id__in=self.def_codes,
                        treasury_account_identifier=OuterRef("treasury_account"),
                    )
                    .annotate(
                        amount=Func("total_budgetary_resources_cpe", function="Sum"),
                        unobligated_balance=Func(
                            "budget_authority_unobligated_balance_brought_forward_cpe", function="Sum"
                        ),
                        deobligation=Func("deobligations_or_recoveries_or_refunds_from_prior_year_cpe", function="Sum"),
                        prior_year=Func("prior_year_paid_obligation_recoveries", function="Sum"),
                        unobligated_adjustments=Func(
                            "adjustments_to_unobligated_balance_brought_forward_fyb", function="Sum"
                        ),
                    )
                    .annotate(
                        total_budget_authority=F("amount")
                        - F("unobligated_balance")
                        - F("deobligation")
                        - F("prior_year")
                        - F("unobligated_adjustments")
                    )
                    .values("total_budget_authority"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values(
                "treasury_account__federal_account__id",
                "treasury_account__federal_account__federal_account_code",
                "treasury_account__federal_account__account_title",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )
