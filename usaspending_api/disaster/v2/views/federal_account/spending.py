from typing import List

from django.db.models import Q, Sum, F, Value, DecimalField, Case, When, OuterRef, Subquery, Func, IntegerField
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api import settings
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.federal_account.federal_account_result import FedAcctResults, FedAccount, TAS
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    PaginationMixin,
    SpendingMixin,
    FabaOutlayMixin,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models.gtas_sf133_balances import GTASSF133Balances
from usaspending_api.search.v2.elasticsearch_helper import get_number_of_unique_nested_terms_accounts
from usaspending_api.disaster.v2.views.elasticsearch_account_base import (
    ElasticsearchAccountDisasterBase,
    ElasticsearchAccountSpendingPaginationMixin,
)


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


class SpendingViewSet(
    PaginationMixin,
    SpendingMixin,
    FabaOutlayMixin,
    ElasticsearchAccountDisasterBase,
    ElasticsearchAccountSpendingPaginationMixin,
    DisasterBase,
):
    """ Returns disaster spending by federal account. """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"
    agg_key = "financial_accounts_by_award.treasury_account_id"
    top_hits_fields = [
        "financial_accounts_by_award.treasury_account_title",
        "financial_accounts_by_award.treasury_account_symbol",
        "financial_accounts_by_award.federal_account_symbol",
        "financial_accounts_by_award.federal_account_title",
        "financial_accounts_by_award.federal_account_id",
    ]

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            defc = self.filters.pop("def_codes")
            self.filters.update({"nested_def_codes": defc})
            self.filters.update(
                {
                    "nested_nonzero_fields": [
                        "financial_accounts_by_award.transaction_obligated_amount",
                        "financial_accounts_by_award.gross_outlay_amount_by_award_cpe",
                    ]
                }
            )
            self.filter_query = QueryWithFilters.generate_accounts_elasticsearch_query(self.filters)
            self.bucket_count = get_number_of_unique_nested_terms_accounts(self.filter_query, f"{self.agg_key}")
            messages = []
            if self.pagination.sort_key in ("id", "code"):
                messages.append(
                    (
                        f"Notice! API Request to sort on '{self.pagination.sort_key}' field isn't fully implemented."
                        " Results were actually sorted using 'description' field."
                    )
                )
            if self.bucket_count > 10000 and self.agg_key == settings.ES_ROUTING_FIELD:
                self.bucket_count = 10000
                messages.append(
                    (
                        "Notice! API Request is capped at 10,000 results. Either download to view all results or"
                        " filter using the 'query' attribute."
                    )
                )

            response = self.query_elasticsearch()
            response["page_metadata"] = get_pagination_metadata(
                self.bucket_count, self.pagination.limit, self.pagination.page
            )
            if messages:
                response["messages"] = messages

            response["results"] = sorted(
                response["results"],
                key=lambda x: x[self.pagination.sort_key],
                reverse=self.pagination.order_by != "desc",
            )
            return Response(response)
        else:
            results = list(self.total_queryset)
            extra_columns = ["total_budgetary_resources"]

        response = construct_response(results, self.pagination)
        response["totals"] = self.accumulate_total_values(results, extra_columns)

        return Response(response)

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        temp_results = {}
        child_results = []
        for bucket in info_buckets:
            child = self._build_child_json_result(bucket)
            child_results.append(child)
        for child in child_results:
            result = self._build_json_result(child)
            child.pop("parent_data")
            if result["id"] in temp_results.keys():
                temp_results[result["id"]] = {
                    "id": int(result["id"]),
                    "code": result["code"],
                    "description": result["description"],
                    "award_count": temp_results[result["id"]]["award_count"] + result["award_count"],
                    # the count of distinct awards contributing to the totals
                    "obligation": temp_results[result["id"]]["obligation"] + result["obligation"],
                    "outlay": temp_results[result["id"]]["outlay"] + result["outlay"],
                    "total_budgetary_resources": None,
                    "children": sorted(
                        (temp_results[result["id"]]["children"] + result["children"]),
                        key=lambda x: x[self.pagination.sort_key],
                        reverse=self.pagination.order_by != "desc",
                    ),
                }
            else:
                temp_results[result["id"]] = result
        results = [x for x in temp_results.values()]
        return results

    def _build_json_result(self, child):
        return {
            "id": child["parent_data"][2],
            "code": child["parent_data"][1],
            "description": child["parent_data"][0],
            "award_count": child["award_count"],
            # the count of distinct awards contributing to the totals
            "obligation": child["obligation"],
            "outlay": child["outlay"],
            "total_budgetary_resources": None,
            "children": [child],
        }

    def _build_child_json_result(self, bucket: dict):
        return {
            "id": int(bucket["key"]),
            "code": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["treasury_account_symbol"],
            "description": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["treasury_account_title"],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket["count_awards_by_dim"]["award_count"]["value"]),
            "obligation": int(bucket["sum_covid_obligation"]["value"]),
            "outlay": int(bucket["sum_covid_outlay"]["value"]),
            "total_budgetary_resources": None,
            "parent_data": [
                bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["federal_account_title"],
                bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["federal_account_symbol"],
                bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["federal_account_id"],
            ],
        }

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
                            then=F("obligations_incurred_by_program_object_class_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("gross_outlay_amount_by_program_object_class_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "total_budgetary_resources": Coalesce(
                Subquery(
                    GTASSF133Balances.objects.filter(
                        disaster_emergency_fund_code__in=self.def_codes,
                        fiscal_period=self.latest_reporting_period["submission_fiscal_month"],
                        fiscal_year=self.latest_reporting_period["submission_fiscal_year"],
                        treasury_account_identifier=OuterRef("treasury_account"),
                    )
                    .annotate(amount=Func("total_budgetary_resources_cpe", function="Sum"))
                    .values("amount"),
                    output_field=DecimalField(),
                ),
                0,
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
