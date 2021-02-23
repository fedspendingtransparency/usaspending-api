from typing import List
from decimal import Decimal

from django.db.models import Q, Sum, F, Value, DecimalField, Case, When, OuterRef, Subquery, Func, IntegerField
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.federal_account.federal_account_result import FedAcctResults, FedAccount, TAS
from usaspending_api.disaster.v2.views.disaster_base import (
    FabaOutlayMixin,
    latest_gtas_of_each_year_queryset,
    PaginationMixin,
    SpendingMixin,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.disaster.v2.views.elasticsearch_account_base import ElasticsearchAccountDisasterBase


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
    """ Returns disaster spending by federal account. """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"
    agg_key = "financial_accounts_by_award.treasury_account_id"  # primary (tier-1) aggregation key
    nested_nonzero_fields = {"obligation": "transaction_obligated_amount", "outlay": "gross_outlay_amount_by_award_cpe"}
    query_fields = [
        "federal_account_symbol",
        "federal_account_symbol.contains",
        "federal_account_title",
        "federal_account_title.contains",
        "treasury_account_symbol",
        "treasury_account_symbol.contains",
        "treasury_account_title",
        "treasury_account_title.contains",
    ]
    top_hits_fields = [
        "financial_accounts_by_award.federal_account_symbol",
        "financial_accounts_by_award.federal_account_title",
        "financial_accounts_by_award.treasury_account_symbol",
        "financial_accounts_by_award.treasury_account_title",
        "financial_accounts_by_award.federal_account_id",
    ]

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            self.has_children = True
            return self.perform_elasticsearch_search()
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
                    "children": temp_results[result["id"]]["children"] + result["children"],
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
            **{
                key: Decimal(bucket.get(f"sum_{val}", {"value": 0})["value"])
                for key, val in self.nested_nonzero_fields.items()
            },
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
                            then=F("obligations_incurred_by_program_object_class_cpe")
                            - F("deobligations_recoveries_refund_pri_program_object_class_cpe"),
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
                            then=F("gross_outlay_amount_by_program_object_class_cpe")
                            + F("ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe")
                            + F("ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "total_budgetary_resources": Coalesce(
                Subquery(
                    latest_gtas_of_each_year_queryset()
                    .filter(
                        disaster_emergency_fund_code__in=self.def_codes,
                        treasury_account_identifier=OuterRef("treasury_account"),
                    )
                    .annotate(
                        amount=Func("total_budgetary_resources_cpe", function="Sum"),
                        unobligated_balance=Func(
                            "budget_authority_unobligated_balance_brought_forward_cpe", function="Sum"
                        ),
                        deobligation=Func("deobligations_or_recoveries_or_refunds_from_prior_year_cpe", function="Sum"),
                        prior_year=Func("prior_year_paid_obligation_recoveries", function="Sum"),
                    )
                    .annotate(
                        total_budget_authority=F("amount")
                        - F("unobligated_balance")
                        - F("deobligation")
                        - F("prior_year")
                    )
                    .values("total_budget_authority"),
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
