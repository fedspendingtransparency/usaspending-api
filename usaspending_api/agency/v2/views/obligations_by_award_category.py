from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any

from usaspending_api.agency.v2.views.agency_base import AgencyBase
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.filter_helpers import create_fiscal_year_filter
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.filters.elasticsearch.filter import QueryType


class ObligationsByAwardCategory(AgencyBase):
    """
    Returns a breakdown of obligations by award category (contracts, IDVs, grants, loans, direct payments, other)
    within the requested fiscal year (or current FY).
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/obligations_by_award_category.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params_to_validate = ["fiscal_year"]

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:

        results = self.query_elasticsearch()

        formatted_results = self.format_results(results)

        return Response(formatted_results)

    def query_elasticsearch(self):
        filters = {}
        filters["time_period"] = create_fiscal_year_filter(self.fiscal_year)
        filters["agencies"] = [{"toptier_id": self.agency_id, "type": "awarding", "tier": "toptier"}]
        query_with_filters = QueryWithFilters(QueryType.TRANSACTIONS)
        filter_query = query_with_filters.generate_elasticsearch_query(filters)
        search = TransactionSearch().filter(filter_query)

        search.aggs.bucket("group_by_award_category", "terms", field="award_category").metric(
            "obligations", "sum", field="generated_pragmatic_obligation"
        )

        return search.handle_execute().to_dict()

    def format_results(self, results):
        results_map = {}
        formatted_categories = []
        total = 0.0
        other_total = 0.0

        category_map = {
            "contract": "contracts",
            "direct payment": "direct_payments",
            "grant": "grants",
            "idv": "idvs",
            "loans": "loans",
            "other": "other",
        }

        # Reformat List of Objects as Map, so they can be alphabetized later
        for obj in results["aggregations"]["group_by_award_category"]["buckets"]:
            key = obj["key"]
            obligation_value = obj["obligations"]["value"]
            total += obligation_value

            if key in category_map.keys():
                results_map[key] = round(obligation_value, 2)
            else:
                other_total += obligation_value

        if results_map.get("other") is not None:
            results_map["other"] += round(other_total)
        else:
            results_map["other"] = round(other_total)

        # Convert Map back to list with extra categories
        for category, category_mapping in category_map.items():
            if category in results_map:
                formatted_categories.append(self.format_category(category_mapping, results_map[category]))
            else:
                formatted_categories.append(self.format_category(category_mapping, 0.0))

        formatted_results = {"total_aggregated_amount": round(total, 2), "results": formatted_categories}

        return formatted_results

    def format_category(self, category, aggregated_amount):
        return {"category": category, "aggregated_amount": aggregated_amount}
