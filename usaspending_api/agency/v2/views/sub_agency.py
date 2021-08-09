from django.db.models import Sum, Q
from django.utils.functional import cached_property
from elasticsearch_dsl import A
from rest_framework.request import Request
from rest_framework.response import Response
from typing import Any, List
from usaspending_api.agency.v2.views.agency_base import AgencyBase, PaginationMixin
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters


class SubAgencyList(PaginationMixin, AgencyBase):
    """
    Obtain the list of subagencies and offices based on the provided toptier_code and fiscal year, as well as award type.
    Can be either by funding agency or awarding agency.
    Based on transaction data.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/agency/toptier_code/sub_agency.md"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.additional_models = [
            {
                "name": "award_type",
                "key": "award_type",
                "type": "array",
                "array_type": "enum",
                "enum_values": list(award_type_mapping.keys()) + ["no intersection"],
                "optional": True,
            },
            {
                "name": "agency_type",
                "key": "agency_type",
                "type": "enum",
                "enum_values": ("awarding", "funding"),
                "default": "awarding",
            },
        ]
        self.params_to_validate = ["fiscal_year", "agency_type", "award_type", "order", "sort"]

    @cached_property
    def _query_params(self):
        query_params = self.request.query_params.copy()
        if query_params.get("award_type") is not None:
            query_params["award_type"] = query_params["award_type"].strip("[]").split(",")
        return self._validate_params(query_params)

    @cache_response()
    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.sortable_columns = ["name", "obligated_amount", "transaction_count", "new_award_count"]
        self.default_sort_column = "obligated_amount"
        results = self.get_sub_agency_list()
        page_metadata = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)
        return Response(
            {
                "toptier_code": self.toptier_code,
                "fiscal_year": self.fiscal_year,
                "page_metadata": page_metadata,
                "results": results,
                "messages": self.standard_response_messages,
            }
        )

    def format_elasticsearch_results(self, results):
        response = []
        buckets = results.aggs.to_dict().get("subtier_agencies", {}).get("buckets", [])
        for bucket in buckets:
            response.append(
                {
                    "subtier_agency_name": bucket.get("key"),
                    "subtier_agency_abbreviation": bucket.get("subagency_info")
                    .get("hits")
                    .get("hits")[0]
                    .get("_source")
                    .get(f"{self._query_params.get('agency_type')}_subtier_agency_abbreviation"),
                    "total_obligations": bucket.get("total_subagency_obligations").get("value"),
                    "transaction_count": bucket.get("doc_count"),
                    "new_award_counts": 0,
                    "children": [],
                }
            )
        return response

    def generate_query(self):
        return {
            "agencies": [
                {"type": self._query_params.get("agency_type"), "tier": "toptier", "name": self.toptier_agency.name}
            ],
            "time_period": [{"start_date": f"{self.fiscal_year - 1}-10-01", "end_date": f"{self.fiscal_year}-09-30"}],
            "award_type_codes": self._query_params.get("award_type", []),
        }

    def get_sub_agency_list(self):
        response = self.query_elasticsearch()
        return self.format_elasticsearch_results(response)

    def query_elasticsearch(self):
        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(self.generate_query())
        search = TransactionSearch().filter(filter_query)
        subagency_dim_metadata = A(
            "top_hits",
            size=1,
            _source={"includes": [f"{self._query_params.get('agency_type')}_subtier_agency_abbreviation"]},
        )
        office_dim_metadata = A(
            "top_hits",
            size=1,
            _source={"includes": [f"{self._query_params.get('agency_type')}_office_name"]},
        )
        subtier_agency_agg = A("terms", field=f"{self._query_params.get('agency_type')}_subtier_agency_name.keyword")
        office_agg = A("terms", field=f"{self._query_params.get('agency_type')}_office_code.keyword")
        obligation_agg = A("sum", field="generated_pragmatic_obligation")
        search.aggs.bucket("subtier_agencies", subtier_agency_agg).metric(
            "total_subagency_obligations", obligation_agg
        ).metric("subagency_info", subagency_dim_metadata).bucket("offices", office_agg).metric(
            "total_office_obligations", A("sum", field="generated_pragmatic_obligation")
        ).metric(
            "office_info", office_dim_metadata
        )
        response = search.handle_execute()
        return response
