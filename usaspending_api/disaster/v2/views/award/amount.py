from rest_framework.request import Request
from rest_framework.response import Response
from usaspending_api.common.cache_decorator import cache_response

from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.elasticsearch_base import ElasticsearchDisasterBase


class AmountViewSet(ElasticsearchDisasterBase):
    """Returns aggregated values of obligation, outlay, and count of Award records"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/amount.md"
    count_only = False

    agg_key = "recipient_agg_key"
    bucket_count = 1
    sort_column_mapping = None

    sum_column_mapping = {
        "obligation": "total_covid_obligation",
        "outlay": "total_covid_outlay",
        "face_value_of_loan": "total_loan_value",
    }

    @cache_response()
    def post(self, request: Request) -> Response:
        self.filter_query = QueryWithFilters.generate_awards_elasticsearch_query(self.filters)

        search = self.build_elasticsearch_search_with_aggregations()

        response = search.handle_execute()
        response = response.aggs.to_dict()

        totals = self.build_totals(response.get("totals", {}).get("filtered_aggs", {}))

        return Response(totals)
