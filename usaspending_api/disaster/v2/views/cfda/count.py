from elasticsearch_dsl import Q as ES_Q
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.disaster.v2.views.elasticsearch_base import ElasticsearchLoansPaginationMixin
from usaspending_api.search.v2.elasticsearch_helper import get_number_of_unique_terms_for_awards
from usaspending_api.search.filters.elasticsearch.filter import QueryType


class CfdaCountViewSet(DisasterBase):
    """
    This route takes DEF Codes and Award Type Codes and returns count of CFDA.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/cfda/count.md"

    required_filters = ["def_codes", "_assistance_award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:
        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(self.filters)

        # Ensure that only non-zero values are taken into consideration
        non_zero_columns = list(ElasticsearchLoansPaginationMixin.sum_column_mapping.values())
        non_zero_queries = []
        for field in non_zero_columns:
            non_zero_queries.append(ES_Q("range", **{field: {"gt": 0}}))
            non_zero_queries.append(ES_Q("range", **{field: {"lt": 0}}))
        filter_query.must.append(ES_Q("bool", should=non_zero_queries, minimum_should_match=1))

        bucket_count = get_number_of_unique_terms_for_awards(filter_query, "cfda_number.hash")

        return Response({"count": bucket_count})
