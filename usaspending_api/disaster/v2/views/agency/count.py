from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.aggregation_helpers import create_count_aggregation
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import AwardTypeMixin
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, FabaOutlayMixin
from usaspending_api.search.filters.elasticsearch.filter import QueryType


class AgencyCountViewSet(AwardTypeMixin, FabaOutlayMixin, DisasterBase):
    """
    Obtain the count of Agencies related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/count.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(self.filters)
        search = AwardSearch().filter(filter_query)
        search.update_from_dict({"size": 0})
        search.aggs.bucket("agency_count", create_count_aggregation("funding_toptier_agency_agg_key.hash"))
        results = search.handle_execute()
        agencies = results.to_dict().get("aggregations", {}).get("agency_count", {}).get("value", 0)
        return Response({"count": agencies})
