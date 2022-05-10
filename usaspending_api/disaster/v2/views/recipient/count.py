from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.aggregation_helpers import create_count_aggregation
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase

from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin


class RecipientCountViewSet(FabaOutlayMixin, AwardTypeMixin, DisasterBase):
    """
    Obtain the count of Recipients related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:
        filter_query = QueryWithFilters.generate_awards_elasticsearch_query(self.filters)
        search = AwardSearch().filter(filter_query)
        search.update_from_dict({"size": 0})
        search.aggs.bucket("recipient_count", create_count_aggregation("recipient_agg_key"))
        results = search.handle_execute()
        recipients = results.to_dict().get("aggregations", {}).get("recipient_count", {}).get("value", 0)

        return Response({"count": recipients})
