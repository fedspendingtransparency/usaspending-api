from elasticsearch_dsl import Q
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.aggregation_helpers import create_count_aggregation
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.search.filters.elasticsearch.filter import QueryType

from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin


class RecipientCountViewSet(FabaOutlayMixin, AwardTypeMixin, DisasterBase):
    """
    Obtain the count of Recipients related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:
        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(self.filters)
        special_recipients = [
            "MULTIPLE RECIPIENTS",
            "REDACTED DUE TO PII",
            "MULTIPLE FOREIGN RECIPIENTS",
            "PRIVATE INDIVIDUAL",
            "PRIVATE INDIVIDUAL",
            "INDIVIDUAL RECIPIENT",
            "MISCELLANEOUS FOREIGN AWARDEES",
        ]
        shoulds = []
        for x in special_recipients:
            shoulds.append(Q("match", **{"recipient_name.keyword": x}))
        should_query = Q("bool", should=shoulds, minimum_should_match=1)
        must_not = Q("bool", must_not=should_query)
        search = AwardSearch().filter(filter_query & must_not)
        search.update_from_dict({"size": 0})
        search.aggs.bucket("recipient_count", create_count_aggregation("recipient_agg_key.hash"))
        results = search.handle_execute()
        recipients = results.to_dict().get("aggregations", {}).get("recipient_count", {}).get("value", 0)

        must = Q("bool", must=should_query)
        search2 = AwardSearch().filter(filter_query & must)
        search2.update_from_dict({"size": 0})
        search2.aggs.bucket("recipient_count", create_count_aggregation("recipient_name.keyword"))
        results2 = search2.handle_execute()
        special_recipients = results2.to_dict().get("aggregations", {}).get("recipient_count", {}).get("value", 0)
        return Response({"count": recipients + special_recipients})
