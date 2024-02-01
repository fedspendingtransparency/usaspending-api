from elasticsearch_dsl import Q as ES_Q
from elasticsearch_dsl.query import QueryString
from elasticsearch_dsl.response import Response as ES_Response
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import LocationSearch
from usaspending_api.common.validator.tinyshield import validate_post_request

models = [
    {"key": "search_text", "name": "search_text", "type": "text", "text_type": "search", "optional": False},
    {"key": "limit", "name": "limit", "type": "integer", "max": 500, "optional": True, "default": 10},
]
ES_LOCATION_FIELDS = (
    "country_name",
    "state_name",
    "cities",
    "counties",
    "zip_codes",
    "current_congressional_district",
    "original_congressional_district",
)


@validate_post_request(models)
class LocationAutocompleteViewSet(APIView):
    """
    This end point returns a list of locations from Elasticsearch that match the given search_text value.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/location.md"

    @cache_response()
    def post(self, request):
        es_results: ES_Response = query_elasticsearch(request.data["search_text"], request.data["limit"])

        city_results = filter(lambda x: x.meta.highlight.coun, es_results)

        response = {"message": "test"}

        return Response(response)


def query_elasticsearch(search_text: str, limit: int = 10) -> ES_Response:
    """Query Elasticsearch for any locations that match the provided `search_text` up to `limit` number of results.

    Args:
        search_text:
            Text to search for in any field in Elasticsearch.
        limit:
            Maximum number of results to return.
            Defaults to 10.

    Returns:
        An Elasticsearch Response object containing the list of locations that contain the provided `search_text`.
    """

    query: QueryString = ES_Q("query_string", query=f"*{search_text}*", fields=ES_LOCATION_FIELDS)

    search: LocationSearch = LocationSearch().extra(size=limit).filter(query)
    search = search.highlight(
        "country_name",
        "state_name",
        "cities",
        "counties",
        "zip_codes",
        "current_congressional_district",
        "original_congressional_district",
    )
    search = search.highlight_options(order="score", pre_tags=[""], post_tags=[""])

    results: ES_Response = search.execute()

    return results


def _country_formatter():
    ...


def _state_formatter():
    ...


def _others_formatter():
    ...
