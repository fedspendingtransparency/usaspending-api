import json
from collections import OrderedDict

from elasticsearch_dsl import Q as ES_Q
from elasticsearch_dsl.response import Response as ES_Response
from elasticsearch_dsl.utils import AttrList
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import LocationSearch
from usaspending_api.common.validator.tinyshield import validate_post_request

models = [
    {
        "key": "search_text",
        "name": "search_text",
        "type": "text",
        "text_type": "search",
        "optional": False,
    },
    {
        "key": "limit",
        "name": "limit",
        "type": "integer",
        "max": 20,
        "optional": True,
        "default": 5,
    },
]


@validate_post_request(models)
class LocationAutocompleteViewSet(APIView):
    """
    This end point returns a list of locations from Elasticsearch that match the given search_text value.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/autocomplete/location.md"

    @cache_response()
    def post(self, request):
        es_results: ES_Response = self._query_elasticsearch(request.data["search_text"], request.data["limit"])

        if len(es_results.aggregations.location_types.buckets) == 0:
            return Response(OrderedDict([("count", 0), ("results", {}), ("messages", [""])]))

        results = self._format_results(es_results.aggregations.location_types.buckets)
        results = {k: v for k, v in results.items() if v is not None}

        # Account for cases where there are multiple results in a single ES document
        results_length = sum(len(x) for x in results.values())
        return Response(OrderedDict([("count", results_length), ("results", results), ("messages", [""])]))

    def _query_elasticsearch(self, search_text: str, limit: int = 10) -> ES_Response:
        """Query Elasticsearch for any locations that match the provided `search_text` up to `limit` number of results
            for each `location_type`.

        Args:
            search_text:
                Text to search for in any field in Elasticsearch.
            limit:
                Maximum number of results, of each location_type, to return.
                Defaults to 10.

        Returns:
            An Elasticsearch Response object containing the list of locations that contain the provided `search_text`.
        """

        # Elasticsearch queries don't work well with the "-" character so we remove it from any searches, specifically
        #   with Congressional districts in mind.
        search_text = search_text.replace("-", "")

        should_query = [
            ES_Q(
                "match_phrase_prefix",
                **{"location": {"query": search_text, "boost": 5}},
            ),
            ES_Q(
                "match_phrase_prefix",
                **{"location.contains": {"query": search_text, "boost": 3}},
            ),
            ES_Q(
                "match",
                **{"location": {"query": search_text, "operator": "and", "boost": 1}},
            ),
        ]

        query = ES_Q("bool", should=should_query, minimum_should_match=1)
        search: LocationSearch = (
            LocationSearch()
            .extra(size=0)
            .query(query)
            .sort(
                {"_score": {"order": "desc"}},
                {"location.keyword": {"order": "asc"}},
            )
        )
        # Group by location_type then get the top `limit` results for each bucket
        search.aggs.bucket("location_types", "terms", field="location_type").metric(
            "most_relevant", "top_hits", size=limit, _source=["location_json", "location_type"]
        )
        results: ES_Response = search.execute()

        return results

    def _format_results(self, location_types_buckets: AttrList) -> dict:
        """Format Elasticsearch results to match the API contract format

        Args:
            es_results: List of buckets created from aggregations

        Returns:
            A dictionary containing all locations that matched the `search_text`

        Example:
            {
                "countries": [
                    {
                        "country_name": "Denmark"
                    },
                    {
                        "country_name": "Sweden"
                    }
                ],
                "cities": [
                    {
                        "city_name": "Denver",
                        "state_name": "Colorado",
                        "country_name": "United States"
                    },
                    {
                        "city_name": "Capelle aan den IJssel",
                        "state_name": None,
                        "country_name": "Netherlands"
                    }
                ],
                "counties": [
                    {
                        "county_name": "Camden County",
                        "state_name": "Arkansas",
                        "country_name": "United States",
                        "county_fips": "12345"
                    }
                ]
            }
        """

        location_singular_plural_mapping = {
            "country": "countries",
            "city": "cities",
            "state": "states",
            "county": "counties",
            "zip_code": "zip_codes",
            "original_cd": "districts_original",
            "current_cd": "districts_current",
        }
        results = {v: [] for v in location_singular_plural_mapping.values()}

        for location_type in location_types_buckets:
            for doc in location_type.most_relevant.hits:
                location_json = json.loads(doc.location_json)

                # The 'location_type' key is only used during indexing so we can remove it now
                del location_json["location_type"]

                results[location_singular_plural_mapping[doc.location_type]].append(location_json)

        return {k: v if v else None for k, v in results.items()}
