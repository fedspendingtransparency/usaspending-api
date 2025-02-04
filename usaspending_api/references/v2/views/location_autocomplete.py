from collections import OrderedDict
from typing import List

from elasticsearch_dsl import Q as ES_Q
from elasticsearch_dsl.response import Response as ES_Response
from elasticsearch_dsl.response.hit import Hit
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import LocationSearch
from usaspending_api.common.validator.tinyshield import validate_post_request

models = [
    {"key": "search_text", "name": "search_text", "type": "text", "text_type": "search", "optional": False},
    {"key": "limit", "name": "limit", "type": "integer", "max": 500, "optional": True, "default": 10},
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

        results = self._format_results(es_results.hits)
        results = {k: v for k, v in results.items() if v is not None}

        # Account for cases where there are multiple results in a single ES document
        results_length = sum(len(x) for x in results.values())
        return Response(OrderedDict([("count", results_length), ("results", results), ("messages", [""])]))

    @staticmethod
    def _filter_results(results: List[Hit], filter_keys: List[str]) -> List[Hit]:
        return list(filter(lambda x: any(key in dir(x.meta.highlight) for key in filter_keys), results))

    def _query_elasticsearch(self, search_text: str, limit: int = 10) -> ES_Response:
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

        # Elasticsearch queries don't work well with the "-" character so we remove it from any searches, specifically
        #   with Congressional districts in mind.
        search_text = search_text.replace("-", "")

        should_query = [
            ES_Q("match_phrase_prefix", **{"location_string": {"query": search_text, "boost": 5}}),
            ES_Q("match_phrase_prefix", **{"location_string.contains": {"query": search_text, "boost": 3}}),
            ES_Q("match", **{"location_string": {"query": search_text, "operator": "and", "boost": 1}}),
        ]

        query = ES_Q("bool", should=should_query, minimum_should_match=1)
        search: LocationSearch = LocationSearch().extra(size=limit).query(query)
        results: ES_Response = search.execute()

        return results

    def _format_results(self, es_results: List[Hit]) -> dict:
        """Format Elasticsearch results to match the API contract format

        Args:
            es_results: Elasticsearch result hits

        Returns:
            A dictionary containing all locations that matched the `search_text`

        Example:
            {
                "countries": ["Denmark", "Sweden"],
                "cities": [
                    "Denver, Colorado, United States",
                    "Camden, Arkansas, United States"
                ],
                "counties": [
                    {
                        "county_name": "Camden County, Arkansas, United States",
                        "county_fips": "12345"
                    }
                ]
            }
        """

        countries = []
        states = []
        cities = []
        counties = []
        zip_codes = []
        original_cds = []
        current_cds = []

        # Key: Value of the `location_type` field on the ES docs
        # Value: Lists from above that will contain the matches for that location type
        location_type_to_list_lookup = {
            "country": countries,
            "city": cities,
            "state": states,
            "county": counties,
            "zip_code": zip_codes,
            "original_congressional_district": original_cds,
            "current_congressional_district": current_cds,
        }

        for doc in es_results:
            if doc.location_type == "county":
                location = {"county_name": doc.location_string, "county_fips": doc.county_fips}
            elif doc.location_type in ("original_congressional_district", "current_congressional_district"):
                location = f"{doc.location_string[:2]}-{doc.location_string[2:]}"
            else:
                location = doc.location_string

            location_type_to_list_lookup[doc.location_type].append(location)

        results = {
            "countries": countries if countries else None,
            "states": states if states else None,
            "cities": cities if cities else None,
            "counties": counties if counties else None,
            "zip_codes": zip_codes if zip_codes else None,
            "districts_original": original_cds if original_cds else None,
            "districts_current": current_cds if current_cds else None,
        }

        return results
