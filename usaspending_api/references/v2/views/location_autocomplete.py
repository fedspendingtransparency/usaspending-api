from collections import OrderedDict
from typing import List, Union

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
        results = {
            "countries": self._format_country_results(self._filter_results(es_results, ["country_name"])),
            "states": self._format_state_results(self._filter_results(es_results, ["state_name"])),
            "cities": self._format_city_results(self._filter_results(es_results, ["city_name"])),
            "counties": self._format_county_results(self._filter_results(es_results, ["county_name", "county_fips"])),
            "zip_codes": self._format_zip_code_results(self._filter_results(es_results, ["zip_code"])),
            "districts_current": self._format_current_cd_results(
                self._filter_results(es_results, ["current_congressional_district"])
            ),
            "districts_original": self._format_original_cd_results(
                self._filter_results(es_results, ["original_congressional_district"])
            ),
        }
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
        es_location_fields = [
            "country_name",
            "state_name",
            "city_name",
            "county_name",
            "zip_code",
            "current_congressional_district",
            "original_congressional_district",
        ]

        # Elasticsearch queries don't work well with the "-" character so we remove it from any searches, specifically
        #   with Congressional districts in mind.
        search_text = search_text.replace("-", "")

        should_query = []

        for es_field in es_location_fields:
            should_query.append(ES_Q("match_phrase_prefix", **{es_field: {"query": search_text, "boost": 5}}))
            should_query.append(
                ES_Q("match_phrase_prefix", **{f"{es_field}.contains": {"query": search_text, "boost": 3}})
            )
            should_query.append(
                ES_Q(
                    "match",
                    **{
                        es_field: {
                            "query": search_text,
                            "operator": "and",
                            "boost": 1,
                        }
                    },
                )
            )

        query = ES_Q("bool", should=should_query, minimum_should_match=1)

        search: LocationSearch = LocationSearch().extra(size=limit).query(query)
        search = search.highlight(*es_location_fields).highlight_options(order="score", pre_tags=[""], post_tags=[""])

        results: ES_Response = search.execute()

        return results

    def _format_country_results(self, es_results: List[Hit]) -> Union[set, None]:
        """Format Elasticsearch results containing country matches

        Args:
            es_results: Elasticsearch results that contained a match in the `country_name` field

        Returns:
            A list containing all country names that matched the `search_text`

        Example:
            [
                {"country_name": "DENMARK"},
                {"country_name": "SWEDEN"}
            ]
        """

        countries = []

        if len(es_results) > 0:
            for doc in es_results:
                country_json = {"country_name": doc.country_name}
                if country_json not in countries:
                    countries.append(country_json)

            return countries
        else:
            return None

    def _format_state_results(self, es_results: List[Hit]) -> Union[set, None]:
        """Format Elasticsearch results containing state matches

        Args:
            es_results: Elasticsearch results that contained a match in the `state_name` field

        Returns:
            A list containing all state names that matched the `search_text` along with the
            country_name field.

        Example:
            [
                {"state_name": "Missouri", "country_name": "UNITED STATES"},
                {"state_name": "Mississippi", "country_name": "UNITED STATES"}
            ]
        """

        states = []

        if len(es_results) > 0:
            for doc in es_results:
                state_json = {"state_name": doc.state_name, "country_name": doc.country_name}
                if state_json not in states:
                    states.append(state_json)

            return states
        else:
            return None

    def _format_city_results(self, es_results: List[Hit]) -> Union[set, None]:
        """Format Elasticsearch results containing city matches

        Args:
            es_results: Elasticsearch results that contained a match in the `cities` field

        Returns:
            A list containing all city names that matched the `search_text` along with the
            state_name and country_name fields.

        Example:
            [
                {"city_name": "HOLDEN", "state_name": "LOUISIANA", "country_name": "UNITED STATES"},
                {"city_name": "DENVER", "state_name": "COLORADO", "country_name": "UNITED STATES"}
            ]
        """

        if len(es_results) > 0:
            cities = []

            for doc in es_results:
                city_json = {"city_name": doc.city_name, "state_name": doc.state_name, "country_name": doc.country_name}
                if city_json not in cities:
                    cities.append(city_json)

            return cities
        else:
            return None

    def _format_county_results(self, es_results: List[Hit]) -> Union[set, None]:
        """Format Elasticsearch results containing county matches

        Args:
            es_results: Elasticsearch results that contained a match in the `counties.name` or `counties.fips` field

        Returns:
            A list of objects with county_fips and county_name properties that matched the `search_text` along with the
            state_name and country_name fields.

        Example:
            [
                {"county_fips": "12039", "county_name": "GADSDEN", "state_name": "FLORIDA", "country_name": "UNITED STATES"},
                {"county_fips": "13039", "county_name": "CAMDEN", "state_name": "GEORGIA", "country_name": "UNITED STATES"}
            ]
        """

        if len(es_results) > 0:
            counties = []

            for doc in es_results:
                county_json = {
                    "county_fips": doc.county_fips,
                    "county_name": doc.county_name,
                    "state_name": doc.state_name,
                    "country_name": doc.country_name,
                }
                if county_json not in counties:
                    counties.append(county_json)

            return counties
        else:
            return None

    def _format_zip_code_results(self, es_results: List[Hit]) -> Union[set, None]:
        """Format Elasticsearch results containing zip code matches

        Args:
            es_results: Elasticsearch results that contained a match in the `zip_codes` field

        Returns:
            A list containing all zip codes that matched the `search_text` along with the
            state_name and country_name fields.

        Example:
            [
                {"zip_code": "60123", "state_name": "ILLINOIS", "country_name": "UNITED STATES"},
                {"zip_code": "61231", "state_name": "ILLINOIS", "country_name": "UNITED STATES"}
            ]
        """

        if len(es_results) > 0:
            zip_codes = []
            for doc in es_results:
                zip_code_json = {
                    "zip_code": doc.zip_code,
                    "state_name": doc.state_name,
                    "country_name": doc.country_name,
                }
                if zip_code_json not in zip_codes:
                    zip_codes.append(zip_code_json)

            return zip_codes
        else:
            return None

    def _format_current_cd_results(self, es_results: List[Hit]) -> Union[set, None]:
        """Format Elasticsearch results containing current congressional district matches

        Args:
            es_results: Elasticsearch results that contained a match in the `current_congressional_districts` field

        Returns:
            A list containing all current congressional districts that matched the `search_text` along with the
            state_name and country_name fields.

        Example:
            [
                {"current_cd": "TX-36", "state_name": "TEXAS", "country_name": "UNITED STATES"},
                {"current_cd": "CA-36", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"}
            ]
        """

        if len(es_results) > 0:
            current_cds = []

            for doc in es_results:
                current_cd_json = {
                    "current_cd": doc.current_congressional_district,
                    "state_name": doc.state_name,
                    "country_name": doc.country_name,
                }
                if current_cd_json not in current_cds:
                    current_cds.append(current_cd_json)

            return current_cds
        else:
            return None

    def _format_original_cd_results(self, es_results: List[Hit]) -> Union[set, None]:
        """Format Elasticsearch results containing original congressional district matches

        Args:
            es_results: Elasticsearch results that contained a match in the `original_congressional_districts` field

        Returns:
            A list containing all original congressional districts that matched the `search_text` along with the
            state_name and country_name fields.

        Example:
            [
                {"original_cd": "NY-36", "state_name": "NEW YORK", "country_name": "UNITED STATES"},
                {"original_cd": "CA-36", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"}
            ]
        """

        if len(es_results) > 0:
            original_cds = []

            for doc in es_results:
                original_cd_json = {
                    "original_cd": doc.original_congressional_district,
                    "state_name": doc.state_name,
                    "country_name": doc.country_name,
                }
                if original_cd_json not in original_cds:
                    original_cds.append(original_cd_json)

            return original_cds
        else:
            return None
