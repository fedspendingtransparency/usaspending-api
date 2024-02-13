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

        if len(es_results) > 0:
            results = {}

            country_results = self._format_country_results(
                list(filter(lambda x: "country_name" in dir(x.meta.highlight), es_results))
            )
            state_results = self._format_state_results(
                list(filter(lambda x: "state_name" in dir(x.meta.highlight), es_results))
            )
            city_results = self._format_city_results(
                list(filter(lambda x: "cities" in dir(x.meta.highlight), es_results))
            )
            county_results = self._format_county_results(
                list(filter(lambda x: "counties" in dir(x.meta.highlight), es_results))
            )
            zip_code_results = self._format_zip_code_results(
                list(filter(lambda x: "zip_codes" in dir(x.meta.highlight), es_results))
            )
            current_cd_results = self._format_current_cd_results(
                list(filter(lambda x: "current_congressional_districts" in dir(x.meta.highlight), es_results))
            )
            original_cd_results = self._format_original_cd_results(
                list(filter(lambda x: "original_congressional_districts" in dir(x.meta.highlight), es_results))
            )

            if country_results is not None:
                results["countries"] = country_results
            if state_results is not None:
                results["states"] = state_results
            if city_results is not None:
                results["cities"] = city_results
            if county_results is not None:
                results["counties"] = county_results
            if zip_code_results is not None:
                results["zip_codes"] = zip_code_results
            if original_cd_results is not None:
                results["districts_original"] = original_cd_results
            if current_cd_results is not None:
                results["districts_current"] = current_cd_results

            # Account for cases where there are multiple results in a single ES document
            results_length = sum(len(x) for x in results.values())

            return Response(OrderedDict([("count", results_length), ("results", results), ("messages", [""])]))
        else:
            return Response(OrderedDict([("count", len(es_results)), ("results", {}), ("messages", [""])]))

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

        es_location_fields = (
            "country_name",
            "state_name",
            "cities",
            "counties",
            "zip_codes",
            "current_congressional_districts",
            "original_congressional_districts",
        )

        # Elasticsearch queries don't work well with the "-" character so we remove it from any searches, specifically
        #   with Congressional districts in mind.
        search_text = search_text.replace("-", "")

        query_string_query = ES_Q("query_string", query=f"*{search_text}*", fields=es_location_fields)
        multi_match_query = ES_Q("multi_match", query=search_text, fields=es_location_fields)
        query = ES_Q("bool", should=[query_string_query, multi_match_query], minimum_should_match=1)

        search: LocationSearch = LocationSearch().extra(size=limit).query(query)
        search = search.highlight(
            "country_name",
            "state_name",
            "cities",
            "counties",
            "zip_codes",
            "current_congressional_districts",
            "original_congressional_districts",
        )
        search = search.highlight_options(order="score", pre_tags=[""], post_tags=[""])

        results: ES_Response = search.execute()

        return results

    def _format_country_results(self, es_results: List[Hit]) -> Union[List, None]:
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
        if len(es_results) > 0:
            return [{"country_name": doc.country_name} for doc in es_results]
        else:
            return None

    def _format_state_results(self, es_results: List[Hit]) -> Union[List, None]:
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

        if len(es_results) > 0:
            return [{"state_name": doc.state_name, "country_name": doc.country_name} for doc in es_results]
        else:
            return None

    def _format_city_results(self, es_results: List[Hit]) -> Union[List, None]:
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
                for city in doc.meta.highlight.cities:
                    cities.append({"city_name": city, "state_name": doc.state_name, "country_name": doc.country_name})
            return cities
        else:
            return None

    def _format_county_results(self, es_results: List[Hit]) -> Union[List, None]:
        """Format Elasticsearch results containing county matches

        Args:
            es_results: Elasticsearch results that contained a match in the `counties` field

        Returns:
            A list containing all county names that matched the `search_text` along with the
            state_name and country_name fields.

        Example:
            [
                {"county_name": "GADSDEN", "state_name": "FLORIDA", "country_name": "UNITED STATES"},
                {"county_name": "CAMDEN", "state_name": "GEORGIA", "country_name": "UNITED STATES"}
            ]
        """

        if len(es_results) > 0:
            counties = []
            for doc in es_results:
                for county in doc.meta.highlight.counties:
                    counties.append(
                        {"county_name": county, "state_name": doc.state_name, "country_name": doc.country_name}
                    )
            return counties
        else:
            return None

    def _format_zip_code_results(self, es_results: List[Hit]) -> Union[List, None]:
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
                for zip in doc.meta.highlight.zip_codes:
                    zip_codes.append({"zip_code": zip, "state_name": doc.state_name, "country_name": doc.country_name})
            return zip_codes
        else:
            return None

    def _format_current_cd_results(self, es_results: List[Hit]) -> Union[List, None]:
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
                for current_cd in doc.meta.highlight.current_congressional_districts:
                    # Add the hyphen back to the Congressional districts to be consistent with other endpoints
                    current_cd = f"{current_cd[:2]}-{current_cd[2:]}"
                    current_cds.append(
                        {"current_cd": current_cd, "state_name": doc.state_name, "country_name": doc.country_name}
                    )
            return current_cds
        else:
            return None

    def _format_original_cd_results(self, es_results: List[Hit]) -> Union[List, None]:
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
                for original_cd in doc.meta.highlight.original_congressional_districts:
                    # Add the hyphen back to the Congressional districts to be consistent with other endpoints
                    original_cd = f"{original_cd[:2]}-{original_cd[2:]}"
                    original_cds.append(
                        {"original_cd": original_cd, "state_name": doc.state_name, "country_name": doc.country_name}
                    )
            return original_cds
        else:
            return None
