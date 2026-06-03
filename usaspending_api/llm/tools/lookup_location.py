import json
import logging
from typing import Any

from elasticsearch_dsl import Q

from usaspending_api.common.elasticsearch.search_wrappers import LocationSearch
from usaspending_api.llm.models.py_models import AITool, AIToolDescription
from usaspending_api.references.models import PopCounty, RefCountryCode

logger = logging.getLogger(__name__)


class LocationLookupTool:
    """Tool for looking up locations in OpenSearch with fuzzy matching support."""

    # Location type mappings
    LOCATION_TYPES = {
        "country", "state", "city", "county", "zip_code", "current_cd", "original_cd"
    }

    ENTITY_DISPLAY_MAP = {
        "country": "Country",
        "state": "State",
        "city": "City",
        "county": "County",
        "zip_code": "Zip code",
        "current_cd": "Current congressional district",
        "original_cd": "Original congressional district",
    }

    def __init__(self):
        """Initialize with cached lookups from database."""
        self._state_code_cache = None
        self._country_code_cache = None

    @property
    def state_codes(self) -> dict[str, str]:
        """Lazy-load state codes from PopCounty model."""
        if self._state_code_cache is None:
            self._state_code_cache = {}
            for county in PopCounty.objects.values('state_name', 'state_code').distinct():
                state_name = county['state_name']
                state_code = county['state_code']
                if state_name and state_code:
                    # Store lowercase for case-insensitive lookup
                    self._state_code_cache[state_name.lower()] = state_code
        return self._state_code_cache

    @property
    def country_codes(self) -> dict[str, str]:
        """Lazy-load country codes from RefCountryCode model."""
        if self._country_code_cache is None:
            self._country_code_cache = {}
            for country in RefCountryCode.objects.values('country_name', 'country_code'):
                country_name = country['country_name']
                country_code = country['country_code']
                if country_name and country_code:
                    # Store lowercase for case-insensitive lookup
                    self._country_code_cache[country_name.lower()] = country_code
        return self._country_code_cache

    def lookup_location(
            self,
            query: str,
            location_type: str | None = None,
            top_k: int = 15
    ) -> dict[str, Any]:
        """
        Search for locations using fuzzy matching.

        Args:
            query: Location search term (name, code, zip, or district)
            location_type: Optional filter by location type
            top_k: Number of results to return (max 100)

        Returns:
            Dictionary with results or error information
        """
        # Validation
        error_response = self._validate_inputs(query, location_type)
        if error_response:
            return error_response

        # Clamp top_k
        top_k = max(1, min(top_k, 100))
        query_upper = query.strip().upper()

        try:
            search = self._build_search(query_upper, location_type, top_k)
            response = search.execute()
        except Exception as e:
            logger.error(f"OpenSearch query failed for query='{query}': {str(e)}", exc_info=True)
            return {"error": f"OpenSearch query failed: {str(e)}", "results": []}

        # Transform results
        results = self._transform_results(response)

        return {
            "results": results,
            "count": len(results),
            "query": query,
            "location_type": location_type
        }

    def _validate_inputs(self, query: str, location_type: str | None) -> dict[str, Any] | None:
        """Validate input parameters and return error dict if invalid, None otherwise."""
        if not query or not query.strip():
            return {"error": "Query cannot be empty", "results": []}

        if location_type and location_type not in self.LOCATION_TYPES:
            return {
                "error": f"Invalid location_type. Must be one of: {', '.join(sorted(self.LOCATION_TYPES))}",
                "results": []
            }

        return None

    def _build_search(
            self,
            query_upper: str,
            location_type: str | None,
            top_k: int
    ) -> LocationSearch:
        """Build the OpenSearch query with fuzzy matching."""
        # Enhanced search queries with better boosting strategy
        should_queries = [
            # Exact match gets highest boost
            Q("term", location__keyword={"value": query_upper, "boost": 10.0}),
            # Standard match for analyzed fields
            Q("match", location={"query": query_upper, "boost": 8.0}),
            # Fuzzy match for typo tolerance (AUTO: 0 edits for 1-2 chars, 1 edit for 3-5, 2 edits for >5)
            Q("match", location={"query": query_upper, "fuzziness": "AUTO", "boost": 5.0}),
            # Contains match for partial matches
            Q("match", location__contains={"query": query_upper, "boost": 3.0}),
            # Wildcard for prefix matching
            Q("wildcard", location__keyword={"value": f"{query_upper}*", "boost": 2.0}),
        ]

        search = LocationSearch()
        search = search.query("bool", should=should_queries, minimum_should_match=1)

        if location_type:
            search = search.filter("term", location_type=location_type)

        search = search[:top_k]
        search = search.source("location", "location_json", "location_type")

        return search

    def _transform_results(self, response: Any) -> list[dict[str, Any]]:
        """Transform OpenSearch hits to SelectedLocation format."""
        results = []
        seen_identifiers = set()

        for hit in response.hits:
            try:
                source = hit.to_dict()
                location_obj = self._transform_to_selected_location(
                    location=source.get("location", ""),
                    location_json=source.get("location_json", ""),
                    location_type=source.get("location_type", ""),
                    score=hit.meta.score,
                )

                identifier = location_obj["identifier"]

                # Skip duplicates (can happen with fuzzy matching)
                if identifier in seen_identifiers:
                    continue

                seen_identifiers.add(identifier)

                # Return in the format expected by selectedLocations
                results.append({
                    identifier: location_obj
                })

            except Exception as e:
                logger.warning(
                    f"Failed to transform location result: {str(e)}",
                    extra={"hit": source if 'source' in locals() else None},
                    exc_info=True
                )
                continue

        return results

    def _transform_to_selected_location(
            self,
            location: str,
            location_json: str,
            location_type: str,
            score: float
    ) -> dict[str, Any]:
        """
        Transform OpenSearch result to SelectedLocation format.

        Returns a dictionary with identifier, filter, and display objects.
        """
        try:
            location_data = json.loads(location_json) if location_json else {}
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse location_json: {e}")
            location_data = {}

        identifier = self._build_identifier(location_data, location_type)
        filter_obj = self._build_filter(location_data, location_type)
        display_obj = self._build_display(location_data, location_type, location)

        return {
            "identifier": identifier,
            "filter": filter_obj,
            "display": display_obj,
            "score": score,
        }

    def _build_identifier(self, data: dict[str, Any], location_type: str) -> str:
        """Build the identifier string based on location type."""
        match location_type:
            case "country":
                result = data.get("country_code", data.get("country_name", "UNKNOWN"))

            case "state":
                state_name = data.get("state_name", "")
                state_code = self._get_state_code(state_name)
                result = f"USA_{state_code}"

            case "city":
                country = data.get("country_name", "USA")
                country_code = self._get_country_code(country)
                state = data.get("state_name", "")
                state_code = self._get_state_code(state) if state else "undefined"
                city = data.get("city_name", "").replace(" ", "_")  # Normalize spaces
                result = f"{country_code}_{state_code}_{city}"

            case "county":
                state = data.get("state_name", "")
                state_code = self._get_state_code(state)
                county_fips = data.get("county_fips", "")
                result = f"USA_{state_code}_{county_fips}"

            case "zip_code":
                zip_code = data.get("zip_code", "")
                result = f"USA_{zip_code}"

            case "current_cd" | "original_cd":
                cd_key = "current_cd" if location_type == "current_cd" else "original_cd"
                cd = data.get(cd_key, "")
                if cd:
                    # Format: KS-03 -> USA_KS_03
                    result = f"USA_{cd.replace('-', '_')}"
                else:
                    result = "UNKNOWN"

            case _:
                result = "UNKNOWN"

        return result

    def _build_filter(self, data: dict[str, Any], location_type: str) -> dict[str, Any]:
        """Build the filter object for the location."""
        filter_obj = {}

        # Always include country
        country_name = data.get("country_name", "UNITED STATES")
        filter_obj["country"] = self._get_country_code(country_name)

        # Add type-specific fields (only non-None values)
        if data.get("state_name"):
            filter_obj["state"] = self._get_state_code(data["state_name"])

        if data.get("city_name"):
            filter_obj["city"] = data["city_name"]

        if data.get("county_fips"):
            filter_obj["county"] = data["county_fips"]

        if data.get("zip_code"):
            filter_obj["zip"] = data["zip_code"]

        # Handle congressional districts
        for cd_type, filter_key in [("current_cd", "district_current"), ("original_cd", "district_original")]:
            if data.get(cd_type):
                cd_parts = data[cd_type].split("-")
                if len(cd_parts) == 2:
                    filter_obj[filter_key] = cd_parts[1]

        return filter_obj

    def _build_display(
            self,
            data: dict[str, Any],
            location_type: str,
            full_location: str
    ) -> dict[str, str]:
        """Build the display object for UI."""
        # Determine standalone name
        standalone_map = {
            "city": data.get("city_name", ""),
            "state": data.get("state_name", ""),
            "county": data.get("county_name", ""),
            "zip_code": data.get("zip_code", ""),
            "current_cd": data.get("current_cd", ""),
            "original_cd": data.get("original_cd", ""),
            "country": data.get("country_name", ""),
        }

        standalone = standalone_map.get(location_type, full_location)
        entity = self.ENTITY_DISPLAY_MAP.get(location_type, "Location")

        return {
            "entity": entity,
            "standalone": standalone,
            "title": full_location,
        }

    def _get_state_code(self, state_name: str) -> str:
        """Convert state name to 2-letter code."""
        if not state_name:
            return ""

        # Case-insensitive lookup
        code = self.state_codes.get(state_name.lower())
        return code if code else state_name[:2].upper()

    def _get_country_code(self, country_name: str) -> str:
        """Convert country name to 3-letter code."""
        if not country_name:
            return "USA"

        # Case-insensitive lookup
        code = self.country_codes.get(country_name.lower())
        return code if code else country_name[:3].upper()


# Create the tool instance
lookup_location_tool = AITool(
    function=LocationLookupTool().lookup_location,
    logging=lambda tool_input: f"Searching the location index for '{tool_input.get('query', 'N/A')}'",
    description=AIToolDescription(
        name="lookup_location",
        description="""
                Search for valid location objects by name, code, zip, or congressional district
                using fuzzy matching. Returns properly formatted SelectedLocation objects ready
                to use in selectedLocations filter. The returned 'identifier' field should be
                used as the dictionary key in selectedLocations.

                This tool supports:
                - Country names and codes (e.g., 'USA', 'United States', 'Germany')
                - State names and codes (e.g., 'Texas', 'TX', 'Texa' with fuzzy match)
                - City names (e.g., 'Chicago', 'New York', 'Chicgo' with fuzzy match)
                - County names and FIPS codes
                - ZIP codes (e.g., '64198', '10001')
                - Congressional districts (e.g., 'MO-04', 'NY-12')

                The tool uses fuzzy matching to handle typos and variations in input.
                For best results, be as specific as possible with your query.

                Examples:
                - lookup_location('Texas')  Returns USA_TX state
                - lookup_location('Texa', 'state')  Returns USA_TX state (fuzzy match)
                - lookup_location('Chicago')  Returns USA_IL_CHICAGO city
                - lookup_location('64198')  Returns USA_64198 zip code
                - lookup_location('MO-04')  Returns USA_MO_04 congressional district
                - lookup_location('Kansas City', 'city')  Returns Kansas City, MO and KS results
                - lookup_location('Germany')  Returns DEU country

                USAGE NOTES:
                1. Always use the returned objects directly in selectedLocations
                2. The identifier is automatically included as the dictionary key
                3. Use location_type to filter results when query is ambiguous
                4. Results are ranked by relevance score
                """.strip(),
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "Location search term (name, code, zip, or district). "
                        "Supports fuzzy matching for typos."
                    ),
                },
                "location_type": {
                    "type": "string",
                    "enum": ["country", "state", "city", "county", "zip_code", "current_cd", "original_cd"],
                    "description": (
                        "Optional: Filter results by specific location type "
                        "to narrow down ambiguous queries"
                    ),
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results to return (default: 15, min: 1, max: 100)",
                    "default": 15,
                    "minimum": 1,
                    "maximum": 100,
                },
            },
            "required": ["query"],
        },
    ),
)
