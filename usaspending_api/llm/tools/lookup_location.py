import json
from typing import Any

from elasticsearch_dsl import Q

from usaspending_api.common.elasticsearch.search_wrappers import LocationSearch
from usaspending_api.llm.tools.reference import state_codes, country_codes
from usaspending_api.llm.models.py_models import AIToolDescription, AITool


class LocationLookupTool:
    """Tool for looking up locations in OpenSearch"""

    def lookup_location(self, query: str, location_type: str | None = None, top_k: int = 5) -> list[dict[str, Any]]:
        query_upper = query.upper()

        # Build search query leveraging your custom analyzers
        should_queries = [
            Q("match", location={"query": query_upper, "boost": 10.0}),
            Q("match", location__contains={"query": query_upper, "boost": 5.0}),
            Q("term", location__keyword={"value": query_upper, "boost": 8.0}),
        ]

        s = LocationSearch()

        s = s.query("bool", should=should_queries, minimum_should_match=1)

        if location_type:
            s = s.filter("term", location_type=location_type)

        s = s[:top_k]
        s = s.source(["location", "location_json", "location_type"])

        # Execute search
        try:
            response = s.execute()
        except Exception as e:
            return {"error": f"OpenSearch query failed: {str(e)}", "results": []}

        # Transform results to SelectedLocation format
        results = []
        for hit in response["hits"]["hits"]:
            try:
                location_obj = self._transform_to_selected_location(
                    hit["_source"]["location"],
                    hit["_source"]["location_json"],
                    hit["_source"]["location_type"],
                    hit["_score"],
                )
                results.append(location_obj)
            except Exception as e:
                # Skip malformed results
                continue
        return results

    def _transform_to_selected_location(
        self, location: str, location_json: str, location_type: str, score: float
    ) -> dict[str, Any]:
        """
        Transform OpenSearch result to SelectedLocation format.

        Parses location_json and constructs the identifier, filter, and display objects.
        """
        # Parse the JSON string
        location_data = json.loads(location_json)

        # Build identifier based on location type
        identifier = self._build_identifier(location_data, location_type)

        # Build filter object
        filter_obj = self._build_filter(location_data, location_type)

        # Build display object
        display_obj = self._build_display(location_data, location_type, location)

        return {"identifier": identifier, "filter": filter_obj, "display": display_obj, "score": score}

    def _build_identifier(self, data: dict, location_type: str) -> str:
        """Build the identifier string based on location type"""
        if location_type == "country":
            return data.get("country_code", data.get("country_name", "UNKNOWN"))

        elif location_type == "state":
            # Extract state code from state_name if available
            # Assuming format like "TEXAS" or you have state_code in data
            state_name = data.get("state_name", "")
            # You might need a lookup table for state name -> code
            # For now, using a simple approach
            return f"USA_{self._get_state_code(state_name)}"

        elif location_type == "city":
            country = data.get("country_name", "USA")
            country_code = self._get_country_code(country)
            state = data.get("state_name", "undefined")
            state_code = self._get_state_code(state) if state != "undefined" else "undefined"
            city = data.get("city_name", "")
            return f"{country_code}_{state_code}_{city}"

        elif location_type == "county":
            state = data.get("state_name", "")
            state_code = self._get_state_code(state)
            county_fips = data.get("county_fips", "")
            return f"USA_{state_code}_{county_fips}"

        elif location_type == "zip_code":
            zip_code = data.get("zip_code", "")
            return f"USA_{zip_code}"

        elif location_type == "current_cd":
            cd = data.get("current_cd", "")
            return f"USA_{cd.replace('-', '_')}"

        elif location_type == "original_cd":
            cd = data.get("original_cd", "")
            return f"USA_{cd.replace('-', '_')}"

        return "UNKNOWN"

    def _build_filter(self, data: dict, location_type: str) -> dict[str, Any]:
        """Build the filter object for the location"""
        filter_obj = {}

        # Always include country
        country_name = data.get("country_name", "UNITED STATES")
        filter_obj["country"] = self._get_country_code(country_name)

        # Add type-specific fields
        if "state_name" in data and data["state_name"]:
            filter_obj["state"] = self._get_state_code(data["state_name"])

        if "city_name" in data and data["city_name"]:
            filter_obj["city"] = data["city_name"]

        if "county_fips" in data and data["county_fips"]:
            filter_obj["county"] = data["county_fips"]

        if "zip_code" in data and data["zip_code"]:
            filter_obj["zip"] = data["zip_code"]

        if "current_cd" in data and data["current_cd"]:
            # Extract just the district number (e.g., "KS-03" -> "03")
            cd_parts = data["current_cd"].split("-")
            if len(cd_parts) == 2:
                filter_obj["district_current"] = cd_parts[1]

        if "original_cd" in data and data["original_cd"]:
            cd_parts = data["original_cd"].split("-")
            if len(cd_parts) == 2:
                filter_obj["district_original"] = cd_parts[1]

        return filter_obj

    @staticmethod
    def _build_display(data: dict, location_type: str, full_location: str) -> dict[str, str]:
        """Build the display object for UI"""
        entity_map = {
            "country": "Country",
            "state": "State",
            "city": "City",
            "county": "County",
            "zip_code": "Zip code",
            "current_cd": "Current congressional district",
            "original_cd": "Original congressional district",
        }

        # Determine standalone name
        if location_type == "city":
            standalone = data.get("city_name", "")
        elif location_type == "state":
            standalone = data.get("state_name", "")
        elif location_type == "county":
            standalone = data.get("county_name", "")
        elif location_type == "zip_code":
            standalone = data.get("zip_code", "")
        elif location_type in ["current_cd", "original_cd"]:
            cd_key = "current_cd" if location_type == "current_cd" else "original_cd"
            standalone = data.get(cd_key, "")
        elif location_type == "country":
            standalone = data.get("country_name", "")
        else:
            standalone = full_location

        return {"entity": entity_map.get(location_type, "Location"), "standalone": standalone, "title": full_location}

    @staticmethod
    def _get_state_code(state_name: str) -> str:
        """Convert state name to 2-letter code"""
        return state_codes.get(state_name.title(), state_name[:2].upper())

    @staticmethod
    def _get_country_code(country_name: str) -> str:
        """Convert country name to 3-letter code"""
        country_code = [code for code in country_codes if code["name"].lower() == country_name.lower()]
        return country_code[0]["code"] if country_codes else country_name[:3].upper()


lookup_location_tool = AITool(
    function=LocationLookupTool().lookup_location,
    logging=lambda tool_input: f"📍 searching the location index for {tool_input['query']}",
    description=AIToolDescription(
        name="lookup_location",
        description="""
            "Search for valid location objects by name, code, zip, or congressional district. "
                "Returns properly formatted SelectedLocation objects ready to use in selectedLocations filter. "
                "The returned 'identifier' field should be used as the dictionary key in selectedLocations.\n\n"
                "Examples:\n"
                "- lookup_location('Texas') → Returns USA_TX state\n"
                "- lookup_location('Chicago') → Returns USA_IL_CHICAGO city\n"
                "- lookup_location('66208') → Returns USA_66208 zip code\n"
                "- lookup_location('KS-03') → Returns USA_KS_03 congressional district"
        """,
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Location search term (name, code, zip, or district)"},
                "location_type": {
                    "type": "string",
                    "enum": ["country", "state", "city", "county", "zip_code", "current_cd", "original_cd"],
                    "description": "Optional: Filter results by location type",
                },
                "top_k": {"type": "integer", "description": "Number of results to return (default: 15)", "default": 15},
            },
            "required": ["query"],
        },
    ),
)
