import requests

from usaspending_api.llm.tools.reference import state_codes, country_codes
from usaspending_api.llm.models.py_models import AIToolDescription, AITool


def location_search(search_text: str):
    response = requests.post(
        "https://api.usaspending.gov/api/v2/autocomplete/location/", data={"search_text": search_text, "limit": 20}
    )
    results = {
        geo: [
            {
                **result,
                "country_name": [
                    country["code"] for country in country_codes if country["name"] == result["country_name"]
                ][0],
                "state_name": state_codes.get(result.get("state_name", "").title()),
            }
            for result in results
        ]
        for geo, results in response.json()["results"].items()
    }
    return results


search_for_location = AITool(
    function=location_search,
    logging=lambda tool_input: f"📍 searching the location index for {tool_input['search_text']}",
    description=AIToolDescription(
        name="get_valid_location_object",
        description="""
            Search the location index for a location. Do not include keywords, only the location name.
            Do not search for abbreviations for place names; always include the full place name, (Good: Texas) (Bad: TX).
            This function will return up to 20 location objects that can be used in the `place_of_performance_locations` parameter of the `search_federal_contracts_and_assistance` function.
            Select the object from the list of location objects that best matches the user's intent. 
            Use this location object as an input to the list of `place_of_performance_locations` parameter of the `search_federal_contracts_and_assistance` tool.
            You will use this location to get further details about the federal spending in the selected location in a subsequent tool call.
        """,
        input_schema={
            "type": "object",
            "properties": {
                "search_text": {"type": "string"},
            },
            "required": ["search_text"],
        },
    ),
)
