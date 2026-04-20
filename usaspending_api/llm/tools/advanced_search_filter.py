import requests

from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    FilterRequest,
    Filters,
    SelectedLocation,
    LocationFilter,
    LocationDisplay,
)


def advanced_search(**kwargs):
    if "selectedLocations" in kwargs:
        converted_locations = {}
        for key, value in kwargs["selectedLocations"].items():
            # If it's a simple dict like {'state': 'TX'}, convert it
            if isinstance(value, dict) and "identifier" not in value:
                state = value.get("state", "")
                country = value.get("country", "USA")
                identifier = f"{country}_{state}" if state else country

                converted_locations[identifier] = SelectedLocation(
                    identifier=identifier,
                    filter=LocationFilter(country=country, state=state if state else None),
                    display=LocationDisplay(
                        entity="State" if state else "Country",
                        standalone=state if state else country,
                        title=state if state else country,
                    ),
                )
            else:
                # Already in correct format
                converted_locations[key] = value

        kwargs["selectedLocations"] = converted_locations

    filters = Filters(**kwargs)
    filter_request = FilterRequest(filters=filters)
    response = requests.post(
        "https://api.usaspending.gov/api/v2/references/filter/", json=filter_request.model_dump(exclude_none=True)
    )

    return response.json()


create_advanced_search_filter = AITool(
    function=advanced_search,
    logging=lambda tool_input: (
        "🔍 creating filter hash with parameters:\n"
        + "\n".join([f"    - {_filter}: {_value}" for _filter, _value in tool_input.items()])
    ),
    description=AIToolDescription(
        name="search_federal_contracts_and_assistance",
        description="""
            This tool performs a search of the USASpending website for federal contracts and assistance.
            Use the keywords parameter to search for spending related to specific topics.
            If the search requires a selected locations parameter you MUST first use the 
            `get_valid_location_object` tool to get a valid location object.  Use this to populate each selected location filter property.
            Use as many parameters as necessary to filter the results to the user's intent.
            This tool return a list of federal contracts and awards filtered by the input parameters.
        """,
        input_schema=Filters.model_json_schema(),
    ),
)
