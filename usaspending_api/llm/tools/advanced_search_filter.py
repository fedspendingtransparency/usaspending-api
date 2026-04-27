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
    filters = Filters(**kwargs)
    filter_request = FilterRequest(filters=filters).model_dump(exclude_none=True)
    if "keyword" in filter_request["filters"]:
        updated_keyword = {v: v for v in filter_request["filters"]["keyword"]}
        filter_request["filters"]["keyword"] = updated_keyword
    response = requests.post("http://usaspending-manage:9000/api/v2/references/filter/", json=filter_request)
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
            `lookup_location` tool to get a valid location object.  Use this to populate each selected location filter property.
            Use as many parameters as necessary to filter the results to the user's intent.
            This tool return a list of federal contracts and awards filtered by the input parameters.
        """,
        input_schema=Filters.model_json_schema(),
    ),
)
