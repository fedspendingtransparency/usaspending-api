import requests

from usaspending_api.llm.models.py_models import AITool, AIToolDescription, AdvancedSearchFilter


def advanced_search(**kwargs):
    validated_filter = AdvancedSearchFilter(**kwargs).model_dump(exclude_none=True)
    response = requests.post(
        "https://api.usaspending.gov/api/v2/search/spending_by_award/",
        json={
            "filters": {"award_type_codes": ["A", "B", "C", "D"], **validated_filter},
            "fields": [
                "Award ID",
                "Recipient Name",
                "Start Date",
                "End Date",
                "Award Amount",
                "Awarding Agency",
                "Awarding Sub Agency",
                "Contract Award Type",
                "Award Type",
                "Funding Agency",
                "Funding Sub Agency",
                "Primary Place of Performance",
            ],
            "spending_level": "awards",
            "limit": 10,
            "order": "desc",
            "sort": "Award Amount",
        },
    )
    return response.json()


create_advanced_search_filter = AITool(
    function=advanced_search,
    logging=lambda tool_input: (
        "🔍 selecting filters:\n" + "\n".join([f"    - {_filter}: {_value}" for _filter, _value in tool_input.items()])
    ),
    description=AIToolDescription(
        name="search_federal_contracts_and_assistance",
        description="""
            This tool performs a search of the USASpending website for federal contracts and assistance.
            Use the keywords parameter to search for spending related to specific topics.
            If the search requires a place_of_performance_location parameter you MUST first use the `get_valid_location_object` tool to get a valid location object.
            Use as many parameters as necessary to filter the results to the user's intent.
            This tool return a list of federal contracts and awards filtered by the input parameters.
        """,
        input_schema=AdvancedSearchFilter.model_json_schema(),
    ),
)
