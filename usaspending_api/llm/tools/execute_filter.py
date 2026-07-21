import json

from pydantic import ValidationError

from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    FilterRequest,
    Filters,
)
from usaspending_api.references.helpers import create_hash
from usaspending_api.references.models import FilterHash


def execute_filter(**kwargs) -> dict[str, str]:
    try:
        filters = Filters(**kwargs)
    except ValidationError as e:
        return {
            "error": str(e),
            "message": "The input parameters are invalid.  Look at the error message and try again.",
        }
    filter_request = FilterRequest(filters=filters).model_dump(exclude_none=True)
    if "keyword" in filter_request["filters"]:
        updated_keyword = {v: v for v in filter_request["filters"]["keyword"]}
        filter_request["filters"]["keyword"] = updated_keyword
    filter_json = json.dumps(filter_request, sort_keys=True)
    hash_key = create_hash(filter_json.encode("utf-8"))
    try:
        FilterHash.objects.get(hash=hash_key)
    except FilterHash.DoesNotExist:
        try:
            fh = FilterHash(hash=hash_key, filter=json.dumps(filter_request))
            fh.save()
        except Exception as e:
            return {
                "error": str(e),
                "message": "There was an error saving the filter hash.  Look at the error message and try again.",
            }
    return {"hash": hash_key}


execute_filter_tool = AITool(
    function=execute_filter,
    logging=lambda tool_input: (
        "Selecting filters:\n" + "\n".join([f"    - {_filter}: {_value}" for _filter, _value in tool_input.items()])
    ),
    description=AIToolDescription(
        name="execute_filter",
        description="""
            This tool selects filters for a USASspending advanced search.
            Use multiple filters if necessary to filter the results to the user's intent.
            Filters are combined with an AND operator.
            Awards will only appear if they meet all of the filter conditions.
        """,
        input_schema=Filters.model_json_schema(),
    ),
)
