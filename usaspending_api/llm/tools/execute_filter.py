import json
import logging

from pydantic import ValidationError

from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    FilterRequest,
    Filters,
)
from usaspending_api.references.helpers import create_hash
from usaspending_api.references.models import FilterHash

logger = logging.getLogger(__name__)


def build_filter_request(filter_input: dict) -> dict:
    """
    Function added to separate the execution of the filter from the construction of the filter request so the eval
    framework can compare the same normalized filter payload used to produce the production hash.
    """
    try:
        filters = Filters(**filter_input)
    except ValidationError as exc:
        return {
            "error": str(exc),
            "message": "The input parameters are invalid. Look at the error message and try again.",
        }

    filter_request = FilterRequest(filters=filters).model_dump(exclude_none=True)

    if "keyword" in filter_request["filters"]:
        filter_request["filters"]["keyword"] = {v: v for v in filter_request["filters"]["keyword"]}

    return filter_request


def execute_filter(**kwargs) -> dict[str, str]:
    logger.info(f"Starting execute_filter with {len(kwargs)} filter parameter(s)", extra={"filter_count": len(kwargs)})

    filter_request = build_filter_request(kwargs)
    logger.debug(f"Filter validation successful: {list(kwargs.keys())}")

    filter_json = json.dumps(filter_request, sort_keys=True)
    hash_key = create_hash(filter_json.encode("utf-8"))
    logger.info(f"Generated filter hash: {hash_key}", extra={"hash": hash_key, "filter_keys": list(kwargs.keys())})

    try:
        FilterHash.objects.get(hash=hash_key)
        logger.info(f"Filter hash already exists in database: {hash_key}", extra={"hash": hash_key, "is_new": False})
    except FilterHash.DoesNotExist:
        logger.info(f"Filter hash not found, creating new entry: {hash_key}", extra={"hash": hash_key, "is_new": True})
        try:
            fh = FilterHash(hash=hash_key, filter=filter_request)
            fh.save()
            logger.info(f"Successfully saved new filter hash: {hash_key}", extra={"hash": hash_key})
        except Exception as e:
            return {
                "error": str(e),
                "message": "There was an error saving the filter hash.  Look at the error message and try again.",
            }

    logger.info(f"Execute filter completed successfully: hash={hash_key}", extra={"hash": hash_key})
    return {"hash": hash_key}


execute_filter_tool = AITool(
    function=execute_filter,
    logging=lambda tool_input: (
        "Selecting filters:\n" + "\n".join([f"    - {_filter}: {_value}" for _filter, _value in tool_input.items()])
    ),
    description=AIToolDescription(
        name="execute_filter",
        description="""
            This tool selects filters for a USAspending advanced search.
            Use multiple filters if necessary to filter the results to the user's intent.
            Filters are combined with an AND operator.
            Awards will only appear if they meet all of the filter conditions.
        """,
        input_schema=Filters.model_json_schema(),
    ),
)
