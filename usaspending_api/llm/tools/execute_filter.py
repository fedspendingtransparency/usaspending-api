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


def execute_filter(**kwargs) -> dict[str, str]:
    logger.info(f"Starting execute_filter with {len(kwargs)} filter parameter(s)", extra={"filter_count": len(kwargs)})

    try:
        filters = Filters(**kwargs)
        logger.debug(f"Filter validation successful: {list(kwargs.keys())}")
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
            This tool selects filters for a USASspending advanced search.
            Use multiple filters if necessary to filter the results to the user's intent.
            Filters are combined with an AND operator.
            Awards will only appear if they meet all of the filter conditions.
        """,
        input_schema=Filters.model_json_schema(),
    ),
)
