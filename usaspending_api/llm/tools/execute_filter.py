import json
import logging

from pydantic import ValidationError

from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    ExecuteFilterInput,
    FilterRequest,
    Filters,
)
from usaspending_api.llm.tools.exceptions import (
    AwardAmountNotParseableError,
    DefcCodeNotRecognizedError,
    TimePeriodNotParseableError,
)
from usaspending_api.references.helpers import create_hash
from usaspending_api.references.models import FilterHash

logger = logging.getLogger(__name__)

_SINGLE_FIELD_ERRORS = {
    "defCodes": DefcCodeNotRecognizedError,
    "awardAmounts": AwardAmountNotParseableError,
    "time_period": TimePeriodNotParseableError,
}


def _build_user_message(field: str, kwargs: dict) -> str:
    if field == "defCodes":
        def_codes = kwargs.get("defCodes", {})
        codes = [*def_codes.get("require", []), *def_codes.get("exclude", [])]
        return f"DEFC code not found: {', '.join(codes)}."
    if field == "awardAmounts":
        return f"Unable to filter awards based on the criteria {kwargs.get('awardAmounts')}."
    return "Unable to filter based on the specified time period. Try rephrasing the dates you're looking for."


def build_filter_request(filter_input: dict) -> dict:
    """
    Validate and canonicalize filter input before it is hashed or evaluated.

    ValidationError is intentionally propagated so callers can preserve their
    own error handling while sharing one canonical filter representation.
    """
    filters = Filters(**filter_input)
    filter_request = FilterRequest(filters=filters).model_dump(exclude_none=True)
    if "keyword" in filter_request["filters"]:
        filter_request["filters"]["keyword"] = {v: v for v in filter_request["filters"]["keyword"]}
    if "selectedAwardIDs" in filter_request["filters"]:
        filter_request["filters"]["selectedAwardIDs"] = {v: v for v in filter_request["filters"]["selectedAwardIDs"]}

    return filter_request


async def execute_filter(**kwargs) -> dict[str, str]:
    logger.info(f"Starting execute_filter with {len(kwargs)} filter parameter(s)", extra={"filter_count": len(kwargs)})

    try:
        filter_request = build_filter_request(kwargs)
    except ValidationError as exc:
        fields = {error["loc"][0] if error["loc"] else None for error in exc.errors()}
        if len(fields) == 1 and (field := next(iter(fields))) in _SINGLE_FIELD_ERRORS:
            raise _SINGLE_FIELD_ERRORS[field](_build_user_message(field, kwargs), model_detail=str(exc)) from exc
        return {
            "error": str(exc),
            "message": "The input parameters are invalid. Look at the error message and try again.",
        }

    logger.debug(f"Filter validation successful: {list(kwargs.keys())}")

    filter_json = json.dumps(filter_request, sort_keys=True)
    hash_key = create_hash(filter_json.encode("utf-8"))
    logger.info(f"Generated filter hash: {hash_key}", extra={"hash": hash_key, "filter_keys": list(kwargs.keys())})

    try:
        await FilterHash.objects.aget(hash=hash_key)
        logger.info(f"Filter hash already exists in database: {hash_key}", extra={"hash": hash_key, "is_new": False})
    except FilterHash.DoesNotExist:
        logger.info(f"Filter hash not found, creating new entry: {hash_key}", extra={"hash": hash_key, "is_new": True})
        try:
            fh = FilterHash(hash=hash_key, filter=filter_request)
            await fh.asave()
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
    logging=lambda _: "Selecting filters",
    description=AIToolDescription(
        name="execute_filter",
        description="""
            This tool selects filters for a USAspending advanced search.
            Use multiple filters if necessary to filter the results to the user's intent.
            Filters are combined with an AND operator.
            Awards will only appear if they meet all of the filter conditions.
        """,
        input_schema=ExecuteFilterInput.model_json_schema(),
    ),
)
