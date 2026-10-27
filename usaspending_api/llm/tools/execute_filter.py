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
    InvalidFilterFieldError,
    TimePeriodNotParseableError,
)
from usaspending_api.references.helpers import create_hash
from usaspending_api.references.models import FilterHash

logger = logging.getLogger(__name__)

_SINGLE_FIELD_ERRORS = {
    "defCodes": DefcCodeNotRecognizedError,
    "awardAmounts": AwardAmountNotParseableError,
    "time_period": TimePeriodNotParseableError,
    "awardType": InvalidFilterFieldError,
    "selectedAwardingAgencies": InvalidFilterFieldError,
    "selectedFundingAgencies": InvalidFilterFieldError,
}

_AGENCY_FIELD_HINT = (
    "Each entry must be the complete, unmodified object returned by lookup_agencies -- including the "
    "full nested toptier_agency (id, toptier_code, abbreviation, name). Don't hand-construct or "
    "partially edit an agency entry. Call lookup_agencies again and use its result object as-is."
)

_USER_MESSAGES = {
    "time_period": "Unable to filter based on the specified time period. Try rephrasing the dates you're looking for.",
    "awardType": "Unable to filter based on the specified award type.",
    "selectedAwardingAgencies": "Unable to filter based on the specified agency.",
    "selectedFundingAgencies": "Unable to filter based on the specified agency.",
}

_MODEL_DETAIL_HINTS = {
    "defCodes": (
        "Check the defCodes field description for the valid DEFC code list and retry with a correct "
        "code, or omit the DEFC filter if none apply."
    ),
    "awardAmounts": (
        "awardAmounts entries must have exactly one key ('specific' or a single 'range-N') -- see the "
        "awardAmounts field description. Retry with a single key, or omit this filter."
    ),
    "time_period": (
        "Use ISO date format YYYY-MM-DD for time_period, and only when timePeriodType='dr'. When "
        "timePeriodType='fy', use timePeriodFY with four-digit year strings instead."
    ),
    "awardType": (
        "awardType must be one of the fixed award-type codes listed in the execute_filter tool schema's "
        "awardType field description (grouped by contracts, IDVs, grants, loans, direct payments, and "
        "other financial assistance). It is unrelated to PSC, NAICS, CFDA, or TAS codes returned by "
        "lookup_codes -- never populate awardType from a lookup_codes result. Check the schema "
        "description for the exact valid codes and retry with a correct one, or omit awardType if none "
        "apply."
    ),
    "selectedAwardingAgencies": _AGENCY_FIELD_HINT,
    "selectedFundingAgencies": _AGENCY_FIELD_HINT,
}


def _build_user_message(field: str, kwargs: dict) -> str:
    if field == "defCodes":
        def_codes = kwargs.get("defCodes", {})
        codes = [*def_codes.get("require", []), *def_codes.get("exclude", [])]
        return f"DEFC code not found: {', '.join(codes)}."
    if field == "awardAmounts":
        return f"Unable to filter awards based on the criteria {kwargs.get('awardAmounts')}."
    return _USER_MESSAGES[field]


def _build_model_detail(field: str, exc: ValidationError) -> str:
    return f"{exc} {_MODEL_DETAIL_HINTS[field]}"


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
            raise _SINGLE_FIELD_ERRORS[field](
                _build_user_message(field, kwargs), model_detail=_build_model_detail(field, exc)
            ) from exc
        return {
            "error": str(exc),
            "message": (
                "The input parameters are invalid. Look at the error message, fix the specific field(s) "
                "called out, and call execute_filter again -- or drop the problematic field and retry "
                "with the filters you're confident about rather than ending the search without a "
                "successful execute_filter call."
            ),
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
