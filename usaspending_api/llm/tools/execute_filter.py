import json
import logging

from pydantic import ValidationError

from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    AIToolResult,
    ExecuteFilterInput,
    FilterRequest,
    Filters,
)
from usaspending_api.llm.tools.exceptions import (
    AgencyNotRecognizedError,
    AwardAmountNotParseableError,
    AwardNotRecognizedError,
    DefcCodeNotRecognizedError,
    FilterValidationError,
    RecipientNotRecognizedError,
    TimePeriodNotParseableError,
)
from usaspending_api.llm.tools.lookup_agency import lookup_agency_tool
from usaspending_api.llm.tools.lookup_award import lookup_award_tool
from usaspending_api.llm.tools.lookup_recipient import lookup_recipient_tool
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
        message = f"DEFC code not found: {', '.join(codes)}."
    elif field == "awardAmounts":
        message = f"Unable to filter awards based on the criteria {kwargs.get('awardAmounts')}."
    else:
        message = "Unable to filter based on the specified time period. Try rephrasing the dates you're looking for."
    return message


async def _validate_agencies_exist(filters: Filters) -> None:
    """Confirm every selected agency is resolvable via lookup_agency."""
    invalid = []
    for field in ("selectedAwardingAgencies", "selectedFundingAgencies"):
        for agency in getattr(filters, field).values():
            query = agency.toptier_agency.toptier_code
            try:
                lookup_result = await lookup_agency_tool.function(query=query)
                results = lookup_result["results"]
            except AgencyNotRecognizedError:
                results = {}
            if f"{agency.id}_{agency.agencyType}" not in results:
                invalid.append(agency.toptier_agency.name)
    if invalid:
        raise AgencyNotRecognizedError(
            f"Unable to filter. These agencies could not be verified: {', '.join(invalid)}. "
            "Call lookup_agency for each and choose one of the returned agencies.",
            model_detail=f"Unresolvable selected agencies: {invalid}",
        )


async def _validate_recipients_exist(filters: Filters) -> None:
    """Confirm every selected recipient name is resolvable via lookup_recipient."""
    invalid = []
    for name in filters.selectedRecipients:
        result = await lookup_recipient_tool.function(query=name)
        found = {n.upper() for n in result.get("recipient_names", [])}
        if name.upper() not in found:
            invalid.append(name)
    if invalid:
        raise RecipientNotRecognizedError(
            f"Unable to filter. These recipients could not be verified: {', '.join(invalid)}. "
            "Call lookup_recipient for each and choose one of the returned recipient names.",
            model_detail=f"Unresolvable selected recipients: {invalid}",
        )


async def _validate_award_ids_exist(filters: Filters) -> None:
    """Confirm every selected award ID is resolvable via lookup_award, and replace each with the
    PIID/FAIN/URI the search award ID filter actually matches (a generated_unique_award_id matches nothing)."""
    invalid = []
    canonical = []
    for award_id in filters.selectedAwardIDs:
        try:
            lookup = await lookup_award_tool.function(query=award_id)
        except AwardNotRecognizedError:
            invalid.append(award_id)
            continue
        resolved = lookup.get("award_id") or award_id
        if resolved not in canonical:
            canonical.append(resolved)
    filters.selectedAwardIDs = canonical
    if invalid:
        raise AwardNotRecognizedError(
            f"Unable to filter. These award IDs could not be verified: {', '.join(invalid)}. "
            "Call lookup_award for each and confirm it exists before retrying.",
            model_detail=(
                f"Unresolvable selected award IDs: {invalid}. These may be recipient identifiers (UEI, DUNS) "
                "or names rather than award IDs -- call lookup_recipient with each and, if it matches, use "
                "selectedRecipients instead of selectedAwardIDs."
            ),
        )


def _filter_request_from_filters(filters: Filters) -> dict:
    """Canonicalize an already-validated Filters instance into the persisted filter request shape."""
    filter_request = FilterRequest(filters=filters).model_dump(exclude_none=True)
    if "keyword" in filter_request["filters"]:
        filter_request["filters"]["keyword"] = {v: v for v in filter_request["filters"]["keyword"]}
    if "selectedAwardIDs" in filter_request["filters"]:
        filter_request["filters"]["selectedAwardIDs"] = {v: v for v in filter_request["filters"]["selectedAwardIDs"]}

    return filter_request


def build_filter_request(filter_input: dict) -> dict:
    """
    Validate and canonicalize filter input before it is hashed or evaluated.

    ValidationError is intentionally propagated so callers can preserve their
    own error handling while sharing one canonical filter representation.
    """
    filters = Filters(**filter_input)
    return _filter_request_from_filters(filters)


async def execute_filter(**kwargs) -> AIToolResult:
    logger.info(f"Starting execute_filter with {len(kwargs)} filter parameter(s)", extra={"filter_count": len(kwargs)})

    try:
        filters = Filters(**kwargs)
    except ValidationError as exc:
        fields = {error["loc"][0] if error["loc"] else None for error in exc.errors()}
        if len(fields) == 1 and (field := next(iter(fields))) in _SINGLE_FIELD_ERRORS:
            raise _SINGLE_FIELD_ERRORS[field](_build_user_message(field, kwargs), model_detail=str(exc)) from exc
        if len(fields) > 1:
            raise FilterValidationError(
                "The input parameters are invalid. Look at the error message and try again.",
                model_detail=str(exc),
            ) from exc
        return AIToolResult(
            results={
                "error": str(exc),
                "message": "The input parameters are invalid. Look at the error message and try again.",
            }
        )

    await _validate_agencies_exist(filters)
    await _validate_recipients_exist(filters)
    await _validate_award_ids_exist(filters)

    filter_request = _filter_request_from_filters(filters)

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
            return AIToolResult(
                results={
                    "error": str(e),
                    "message": "There was an error saving the filter hash.  Look at the error message and try again.",
                }
            )

    logger.info(f"Execute filter completed successfully: hash={hash_key}", extra={"hash": hash_key})
    return AIToolResult(results={"hash": hash_key})


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
