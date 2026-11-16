import json
import logging
from typing import Any

from asgiref.sync import sync_to_async
from pydantic import ValidationError

from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException
from usaspending_api.llm.models.py_models import (
    AITool,
    AIToolDescription,
    AIToolResult,
    ExecuteFilterInput,
    FilterRequest,
    Filters,
    SelectedAgency,
)
from usaspending_api.llm.tools.exceptions import (
    FINAL_TOOL_RETRY_INSTRUCTIONS,
    AgencyNotRecognizedError,
    AwardAmountNotParseableError,
    AwardNotRecognizedError,
    DefcCodeNotRecognizedError,
    FilterValidationError,
    RecipientNotRecognizedError,
    TimePeriodNotParseableError,
    ToolUseRuntimeError,
)
from usaspending_api.llm.tools.helpers.search_filters import validate_for_search_api
from usaspending_api.llm.tools.lookup_award import lookup_award_tool
from usaspending_api.llm.tools.lookup_recipient import lookup_recipient_tool
from usaspending_api.references.helpers import create_hash
from usaspending_api.references.models import FilterHash
from usaspending_api.references.models.agency import Agency
from usaspending_api.search.models.mv_agency_autocomplete import AgencyAutocompleteMatview

logger = logging.getLogger(__name__)

_SINGLE_FIELD_ERRORS = {
    "defCodes": DefcCodeNotRecognizedError,
    "awardAmounts": AwardAmountNotParseableError,
    "time_period": TimePeriodNotParseableError,
}

GENERIC_USER_MESSAGE = "Failed to select filters."
LOOKUP_OBJECT_INSTRUCTIONS = (
    "Selected locations, agencies, recipients, and award IDs must be passed exactly as returned by their lookup "
    "tool, unchanged. Do not construct them yourself; call the lookup tool again if you no longer have the result."
)
LOOKUP_OBJECT_FIELDS = {
    "selectedLocations",
    "selectedAwardingAgencies",
    "selectedFundingAgencies",
    "selectedRecipients",
    "selectedAwardIDs",
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


async def _agency_exists(agency: SelectedAgency) -> bool:
    """Whether the selected agency id exists with the stated type under the stated toptier agency.

    Checked directly by id: lookup_agencies' exact/prefix path returns ids from the agency autocomplete matview and its
    semantic path returns agency-table ids, so a selection is valid if either source has it.
    """
    is_toptier = agency.agencyType == "toptier"
    toptier_agency_id = agency.toptier_agency.id
    if await AgencyAutocompleteMatview.objects.filter(
        agency_autocomplete_id=agency.id, toptier_flag=is_toptier, toptier_agency_id=toptier_agency_id
    ).aexists():
        return True
    return await Agency.objects.filter(
        id=agency.id, toptier_flag=is_toptier, toptier_agency_id=toptier_agency_id
    ).aexists()


async def _validate_agencies_exist(filters: Filters) -> None:
    """Confirm every selected agency is a real agency selection as returned by lookup_agencies."""
    invalid = []
    for field in ("selectedAwardingAgencies", "selectedFundingAgencies"):
        for agency in getattr(filters, field).values():
            if not await _agency_exists(agency):
                invalid.append(agency.subtier_agency.name if agency.subtier_agency else agency.toptier_agency.name)
    if invalid:
        raise AgencyNotRecognizedError(
            f"Unable to filter. These agencies could not be verified: {', '.join(invalid)}.",
            model_detail=(
                f"Unresolvable selected agencies: {invalid}. Use the agency entries exactly as returned by "
                "lookup_agencies (same id, agencyType, and toptier_agency)."
            ),
            instructions=f"{LOOKUP_OBJECT_INSTRUCTIONS} {FINAL_TOOL_RETRY_INSTRUCTIONS}",
        )


async def _validate_recipients_exist(filters: Filters) -> None:
    """Confirm every selected recipient name is resolvable via lookup_recipient."""
    invalid = []
    for name in filters.selectedRecipients:
        result = await lookup_recipient_tool.function(query=name)
        found = {n.upper() for n in result.results.get("selectedRecipients", [])}
        if name.upper() not in found:
            invalid.append(name)
    if invalid:
        raise RecipientNotRecognizedError(
            f"Unable to filter. These recipients could not be verified: {', '.join(invalid)}. "
            "Call lookup_recipient for each and choose one of the returned recipient names.",
            model_detail=f"Unresolvable selected recipients: {invalid}",
            instructions=(
                "Call lookup_recipient for each unresolved name and use one of the returned recipient names exactly. "
                f"{FINAL_TOOL_RETRY_INSTRUCTIONS}"
            ),
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
        resolved = lookup.results.get("award_id") or award_id
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
            instructions=FINAL_TOOL_RETRY_INSTRUCTIONS,
        )


def _drop_none_values(value: Any) -> Any:
    """Remove dict entries whose value is None, at any depth. List items are kept, since a None can be meaningful there
    (an open-ended award amount range is [1000000, None]).

    The website forwards the saved filters to the search endpoints as they are, and they reject None values, e.g. a
    location with "zip": null. Typed fields already omit None; this covers fields typed as Any.
    """
    if isinstance(value, dict):
        return {key: _drop_none_values(item) for key, item in value.items() if item is not None}
    if isinstance(value, list):
        return [_drop_none_values(item) for item in value]
    return value


def _filter_request_from_filters(filters: Filters) -> dict:
    """Canonicalize an already-validated Filters instance into the persisted filter request shape."""
    filter_request = _drop_none_values(FilterRequest(filters=filters).model_dump(exclude_none=True))
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


def _has_no_filters(filters: Filters) -> bool:
    """Whether nothing was selected, i.e. every field still holds its default value."""
    return filters.model_dump() == Filters().model_dump()


def _validate_search_api_accepts(filter_request: dict) -> None:
    """The search endpoints apply their own rules to saved filters; reject what they would so the model can fix it."""
    try:
        validate_for_search_api(filter_request)
    except (InvalidParameterException, UnprocessableEntityException) as exc:
        reason = str(exc.detail)
        raise FilterValidationError(
            "Unable to filter. One of the selected filters is not valid for search.",
            model_detail=f"The search would reject these filters: {reason}",
            instructions=f"Fix the filter described in 'error' (for example, a keyword must be at least 3 characters "
            f"long, so use a complete phrase instead of an abbreviation). {FINAL_TOOL_RETRY_INSTRUCTIONS}",
        ) from exc


async def validate_filters(**kwargs) -> tuple[Filters, dict]:
    """Validate the model's filter input and return the Filters plus the canonical persisted filter request."""
    try:
        # Validating and dumping the filters use the synchronous ORM, so keep both off the event loop
        filters = await sync_to_async(Filters)(**kwargs)
    except ValidationError as exc:
        fields = {error["loc"][0] if error["loc"] else None for error in exc.errors()}
        model_detail = str(exc)
        instructions = FINAL_TOOL_RETRY_INSTRUCTIONS
        if fields & LOOKUP_OBJECT_FIELDS:
            instructions = f"{LOOKUP_OBJECT_INSTRUCTIONS} {instructions}"
        if len(fields) == 1 and (field := next(iter(fields))) in _SINGLE_FIELD_ERRORS:
            raise _SINGLE_FIELD_ERRORS[field](
                _build_user_message(field, kwargs), model_detail=model_detail, instructions=instructions
            ) from exc
        raise FilterValidationError(GENERIC_USER_MESSAGE, model_detail=model_detail, instructions=instructions) from exc

    # An empty filter set would save a hash that matches every award, which the website cannot display.
    if await sync_to_async(_has_no_filters)(filters):
        raise FilterValidationError(
            "Unable to filter. No filters were selected.",
            model_detail="execute_filter was called without any filters.",
            instructions=(
                "Select at least one filter that matches the user's query. If nothing else applies, use a keyword "
                f"taken from the query. {FINAL_TOOL_RETRY_INSTRUCTIONS}"
            ),
        )

    await _validate_agencies_exist(filters)
    await _validate_recipients_exist(filters)
    await _validate_award_ids_exist(filters)

    filter_request = await sync_to_async(_filter_request_from_filters)(filters)
    # The search API's validator reads reference data (such as the valid DEFC codes) with the synchronous ORM
    await sync_to_async(_validate_search_api_accepts)(filter_request)

    logger.debug(f"Filter validation successful: {list(kwargs.keys())}")
    return filters, filter_request


async def save_filter_request(filter_request: dict) -> str:
    """Save the canonical filter request (if it is new) and return its hash."""
    filter_json = json.dumps(filter_request, sort_keys=True)
    hash_key = create_hash(filter_json.encode("utf-8"))
    logger.info(
        f"Generated filter hash: {hash_key}",
        extra={"hash": hash_key, "filter_keys": list(filter_request.get("filters", {}).keys())},
    )

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
            raise ToolUseRuntimeError(
                "There was an error saving the search filters. Please try again.",
                model_detail=f"There was an error saving the filter hash: {e}",
                instructions="Call execute_filter again with the same input.",
            ) from e

    return hash_key


async def execute_filter(**kwargs) -> AIToolResult:
    logger.info(f"Starting execute_filter with {len(kwargs)} filter parameter(s)", extra={"filter_count": len(kwargs)})

    _, filter_request = await validate_filters(**kwargs)
    hash_key = await save_filter_request(filter_request)

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
