import json
import logging

from asgiref.sync import sync_to_async
from pydantic import ValidationError

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
    AgencyNotRecognizedError,
    AwardAmountNotParseableError,
    AwardNotRecognizedError,
    DefcCodeNotRecognizedError,
    FilterValidationError,
    RecipientNotRecognizedError,
    TimePeriodNotParseableError,
    ToolUseRuntimeError,
)
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
RETRY_INSTRUCTION = "Fix the input based on the error above and call execute_filter again."


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
                "lookup_agencies (same id, agencyType, and toptier_agency); call lookup_agencies again if needed."
            ),
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
        # Validating and dumping the filters use the synchronous ORM, so keep both off the event loop
        filters = await sync_to_async(Filters)(**kwargs)
    except ValidationError as exc:
        fields = {error["loc"][0] if error["loc"] else None for error in exc.errors()}
        model_detail = f"{exc}\n\n{RETRY_INSTRUCTION}"
        if len(fields) == 1 and (field := next(iter(fields))) in _SINGLE_FIELD_ERRORS:
            raise _SINGLE_FIELD_ERRORS[field](_build_user_message(field, kwargs), model_detail=model_detail) from exc
        raise FilterValidationError(GENERIC_USER_MESSAGE, model_detail=model_detail) from exc

    await _validate_agencies_exist(filters)
    await _validate_recipients_exist(filters)
    await _validate_award_ids_exist(filters)

    filter_request = await sync_to_async(_filter_request_from_filters)(filters)

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
            raise ToolUseRuntimeError(
                "There was an error saving the search filters. Please try again.",
                model_detail=f"There was an error saving the filter hash: {e}",
            ) from e

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
