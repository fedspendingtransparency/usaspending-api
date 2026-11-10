RETRY_INSTRUCTIONS = (
    "Fix the specific issue described in 'error' and call the tool again. Do not repeat the same input. "
    "If you cannot fix it, omit this value and continue."
)
FINAL_TOOL_RETRY_INSTRUCTIONS = (
    "Fix the specific issue described in 'error' and call execute_filter again. "
    "Do not end the search without a successful execute_filter call."
)
GENERIC_TOOL_FAILURE_INSTRUCTIONS = (
    "The tool failed unexpectedly. Retry once with the same input; if it fails again, "
    "omit this value and continue toward execute_filter."
)


class ToolUseRuntimeError(RuntimeError):
    """Raised by a tool when it can't fulfill a request for a reason that should be shown to the user.

    `model_detail` explains to the model what went wrong; `instructions` tells it what to do next. Subclasses
    may set a default `instructions`, which can be overridden per raise.
    """

    instructions: str | None = None

    def __init__(self, user_message: str, model_detail: str | None = None, instructions: str | None = None):
        super().__init__(user_message)
        self.user_message = user_message
        self.model_detail = model_detail if model_detail is not None else user_message
        if instructions is not None:
            self.instructions = instructions


class InvalidToolInputError(ToolUseRuntimeError):
    """The model called a tool with input that can't be used (e.g. an empty query or an unsupported option)."""

    instructions = RETRY_INSTRUCTIONS


class LocationNotRecognizedError(ToolUseRuntimeError):
    instructions = (
        "No location matched this query. Locations can be a country, state, city, county, ZIP code, or "
        "congressional district (e.g. 'MO-04'). Check the spelling and try the standard name or abbreviation, or "
        "set location_type to the kind of place you mean. Phrases that are not a single place (such as 'all "
        "states') or that are dates cannot be looked up. Do not repeat the same query; if no specific place "
        "applies, omit the location filter and continue."
    )


class AgencyNotRecognizedError(ToolUseRuntimeError):
    pass


class CodeNotRecognizedError(ToolUseRuntimeError):
    pass


class NaicsCodeNotRecognizedError(CodeNotRecognizedError):
    pass


class PscCodeNotRecognizedError(CodeNotRecognizedError):
    pass


class AssistanceListingNotRecognizedError(CodeNotRecognizedError):
    pass


class TasNotRecognizedError(CodeNotRecognizedError):
    pass


class DefcCodeNotRecognizedError(ToolUseRuntimeError):
    pass


class AwardAmountNotParseableError(ToolUseRuntimeError):
    pass


class TimePeriodNotParseableError(ToolUseRuntimeError):
    pass


class AwardNotRecognizedError(ToolUseRuntimeError):
    pass


class RecipientNotRecognizedError(ToolUseRuntimeError):
    pass


class FilterValidationError(ToolUseRuntimeError):
    pass
