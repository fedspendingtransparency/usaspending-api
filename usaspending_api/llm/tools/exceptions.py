class ToolUseRuntimeError(RuntimeError):
    """Raised by a tool when it can't fulfill a request for a reason that should be shown to the user."""

    def __init__(self, user_message: str, model_detail: str | None = None):
        super().__init__(user_message)
        self.user_message = user_message
        self.model_detail = model_detail if model_detail is not None else user_message


# Appended to every tool error's model_detail by handle_tool_use, so the model is explicitly told how
# to proceed instead of silently giving up or retrying the same broken call (the observed failure
# modes behind "ended without completing" eval errors).
RETRY_HINT_COMPLETION_TOOL = (
    "Fix the specific issue above and call execute_filter again with the correction. Do not repeat the "
    "exact same call, and do not end the search without a successful execute_filter call."
)
RETRY_HINT_OTHER_TOOL = (
    "Try again with a different query (e.g. shorter, more general, or a single keyword instead of a "
    "full phrase). If it still can't be resolved, drop this value and proceed to execute_filter with "
    "the filters you were able to resolve -- do not end the search without calling execute_filter."
)


class LocationNotRecognizedError(ToolUseRuntimeError):
    pass


class AgencyNotRecognizedError(ToolUseRuntimeError):
    pass


class CodeNotRecognizedError(ToolUseRuntimeError):
    pass


class InvalidFilterFieldError(ToolUseRuntimeError):
    """A single execute_filter field failed validation on its own terms (not a code/agency/location lookup)."""

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
