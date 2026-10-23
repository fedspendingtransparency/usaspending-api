class ToolUseRuntimeError(RuntimeError):
    """Raised by a tool when it can't fulfill a request for a reason that should be shown to the user."""

    def __init__(self, user_message: str, model_detail: str | None = None):
        super().__init__(user_message)
        self.user_message = user_message
        self.model_detail = model_detail if model_detail is not None else user_message


class LocationNotRecognizedError(ToolUseRuntimeError):
    pass


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


class AwardTypeEraMismatchError(ToolUseRuntimeError):
    pass


class AwardNotRecognizedError(ToolUseRuntimeError):
    pass


class RecipientNotRecognizedError(ToolUseRuntimeError):
    pass
