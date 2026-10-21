class ToolUseRuntimeError(RuntimeError):
    """Raised by a tool when it can't fulfill a request for a reason that should be shown to the user."""


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
