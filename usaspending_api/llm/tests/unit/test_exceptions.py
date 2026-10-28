import pytest

from usaspending_api.llm.tools.exceptions import (
    AwardNotRecognizedError,
    RecipientNotRecognizedError,
    ToolUseRuntimeError,
)


class TestToolUseRuntimeError:
    def test_model_detail_defaults_to_user_message(self):
        error = ToolUseRuntimeError("Unable to filter. Something was not recognized.")

        assert error.user_message == "Unable to filter. Something was not recognized."
        assert error.model_detail == "Unable to filter. Something was not recognized."
        assert str(error) == "Unable to filter. Something was not recognized."

    def test_explicit_model_detail_is_preserved_separately(self):
        error = ToolUseRuntimeError("Clean user-facing message.", model_detail="Verbose detail for the model.")

        assert error.user_message == "Clean user-facing message."
        assert error.model_detail == "Verbose detail for the model."
        assert str(error) == "Clean user-facing message."


class TestNewValidationExceptions:
    """RecipientNotRecognizedError and AwardNotRecognizedError are plain ToolUseRuntimeError subclasses;
    they rely entirely on the base class behavior."""

    @pytest.mark.parametrize("error_class", [RecipientNotRecognizedError, AwardNotRecognizedError])
    def test_is_a_tool_use_runtime_error(self, error_class):
        error = error_class("User-facing message.", model_detail="Model detail.")

        assert isinstance(error, ToolUseRuntimeError)
        assert error.user_message == "User-facing message."
        assert error.model_detail == "Model detail."
