import pytest

from usaspending_api.llm.tools.exceptions import (
    AwardNotRecognizedError,
    InvalidToolInputError,
    LocationNotRecognizedError,
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


class TestInstructions:
    def test_instructions_default_to_none(self):
        assert ToolUseRuntimeError("Message.").instructions is None

    def test_explicit_instructions_are_preserved(self):
        error = ToolUseRuntimeError("Message.", instructions="Do this next.")

        assert error.instructions == "Do this next."

    @pytest.mark.parametrize("error_class", [InvalidToolInputError, LocationNotRecognizedError])
    def test_subclass_defaults_and_overrides(self, error_class):
        assert error_class("Message.").instructions
        assert error_class("Message.", instructions="Custom.").instructions == "Custom."

    def test_override_does_not_change_the_class_default(self):
        InvalidToolInputError("Message.", instructions="Custom.")

        assert InvalidToolInputError("Message.").instructions != "Custom."


class TestNewValidationExceptions:
    """RecipientNotRecognizedError and AwardNotRecognizedError are plain ToolUseRuntimeError subclasses;
    they rely entirely on the base class behavior."""

    @pytest.mark.parametrize("error_class", [RecipientNotRecognizedError, AwardNotRecognizedError])
    def test_is_a_tool_use_runtime_error(self, error_class):
        error = error_class("User-facing message.", model_detail="Model detail.")

        assert isinstance(error, ToolUseRuntimeError)
        assert error.user_message == "User-facing message."
        assert error.model_detail == "Model detail."
