from usaspending_api.llm.tools.exceptions import ToolUseRuntimeError


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
