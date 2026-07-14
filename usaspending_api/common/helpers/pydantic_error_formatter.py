from typing import Any

from pydantic import ValidationError

# Pydantic V2 error types mapped to the type names our API has always used
API_TYPE_BY_ERROR_TYPE = {
    "bool_parsing": "boolean",
    "bool_type": "boolean",
    "date_from_datetime_parsing": "date",
    "date_parsing": "date",
    "dict_type": "object",
    "float_parsing": "float",
    "float_type": "float",
    "int_parsing": "integer",
    "int_type": "integer",
    "list_type": "array",
    "model_attributes_type": "object",
    "model_type": "object",
    "string_type": "text",
}


def _key_name(loc: tuple[str | int, ...]) -> str:
    """Convert a Pydantic error location into the pipe delimited key used in our API messages.

    List indexes and union member tags are dropped, so ("filters", "recipient_locations", 0, "state") becomes
    "filters|recipient_locations|state" and ("filters", "naics_codes", "list[str]") becomes "filters|"naics_codes".
    """

    field_names = [part for part in loc if isinstance(part, str) and "[" not in part and part.islower()]
    return "|".join(field_names)


def pydantic_error_formatter(error: ValidationError) -> str:  # noqa: PLR0911
    errors: list[dict[str, Any]] = error.errors()
    key_name = _key_name(errors[0]["loc"])

    key_errors = [err for err in errors if _key_name(err["loc"]) == key_name]
    rule_error = next((err for err in key_errors if err["type"] not in API_TYPE_BY_ERROR_TYPE), None)

    if rule_error is None:
        expected_types = ", ".join(dict.fromkeys(API_TYPE_BY_ERROR_TYPE[err["type"]] for err in key_errors))
        return f"Invalid value in '{key_name}'. '{errors[0]['input']}' is not a valid type ({expected_types})."

    if rule_error["type"] == "missing":
        return f"Missing value: '{key_name}' is a required field"

    if rule_error["type"] == "literal_error":
        return f"Field '{key_name}' is outside valid values {rule_error['ctx']['expected']}"

    message: str = rule_error["msg"].removeprefix("Value error, ")
    return f"Invalid value in '{key_name}': {message}"
