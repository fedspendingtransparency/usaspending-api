from pydantic import ValidationError


def error_formatter(error: ValidationError) -> str:
    errors: list[dict] = error.errors()

    for error in errors:
        # Missing required fields
        if error.get('msg') == 'Field required':
            key_name = error['loc'][0]

            return f"Missing value: '{key_name}' is a required field"

        # Incorrect type for a filter
        else:
            # Error with a filter field
            if error.get("loc")[0] == "filters" and len(error["loc"]) > 1:
                key_name = error["loc"][0] + "|" + error["loc"][1]
                bad_value = error['input']
                expected_type = error['type'].strip("_type")

                return f"Invalid value in '{key_name}'. '{bad_value}' is not a valid type ({expected_type})."
            # Error in top-level field
            else:
                key_name = error["loc"][0]
                bad_value = error["input"]
                expected_type = error["type"].strip("_type")

                return f"Invalid value in '{key_name}'. '{bad_value}' is not a valid type ({expected_type})."
