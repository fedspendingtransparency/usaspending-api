from pydantic import ValidationError


def pydantic_error_formatter(error: ValidationError) -> str:
    errors: list[dict] = error.errors()

    for error in errors:
        # Missing required fields
        if error.get('msg') == 'Field required':
            key_name = error['loc'][0]

            return f"Missing value: '{key_name}' is a required field"

        # Incorrect type for a filter
        else:
            key_name = error["loc"][0] + "|" + error["loc"][1] if len(error['loc']) > 1 else error['loc'][0]
            bad_value = error['input']

            if error['type'].endswith('_type'):
                expected_type = error['type'].strip("_type")
            elif error.get('ctx'):
                expected_type = error['ctx']['expected_type']
            else:
                expected_type = "dictionary"

            return f"Invalid value in '{key_name}'. '{bad_value}' is not a valid type ({expected_type})."
