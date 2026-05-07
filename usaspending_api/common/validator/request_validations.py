from usaspending_api.references.models.disaster_emergency_fund_code import DisasterEmergencyFundCode


def is_valid_def_code(code: str | list[str] | None) -> str | list[str] | None:
    """Validate if the code or list of codes provided are valid Disaster Emergency Fund codes

    Args:
        code: The string or list of strings to validate

    Returns:
        The provided code or list of code if they are valid

    Raises:
        ValueError: If the provided code or list of codes are invalid
    """

    if code is None:
        return None

    valid_def_codes = list(DisasterEmergencyFundCode.objects.values_list('code', flat=True))

    if isinstance(code, str):
        code = code.upper()
        if code not in valid_def_codes:
            raise ValueError(
                f"Invalid Disaster Emergency Fund code: {code}. Must be one of {', '.join(valid_def_codes)}"
            )
        return code

    if isinstance(code, list):
        validated_def_codes = []
        for c in code:
            if not isinstance(c, str):
                raise ValueError(f"All DEF codes must be strings, got: {type(c)}")
            c = c.upper()
            if c not in valid_def_codes:
                raise ValueError(f"Invalid DEF code: {c}. Must be one of: {', '.join(valid_def_codes)}")
            validated_def_codes.append(c)
        return validated_def_codes

    raise ValueError(f"DEF code must be a string or list of strings, got: {type(code)}")
