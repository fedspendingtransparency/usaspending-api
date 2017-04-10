def canonicalize_string(val):
    """
    Return version of string in UPPERCASE and without redundant whitespace.
    """

    try:
        return " ".join(val.upper().split())
    except AttributeError:  # was not upper-able, so was not a string
        return val


canonicalizable = [
    'address_line1', 'address_line2', 'address_line3', 'city_name',
    'county_name', 'foreign_city_name', 'foreign_province',
    'state_name'
]


def canonicalize_location_dict(dct):
    """
    Canonicalize location-related values in `dct` according to the
    rules in `canonicalize_string`.
    """
    for field in canonicalizable:
        if field in dct:
            dct[field] = canonicalize_string(dct[field])
    return dct


def canonicalize_location_instance(loc):
    """
    Canonicalize location-related values in a model instance according to the
    rules in `canonicalize_string`.
    """
    for field in canonicalizable:
        current_val = getattr(loc, field)
        if current_val:
            setattr(loc, field, canonicalize_string(current_val))
    loc.save()
