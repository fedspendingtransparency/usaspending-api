def canonicalize_string(val):
    """
    Return version of string in UPPERCASE and without redundant whitespace.
    """

    try:
        return " ".join(val.upper().split())
    except AttributeError:  # was not upper-able, so was not a string
        return val


canonicalizable = ["address_line", "city_name", "county_na", "state_nam", "province", "place_of_performance_city"]
# Some field names like 'place_of_perform_county_na' are truncated


def fields_by_partial_names(model, substrings):
    """
    For any model, all field names containing any substring from `substrings`.
    """
    for f in model._meta.fields:
        for substring in substrings:
            if substring in f.name:
                yield f.name
                continue


def canonicalize_location_dict(dct):
    """
    Canonicalize location-related values in `dct` according to the
    rules in `canonicalize_string`.
    """
    for partial_field_name in canonicalizable:
        for field in dct:
            if partial_field_name in field:
                dct[field] = canonicalize_string(dct[field])
    return dct


def canonicalize_location_instance(loc):
    """
    Canonicalize location-related values in a model instance according to the
    rules in `canonicalize_string`.
    """
    for field in fields_by_partial_names(loc, canonicalizable):
        current_val = getattr(loc, field)
        if current_val:
            setattr(loc, field, canonicalize_string(current_val))
    loc.save()
