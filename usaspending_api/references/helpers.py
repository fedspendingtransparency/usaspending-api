def canonicalize_string(val):
    """
    Return version of string in UPPERCASE and without redundant whitespace.
    """

    try:
        return " ".join(val.upper().split())
    except AttributeError:  # was not upper-able, so was not a string
        return val
