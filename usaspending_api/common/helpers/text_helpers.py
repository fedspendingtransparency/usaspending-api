import re
import string
import unicodedata

from random import choice


STANDARDIZE_WHITESPACE = re.compile(r"\s+")


def slugify_text_for_file_names(text, default=None, max_length=None):
    """
    This function is inspired by django.utils.text.slugify.  The goal here is
    to turn a string into something that can be used in a filename on Windows,
    Linux, and OSX.  We are going to be SUPER conservative and:

        - convert everything to ASCII
        - only allow letters, numbers, and underscores
        - replace other characters with underscores
        - collapse doubled up underscores
        - remove leading and trailing underscores

    This can obviously be detuned a bit if we find these settings to be too
    conservative.

    Returns the slugified value if there is one or default if the slugified
    string turns out to be falsy.
    """
    value = text or ""

    # Convert to ASCII
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")

    # Replace non-word characters with underscores
    value = re.sub(r"\W", "_", value, re.ASCII)

    # Collapse down repeated underscores
    value = re.sub(r"_+", "_", value, re.ASCII)

    # Remove leading and trailing underscores
    value = value.strip("_")

    if value:
        return value[:max_length] if max_length else value

    return default


def standardize_whitespace(string):
    """
    Standardize whitespace by replacing the various kinds of whitespace with a standard space,
    collapsing down multiple whitespaces, and removing leading and trailing whitespace.
    """
    return STANDARDIZE_WHITESPACE.sub(" ", string).strip() if string else string


def standardize_nullable_whitespace(string):
    """
    Same as standardize_whitespace but for cases where we store missing strings as nulls instead
    of blanks.
    """
    return standardize_whitespace(string) or None


def generate_random_string(size=6, chars=string.ascii_lowercase + string.digits):
    """
    Generates a random string of the specified size consisting entirely of lowercase ASCII
    characters and numeric digits.  Safe for file or table names with the caveat that some systems
    disallow names starting with a numeric digit.  If that's its intended use, consider prefacing
    with a controlled string (e.g. "temp_file_" + generate_random_string()) or providing your own
    character set.
    """
    return "".join(choice(chars) for _ in range(size))
