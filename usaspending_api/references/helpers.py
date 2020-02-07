from typing import Optional

from usaspending_api.references.models.cgac import CGAC
from usaspending_api.references.models.frec import FREC


def canonicalize_string(val):
    """
    Return version of string in UPPERCASE and without redundant whitespace.
    """

    try:
        return " ".join(val.upper().split())
    except AttributeError:  # was not upper-able, so was not a string
        return val


def retrive_agency_name_from_code(code: str) -> Optional[str]:
    """Return the agency name associated with the provided CGAC or FREC"""

    cgac_agency = CGAC.objects.filter(cgac_code=code).values("agency_name").first()
    if cgac_agency:
        return cgac_agency["agency_name"]

    frec_agency = FREC.objects.filter(frec_code=code).values("agency_name").first()
    if frec_agency:
        return frec_agency["agency_name"]

    return None
