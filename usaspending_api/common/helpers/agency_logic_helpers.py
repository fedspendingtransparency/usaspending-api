from usaspending_api.download.lookups import CFO_CGACS
from usaspending_api.references.constants import (
    DOD_SUBSUMED_CGAC,
    DOD_CGAC,
    DOD_ARMED_FORCES_TAS_CGAC_FREC,
    DHS_CGAC,
    DHS_SUBSUMED_CGAC,
)
from usaspending_api.references.models import ToptierAgency

TOPTIER_FREC_CACHE = None


def agency_from_identifiers(cgac, frec):
    """
    General logic for what agency to include a given CGAC/FREC combination under, accounting for all rollups.
    :param cgac:
    :param frec:
    :return: string, which is the toptier_code for the correct agency to count this combination under
    """
    global TOPTIER_FREC_CACHE
    # If this function hasn't been called, populate the cache for future reference
    if not TOPTIER_FREC_CACHE:
        TOPTIER_FREC_CACHE = [elem["toptier_code"] for elem in ToptierAgency.objects.values("toptier_code")]

    if (cgac, frec) in DOD_ARMED_FORCES_TAS_CGAC_FREC:
        return DOD_CGAC

    if cgac in DOD_SUBSUMED_CGAC:
        return DOD_CGAC

    if cgac in DHS_SUBSUMED_CGAC:
        return DHS_CGAC

    if frec and frec in TOPTIER_FREC_CACHE:
        return frec
    else:
        return cgac


def cfo_presentation_order(agency_list):
    cfo_agencies = sorted(
        [a for a in agency_list if a["toptier_code"] in CFO_CGACS], key=lambda a: CFO_CGACS.index(a["toptier_code"]),
    )
    other_agencies = sorted([a for a in agency_list if a["toptier_code"] not in CFO_CGACS], key=lambda a: a["name"])
    return {"cfo_agencies": cfo_agencies, "other_agencies": other_agencies}
