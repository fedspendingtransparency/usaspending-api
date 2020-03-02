from usaspending_api.references.models import ToptierAgency
from usaspending_api.download.lookups import CFO_CGACS
from usaspending_api.references.constants import DOD_SUBSUMED_CGAC, DOD_CGAC


def agency_from_identifiers(cgac, frec):
    if frec:
        matching_agency = ToptierAgency.objects.filter(toptier_code=frec).first()
    if not frec or not matching_agency:
        matching_agency = ToptierAgency.objects.filter(toptier_code=cgac).first()

    # the army, navy, and air force are considered part of the DoD, despite being labled as toptier
    if matching_agency and matching_agency.toptier_code in DOD_SUBSUMED_CGAC:
        matching_agency = ToptierAgency.objects.filter(toptier_code=DOD_CGAC).first()

    return matching_agency


def cfo_presentation_order(agency_list):
    cfo_agencies = sorted(
        [a for a in agency_list if a["toptier_code"] in CFO_CGACS], key=lambda a: CFO_CGACS.index(a["toptier_code"]),
    )
    other_agencies = sorted([a for a in agency_list if a["toptier_code"] not in CFO_CGACS], key=lambda a: a["name"])
    return {"cfo_agencies": cfo_agencies, "other_agencies": other_agencies}
