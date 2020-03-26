from usaspending_api.download.lookups import CFO_CGACS

TOPTIER_FREC_CACHE = None


def cfo_presentation_order(agency_list):
    cfo_agencies = sorted(
        [a for a in agency_list if a["toptier_code"] in CFO_CGACS], key=lambda a: CFO_CGACS.index(a["toptier_code"]),
    )
    other_agencies = sorted([a for a in agency_list if a["toptier_code"] not in CFO_CGACS], key=lambda a: a["name"])
    return {"cfo_agencies": cfo_agencies, "other_agencies": other_agencies}
