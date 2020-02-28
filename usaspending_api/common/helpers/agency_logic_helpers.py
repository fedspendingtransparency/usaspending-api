from usaspending_api.references.models import ToptierAgency


def agency_from_identifiers(cgac, frec):
    if frec:
        matching_agency = ToptierAgency.objects.filter(toptier_code=frec).first()
    if not frec or not matching_agency:
        matching_agency = ToptierAgency.objects.filter(toptier_code=cgac).first()
    return matching_agency
