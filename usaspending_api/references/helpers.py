from typing import Optional

from django.utils.text import slugify

from usaspending_api.references.models.cgac import CGAC
from usaspending_api.references.models.frec import FREC
from usaspending_api.references.models import ToptierAgencyPublishedDABSView


def retrive_agency_name_from_code(code: str) -> Optional[str]:
    """Return the agency name associated with the provided CGAC or FREC"""

    cgac_agency = CGAC.objects.filter(cgac_code=code).values("agency_name").first()
    if cgac_agency:
        return cgac_agency["agency_name"]

    frec_agency = FREC.objects.filter(frec_code=code).values("agency_name").first()
    if frec_agency:
        return frec_agency["agency_name"]

    return None


def generate_agency_slugs_for_agency_list(agency_list):
    """
    Generates a dictionary of { <toptier_agency_id>: <agency_slug> } if the toptier_agency
    has a valid File C submission. This dictionary can then be used to populate results for an
    endpoint without the need to query the DB 1:1 for each agency in a response.
    """
    agency_names = (
        ToptierAgencyPublishedDABSView.objects.filter(toptier_agency_id__in=agency_list)
        .distinct("toptier_agency_id", "name")
        .values("toptier_agency_id", "name")
    )
    return {res["toptier_agency_id"]: slugify(res["name"]) for res in agency_names}
