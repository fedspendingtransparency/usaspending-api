import logging
from typing import Tuple, Optional

from usaspending_api.recipient.models import StateData
from usaspending_api.references.models import Agency, Cfda, PSC, RefCountryCode, NAICS

logger = logging.getLogger(__name__)


def fetch_agency_tier_id_by_agency(agency_name: str, is_subtier: bool = False) -> Optional[int]:
    agency_type = "subtier_agency" if is_subtier else "toptier_agency"
    columns = ["id"]
    filters = {"{}__name".format(agency_type): agency_name}
    if not is_subtier:
        # Note: The awarded/funded subagency can be a toptier agency, so we don't filter only subtiers in that case.
        filters["toptier_flag"] = True
    result = Agency.objects.filter(**filters).values(*columns).first()
    if not result:
        logger.warning("{} not found for agency_name: {}".format(",".join(columns), agency_name))
        return None
    return result[columns[0]]


def fetch_cfda_id_title_by_number(cfda_number: str) -> Optional[Tuple[int, str]]:
    columns = ["id", "program_title"]
    result = Cfda.objects.filter(program_number=cfda_number).values(*columns).first()
    if not result:
        logger.warning("{} not found for cfda_number: {}".format(",".join(columns), cfda_number))
        return None, None
    return result[columns[0]], result[columns[1]]


def fetch_psc_description_by_code(psc_code: str) -> Optional[str]:
    columns = ["description"]
    result = PSC.objects.filter(code=psc_code).values(*columns).first()
    if not result:
        logger.warning("{} not found for psc_code: {}".format(",".join(columns), psc_code))
        return None
    return result[columns[0]]


def fetch_country_name_from_code(country_code: str) -> Optional[str]:
    columns = ["country_name"]
    result = RefCountryCode.objects.filter(country_code=country_code).values(*columns).first()
    if not result:
        logger.warning("{} not found for country_code: {}".format(",".join(columns), country_code))
        return None
    return result[columns[0]]


def fetch_state_name_from_code(state_code: str) -> Optional[str]:
    columns = ["name"]
    result = StateData.objects.filter(code=state_code).values(*columns).first()
    if not result:
        logger.warning("{} not found for state_code: {}".format(",".join(columns), state_code))
        return None
    return result[columns[0]]


def fetch_naics_description_from_code(naics_code: str, passthrough: str = None) -> Optional[str]:
    columns = ["description"]
    result = NAICS.objects.filter(code=naics_code).values(*columns).first()
    if not result:
        logger.warning("{} not found for naics_code: {}".format(",".join(columns), naics_code))
        return passthrough
    return result[columns[0]]
