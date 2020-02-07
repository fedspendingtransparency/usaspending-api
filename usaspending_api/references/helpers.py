from django.db.models import Q
from typing import Optional
from usaspending_api.references.constants import DOD_ARMED_FORCES_CGAC, DOD_ARMED_FORCES_TAS_CGAC_FREC
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


def dod_tas_agency_filter(field_name=None, funding=True):
    """
    IMPORANT:  As of this writing, we do not receive FREC (fr_entity_code) for Awarding Agencies so any
    attempt to filter Awarding Agencies by TAS would be incomplete.  If we ever do receive Awarding FRECs,
    we will need to update this function accordingly.

    The Department of Defense Treasury Appropriation Symbol filter is not 100% straightforward since it
    includes multiple subsumed agencies and TAS symbols.  To this end, let's encapsulate that logic here
    in a nice, convenient little function.

    field_name is the foreign key field name that points to the TreasuryAppropriationAccount
    (treasury_appropriation_account) table (which is our Treasury Appropriation Symbol table).  As of this
    writing, field name can be:

        "treasury_account" for FinancialAccountsByAwards or TasProgramActivityObjectClassQuarterly
        "treasury_account_identifier" for AppropriationAccountBalancesQuarterly
        "treasuryappropriationaccount" for FederalAccount

    If field_name is None, it is assumed the filter is directly against the TreasuryAppropriationAccount
    table/model.

    funding is a boolean flag used to toggle between Funding and Awarding agency filtering.  See caveat
    above for important information regarding Awarding agencies.
    """
    valid_field_names = (None, "treasury_account", "treasury_account_identifier", "treasuryappropriationaccount")
    if field_name not in valid_field_names:
        raise RuntimeError(f"field_name is expected to be one of {valid_field_names}.")

    prefix = f"{field_name}__" if field_name is not None else ""

    # Funding agency is identified by AID (agency_id) and potentially AID FREC (fr_entity_code).
    taa_filter = Q(**{f"{prefix}agency_id__in": DOD_ARMED_FORCES_CGAC})
    for cgac, frec in DOD_ARMED_FORCES_TAS_CGAC_FREC:
        taa_filter |= Q(Q(**{f"{prefix}agency_id": cgac}) & Q(**{f"{prefix}fr_entity_code": frec}))

    # Awarding agency.
    if funding is False:
        # Awarding Agency is identified by ATA (allocation_transfer_agency_id).  If ATA is missing, then
        # the Awarding Agency is the Funding Agency.
        taa_filter = Q(
            Q(**{f"{prefix}allocation_transfer_agency_id__in": DOD_ARMED_FORCES_CGAC})
            | Q(Q(**{f"{prefix}allocation_transfer_agency_id__isnull": True}) & taa_filter)
        )

    return taa_filter


def retrive_agency_name_from_code(code: str) -> Optional[str]:
    """Return the agency name associated with the provided CGAC or FREC"""

    cgac_agency = CGAC.objects.filter(cgac_code=code).values("agency_name").first()
    if cgac_agency:
        return cgac_agency["agency_name"]

    frec_agency = FREC.objects.filter(frec_code=code).values("agency_name").first()
    if frec_agency:
        return frec_agency["agency_name"]

    return None
