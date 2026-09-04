import re
from typing import Optional, Callable


def get_naics_ancestors(code: str) -> list[str]:
    """
    Get all ancestor codes for NAICS.
    NAICS hierarchy: 2-digit → 4-digit → 6-digit

    Examples:
        "336411" → ["33", "3364"]
        "3364" → ["33"]
        "33" → []
    """
    ancestors = []
    for length in range(2, len(code), 2):
        ancestors.append(code[:length])
    return ancestors


def get_psc_ancestors(code: str) -> list[str]:
    """
    Get all ancestor codes for PSC.
    PSC hierarchy: Variable length, each level is one character shorter

    Examples:
        "1055" → ["10", "105"]
        "105" → ["10"]
        "10" → []
    """
    ancestors = []
    if len(code) > 2:
        ancestors.append(code[:2])
        if not code[:2].isdigit():
            ancestors.append(code[0])
    return ancestors


def get_cfda_ancestors(code: str) -> list[str]:
    """
    Get all ancestor codes for CFDA (Assistance Listings).
    CFDA hierarchy: Agency (2 digits) → Program (2 digits after decimal)

    Examples:
        "15.619" → ["15"]
        "10.557" → ["10"]
        "15" → []
    """
    if "." in code:
        agency = code.split(".")[0]
        return [agency]
    return []


def get_tas_ancestors(code: str) -> list[str]:
    """
    Get all ancestor codes for TAS.
    TAS hierarchy: AID → AID-MAIN → Full rendering label

    Format: AID-[BPOA/EPOA]-MAIN-SUB or AID-[X]-MAIN-SUB

    Examples:
        "302-2017/2018-1700-000" → ["302", "302-1700"]
        "009-X-0200-000" → ["009", "009-0200"]
        "302-1700" → ["302"]
        "302" → []
    """

    # Parse TAS components using regex
    # Pattern: AID-[period or X]-MAIN-SUB
    parts = code.split("-")
    toptier_code = parts[0]
    ancestors = [toptier_code]
    if len(parts) >= 3:
        ancestors.append(toptier_code + "-" + parts[2])
    return ancestors


# Parent code functions (get immediate parent only)
def get_naics_parent(code: str) -> Optional[str]:
    """Get immediate parent for NAICS"""
    if len(code) > 2:
        return code[:-2]
    return None


def get_psc_parent(code: str) -> Optional[str]:
    """Get immediate parent for PSC"""
    if len(code) > 2:
        return code[:-2]
    elif len(code) == 2 and not code.isdigit():
        return code[:-1]
    return None


def get_cfda_parent(code: str) -> Optional[str]:
    """Get immediate parent for CFDA"""
    if "." in code:
        return code.split(".")[0]
    return None


def get_tas_parent(code: str) -> Optional[str]:
    """
    Get immediate parent for TAS.

    Examples:
        "302-2017/2018-1700-000" → "302-1700"
        "302-1700" → "302"
        "302" → None
    """
    # Parse TAS components
    parts = code.split("-")
    parent = None
    if len(parts) == 2:
        parent = parts[0]
    elif len(parts) >= 3:
        parent = parts[0] + "-" + parts[2]
    return parent
