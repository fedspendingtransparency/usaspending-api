def get_naics_ancestors(code: str) -> set[str]:
    """
    Get all ancestor codes for NAICS.
    NAICS hierarchy: 2-digit -> 4-digit -> 6-digit

    Examples:
        "336411" -> {"33", "3364"}
        "3364" -> {"33"}
        "33" -> {}
    """
    return {code[:length] for length in range(2, len(code), 2)}


def get_psc_ancestors(code: str) -> set[str]:
    """
    Get all ancestor codes for PSC.

    Examples:
        "1055" -> {"10"}
        "10" -> {}
        "AG" -> {"A"}
        "AG10" -> {"A", "AG"}
    """
    ancestors = set()
    if len(code) > 2:
        ancestors.add(code[:2])
        if not code[:2].isdigit():
            ancestors.add(code[0])
    elif len(code) == 2 and not code[:2].isdigit():
        ancestors.add(code[0])
    return ancestors


def get_cfda_ancestors(code: str) -> set[str]:
    """
    Get all ancestor codes for CFDA (Assistance Listings).
    CFDA hierarchy: Agency (2 digits) -> Program (2 digits after decimal)

    Examples:
        "15.619" → ["15"]
        "10.557" → ["10"]
        "15" → []
    """
    if "." in code:
        agency = code.split(".")[0]
        return set([agency])
    return set()


def get_tas_ancestors(code: str) -> set[str]:
    """
    Get all ancestor codes for TAS.
    TAS hierarchy: AID -> AID-MAIN ->Full rendering label

    Format: [ATA-]AID-[BPOA/EPOA|X]-MAIN-SUB

    Examples:
        "302-2017/2018-1700-000" -> ["302", "302-1700"]
        "009-X-0200-000"-> ["009", "009-0200"]
        "019-011-X-1071-000" -> {"011", "011-1071"}  (ATA present)
        "302-1700" -> ["302"]
        "302"-> []
    """

    parts = code.split("-")
    if len(parts) < 2:
        return set()
    if len(parts) < 4:
        return {parts[0]}
    aid = parts[-4]
    main = parts[-2]
    return {aid, f"{aid}-{main}"}


def get_naics_parent(code: str) -> str | None:
    """Get immediate parent for NAICS"""
    if len(code) > 2:
        return code[:-2]
    return None


def get_psc_parent(code: str) -> str | None:
    """Get immediate parent for PSC"""
    if len(code) > 2:
        return code[:-2]
    elif len(code) == 2 and not code.isdigit():
        return code[:-1]
    return None


def get_cfda_parent(code: str) -> str | None:
    """Get immediate parent for CFDA"""
    if "." in code:
        return code.split(".")[0]
    return None


def get_tas_parent(code: str) -> str | None:
    """
    Get immediate parent for TAS.

    Examples:
        "302-2017/2018-1700-000" -> "302-1700"
        "019-011-X-1071-000" -> "011-1071"  (ATA present)
        "302-1700" -> "302"
        "302" -> None
    """
    parts = code.split("-")
    parent = None
    if 2 <= len(parts) < 4:
        parent = parts[0]
    elif len(parts) >= 4:
        parent = parts[-4] + "-" + parts[-2]
    return parent
