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


def _has_psc_three_character_tier(code: str) -> bool:
    """Whether the PSC filter tree has a three character tier between a code and its two character parent. Only
    research and development codes (A...) do, and not the AU ones, which skip it."""
    return code[:1] == "A" and not code.startswith("AU")


def get_psc_tree_code(code: str) -> str:
    """
    The code of the node that stands for a PSC code in the PSC filter tree.

    In the tree, a research and development code ending in 0 is not a code of its own. It is the three character node
    above the codes that start the same way, with the description of the code ending in 0, and it is what the client
    filters on (e.g. [["Research and Development", "AA", "AA1"]]). There is no "AA10" for the client.

    Examples:
        "AA10" -> "AA1"
        "AA11" -> "AA11"
        "AU10" -> "AU10"  (AU skips the three character tier)
        "1010" -> "1010"
    """
    if len(code) == 4 and _has_psc_three_character_tier(code) and code.endswith("0"):
        return code[:3]
    return code


def get_psc_ancestors(code: str) -> set[str]:
    """
    Get all ancestor codes for PSC.

    Examples:
        "1055" -> {"10"}
        "10" -> {}
        "AG" -> {"A"}
        "AG10" -> {"A", "AG"}
        "AG11" -> {"A", "AG", "AG1"}  (research and development codes have a three character tier)
        "AG1" -> {"A", "AG"}
    """
    ancestors = set()
    if len(code) == 4 and _has_psc_three_character_tier(code) and not code.endswith("0"):
        return {code[0], code[:2], code[:3]}
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
    """Get immediate parent for PSC. For research and development, "AG11" is under "AG1", which is under "AG"."""
    result = None
    if len(code) == 4 and _has_psc_three_character_tier(code) and not code.endswith("0"):
        result = code[:3]
    elif len(code) == 3 and _has_psc_three_character_tier(code):
        result = code[:2]
    elif len(code) > 2:
        result = code[:-2]
    elif len(code) == 2 and not code.isdigit():
        result = code[:-1]
    return result


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
