"""
Build the hierarchical paths the PSC and TAS filters use in 'require' and 'exclude'.

A path is fully determined by its last code, so these are used both to format codes found by lookup_codes and to
correct paths the llm assembled by hand.
"""

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.llm.tools.helpers.hierarchy_parsers import get_tas_parent
from usaspending_api.references.v2.views.filter_tree.psc_filter_tree import PSC_GROUPS, PscGroupLabels


def build_psc_path(code: str) -> list[str]:
    """Path from the Tier1 group label down to a PSC code, with a segment for each tier the code sits under.

    Examples:
        "B502" -> ["Service", "B", "B5", "B502"]
        "1005" -> ["Product", "10", "1005"]
        "AA11" -> ["Research and Development", "AA", "AA1", "AA11"]
        "A" -> ["Research and Development"]
        "Research and Development" -> ["Research and Development"]
    """
    if code in PSC_GROUPS:
        return [str(code)]

    if code[0].isdigit():
        group, tier_lengths = PscGroupLabels.PRODUCT, (2, 4)
    elif code[0] in PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms:
        group = PscGroupLabels.RESEARCH_AND_DEVELOPMENT
        tier_lengths = (2, 4) if code.startswith("AU") else (2, 3, 4)  # AU skips the three character tier
    else:
        group, tier_lengths = PscGroupLabels.SERVICE, (1, 2, 4)

    return [str(group), *[code[:length] for length in tier_lengths if length <= len(code)]]


def resolve_tas_path(tas_rendering_label: str) -> list[str] | None:
    """Path to a TAS as the TAS filter tree lists it: [toptier agency code, federal account code, rendering label].

    The agency is that of the TAS's federal account, which can differ from the AID in its label (e.g. an AID of 011
    is listed under the toptier code 1100), so it can only be found in the database. Uses the synchronous ORM.
    None if the TAS isn't found or has no federal account or agency."""
    row = (
        TreasuryAppropriationAccount.objects.filter(tas_rendering_label=tas_rendering_label)
        .values_list("federal_account__parent_toptier_agency__toptier_code", "federal_account__federal_account_code")
        .first()
    )
    return [row[0], row[1], tas_rendering_label] if row and all(row) else None


def build_tas_path(code: str) -> list[str]:
    """Path from the AID down to a TAS code (an agency, AID-MAIN, or full rendering label), read from the code alone.
    It starts at the AID, which is not always the toptier agency, so prefer the path of the node in the search tree or
    resolve_tas_path.

    Examples:
        "309-2025/2025-0200-000" -> ["309", "309-0200", "309-2025/2025-0200-000"]
        "309-0200" -> ["309", "309-0200"]
        "309" -> ["309"]
    """
    path = [code]
    while parent := get_tas_parent(path[0]):
        path.insert(0, parent)
    return path
