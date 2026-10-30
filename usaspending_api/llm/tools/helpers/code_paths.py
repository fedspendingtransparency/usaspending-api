"""
Build the hierarchical paths the PSC and TAS filters use in 'require' and 'exclude'.

A path is fully determined by its last code, so these are used both to format codes found by lookup_codes and to
correct paths the llm assembled by hand.
"""

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


def build_tas_path(code: str) -> list[str]:
    """Path from the toptier agency down to a TAS code (an agency, AID-MAIN, or full rendering label).

    Examples:
        "309-2025/2025-0200-000" -> ["309", "309-0200", "309-2025/2025-0200-000"]
        "309-0200" -> ["309", "309-0200"]
        "309" -> ["309"]
    """
    path = [code]
    while parent := get_tas_parent(path[0]):
        path.insert(0, parent)
    return path
