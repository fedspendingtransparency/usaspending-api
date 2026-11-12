"""
Build the 'counts' for the NAICS, PSC, and TAS filters from the codes in 'require' and 'exclude'.

Each count is the number of leaf nodes (6-digit NAICS, 4-character PSC, or TAS) that remain under a root node once
every code in 'require' is expanded and every code in 'exclude' is removed. A middle tier code covers all of the
leaf nodes beneath it, so selecting "1111" counts every 6-digit code starting with "1111". The counting rules mirror the
filter tree endpoints (references/v2/views/filter_tree) so the numbers match what the website displays.

These functions use the synchronous ORM. Call them from async code through sync_to_async.
"""

from functools import reduce
from operator import or_

from django.db.models import Count, Exists, F, OuterRef, Q
from django.db.models.functions import Length, Substr

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.helpers.business_logic_helpers import faba_with_file_D_data
from usaspending_api.references.models import NAICS, PSC
from usaspending_api.references.v2.views.filter_tree.psc_filter_tree import PSC_GROUPS, PscGroupLabels

NAICS_LEAF_LENGTH = 6
NAICS_ROOT_LENGTH = 2
PSC_LEAF_LENGTH = 4


def _any_of(queries: list[Q]) -> Q:
    return reduce(or_, queries)


def build_naics_counts(require: list[str], exclude: list[str]) -> list[dict]:
    """Count the 6-digit NAICS codes under each 2-digit root, e.g. require=["1111"] -> every 6-digit code "1111xx"."""
    if not require:
        return []

    leaves = NAICS.objects.annotate(code_length=Length("code")).filter(code_length=NAICS_LEAF_LENGTH)
    leaves = leaves.filter(_any_of([Q(code__startswith=code) for code in require]))
    if exclude:
        leaves = leaves.exclude(_any_of([Q(code__startswith=code) for code in exclude]))

    totals = dict(
        leaves.annotate(root=Substr("code", 1, NAICS_ROOT_LENGTH))
        .values_list("root")
        .annotate(total=Count("code"))
        .values_list("root", "total")
    )
    descriptions = dict(NAICS.objects.filter(code__in=totals.keys()).values_list("code", "description"))

    # Keep the order in which roots first appear in 'require' so the output is stable
    ordered_roots = list(dict.fromkeys(code[:NAICS_ROOT_LENGTH] for code in require))
    return [
        {"value": root, "label": descriptions.get(root, ""), "count": totals[root]}
        for root in ordered_roots
        if totals.get(root)
    ]


def _psc_path_query(path: list[str]) -> Q:
    """Query for the 4-character PSC codes that sit beneath a [Tier1 group, ...codes] path."""
    group = PSC_GROUPS[path[0]]
    if len(path) == 1:
        return Q(code__regex=group.count_pattern)

    code = path[-1]
    query = Q(code__startswith=code, length=PSC_LEAF_LENGTH)
    # R&D third tier codes (e.g. "AA1") don't own the 4-character code ending in 0 (e.g. "AA10"); that code is a
    # second tier node. This is the same rule the PSC filter tree uses for its counts.
    if path[0] == PscGroupLabels.RESEARCH_AND_DEVELOPMENT and len(code) == 3:
        query &= ~Q(code__endswith="0")
    return query


def build_psc_counts(require: list[list[str]], exclude: list[list[str]]) -> list[dict]:
    """Count the 4-character PSC codes under each Tier1 group (Research and Development, Service, Product)."""
    counts = []
    for group_label in dict.fromkeys(path[0] for path in require):
        group_require = [_psc_path_query(path) for path in require if path[0] == group_label]
        leaves = PSC.objects.filter(_any_of(group_require))
        group_exclude = [_psc_path_query(path) for path in exclude if path[0] == group_label]
        if group_exclude:
            leaves = leaves.exclude(_any_of(group_exclude))

        total = leaves.count()
        if total:
            counts.append({"value": group_label, "label": "", "count": total})
    return counts


def _tas_path_query(path: list[str]) -> Q:
    """Query for the TAS beneath a [toptier agency code, federal account code, TAS rendering label] path."""
    query = Q(agency=path[0])
    if len(path) > 1:
        query &= Q(federal_account__federal_account_code=path[1])
    if len(path) > 2:
        query &= Q(tas_rendering_label=path[2])
    return query


def build_tas_counts(require: list[list[str]], exclude: list[list[str]]) -> list[dict]:
    """Count the TAS under each toptier agency. As in the TAS filter tree, only TAS with award data are counted."""
    if not require:
        return []

    accounts = TreasuryAppropriationAccount.objects.annotate(
        has_faba=Exists(faba_with_file_D_data().filter(treasury_account=OuterRef("pk"))),
        agency=F("federal_account__parent_toptier_agency__toptier_code"),
    ).filter(_any_of([_tas_path_query(path) for path in require]), has_faba=True)
    if exclude:
        accounts = accounts.exclude(_any_of([_tas_path_query(path) for path in exclude]))

    totals = {
        row["agency"]: row
        for row in accounts.values(
            "agency",
            "federal_account__parent_toptier_agency__name",
            "federal_account__parent_toptier_agency__abbreviation",
        ).annotate(total=Count("treasury_account_identifier"))
    }

    ordered_roots = list(dict.fromkeys(path[0] for path in require))
    return [
        {
            "value": root,
            "label": (
                f"{totals[root]['federal_account__parent_toptier_agency__name']} "
                f"({totals[root]['federal_account__parent_toptier_agency__abbreviation']})"
            ),
            "count": totals[root]["total"],
        }
        for root in ordered_roots
        if root in totals
    ]
