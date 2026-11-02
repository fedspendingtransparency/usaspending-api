import re
from dataclasses import dataclass
from enum import StrEnum
from string import ascii_uppercase, digits
from typing import Any

from django.db.models import Q
from django.db.models.functions import Substr

from usaspending_api.references.models import PSC
from usaspending_api.references.v2.views.filter_tree.filter_tree import FilterTree


@dataclass
class PscGroup:
    pattern: str
    count_pattern: str
    terms: list[str]
    expanded_terms: list[list[str]]


class PscGroupLabels(StrEnum):
    RESEARCH_AND_DEVELOPMENT = "Research and Development"
    SERVICE = "Service"
    PRODUCT = "Product"


PSC_GROUPS = {
    # A
    PscGroupLabels.RESEARCH_AND_DEVELOPMENT: PscGroup(
        pattern=r"^A.$",
        count_pattern=r"^A...$",
        terms=["A"],
        expanded_terms=[["A"]],
    ),
    # B - Z
    PscGroupLabels.SERVICE: PscGroup(
        pattern=r"^[B-Z]$",
        count_pattern=r"^[B-Z][A-Z0-9][A-Z0-9][A-Z0-9]$",
        expanded_terms=[[letter] for letter in ascii_uppercase if letter != "A"],
        terms=[letter for letter in ascii_uppercase if letter != "A"],
    ),
    # 0 - 9
    PscGroupLabels.PRODUCT: PscGroup(
        pattern=r"^\d\d$",
        count_pattern=r"^\d\d\d\d$",
        expanded_terms=[[digit] for digit in digits],
        terms=[digit for digit in digits],
    ),
}


class PSCFilterTree(FilterTree):
    def raw_search(self, tiered_keys: list[str], child_layers: int, filter_string: str) -> list[dict[str, Any]]:  # noqa: PLR0912
        if not self._path_is_valid(tiered_keys):
            return []
        top = len(tiered_keys)
        bottom = (child_layers if child_layers != -1 else 3) + top
        retval = tier3_nodes = tier2_nodes = tier1_nodes = []
        if bottom >= 3 or (top == 2 and (tiered_keys[0] == "Product" or tiered_keys[1] == "AU")):
            tier3_nodes = self.tier_3_search(tiered_keys, filter_string)
        if bottom >= 2:
            tier2_nodes = self.tier_2_search(tiered_keys, filter_string, tier3_nodes)
        if bottom >= 1:
            tier1_nodes = self.tier_1_search(tiered_keys, filter_string, tier2_nodes)

        if top == 3:
            retval = tier3_nodes
        if top <= 2:
            tier2_nodes = self._combine_nodes(tier2_nodes, tier3_nodes)
            if top == 2:
                if tiered_keys[0] == "Product" or tiered_keys[1] == "AU":
                    retval = tier3_nodes
                else:
                    retval = tier2_nodes
        if top <= 1:
            tier1_nodes = self._combine_nodes(tier1_nodes, tier2_nodes)
            if top == 1:
                retval = tier1_nodes
        if top == 0:
            toptier_nodes = self.toptier_search(filter_string, tier1_nodes + tier2_nodes)
            toptier_nodes = self._combine_nodes(toptier_nodes, tier2_nodes)
            toptier_nodes = self._combine_nodes(toptier_nodes, tier1_nodes)
            retval = toptier_nodes

        return retval

    def tier_3_search(self, ancestor_array: list[str], filter_string: str) -> list[dict[str, Any]]:
        filters = [Q(length=4)]
        if ancestor_array:
            parent = ancestor_array[-1]
            if len(parent) > 3:
                psc_group = PSC_GROUPS.get(parent)
                count_pattern = psc_group.count_pattern if psc_group is not None else "(?!)"
                filters.append(Q(code__iregex=count_pattern))
            else:
                filters.append(
                    Q(
                        Q(code__startswith=parent)
                        & (
                            (
                                ~Q(code__endswith="0")
                                & Q(code__startswith=PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms[0])
                            )
                            | ~Q(code__startswith=PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms[0])
                        )
                    )
                )
        if filter_string:
            filters.append(Q(Q(code__icontains=filter_string) | Q(description__icontains=filter_string)))
        retval = []
        results = PSC.objects.filter(*filters)
        for object in results:
            ancestors = []
            if object.code.isdigit():
                ancestors.append(str(PscGroupLabels.PRODUCT))
                ancestors.append(object.code[:2])
            elif object.code[0] in PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms:
                ancestors.append(str(PscGroupLabels.RESEARCH_AND_DEVELOPMENT))
                ancestors.append(object.code[:2])
                # `AU` is a special case, it skips the length=3 codes, unlike other R&D PSCs
                if object.code[:2] != "AU":
                    ancestors.append(object.code[:3])
            else:
                ancestors.append(str(PscGroupLabels.SERVICE))
                ancestors.append(object.code[:1])
                ancestors.append(object.code[:2])
            retval.append(
                {
                    "id": object.code,
                    "ancestors": ancestors,
                    "description": object.description,
                    "count": 0,
                    "children": None,
                }
            )
        return sorted(retval, key=lambda x: x["id"])

    def tier_2_search(
        self, ancestor_array: list[str], filter_string: str, lower_tier_nodes: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        filters = [
            Q(
                Q(Q(length=2) & ~Q(code__startswith=PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms[0]))
                | Q(
                    Q(code__endswith="0")
                    & Q(code__startswith=PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms[0])
                    & Q(length=4)
                )
            )
        ]
        query = Q()
        if ancestor_array:
            parent = ancestor_array[-1]
            if len(parent) > 3:
                psc_group = PSC_GROUPS.get(parent)
                pattern = psc_group.pattern if psc_group is not None else "(?!)"
                query |= Q(code__iregex=pattern)
            else:
                query |= Q(code__startswith=parent)
        if lower_tier_nodes:
            lower_tier_codes = [
                (
                    node["id"][:2]
                    if node["id"][:2]
                    == "AU"  # `AU` is a special case, it skips the length=3 codes, unlike other R&D PSCs
                    or node["id"][0] not in PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms
                    else node["id"][:3]
                )
                for node in lower_tier_nodes
            ]
            lower_tier_codes = list(dict.fromkeys(lower_tier_codes))
            for code in lower_tier_codes:
                query |= Q(code=code)
        if filter_string:
            query |= Q(Q(code__icontains=filter_string) | Q(description__icontains=filter_string))
        filters.append(query)
        retval = []
        for object in PSC.objects.filter(*filters):
            ancestors = []
            code = object.code
            if object.code.isdigit():
                ancestors.append(str(PscGroupLabels.PRODUCT))
            elif object.code[0] in PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms:
                ancestors.append(str(PscGroupLabels.RESEARCH_AND_DEVELOPMENT))
                ancestors.append(object.code[:2])
                code = object.code[:3]
            else:
                ancestors.append(str(PscGroupLabels.SERVICE))
                ancestors.append(object.code[:1])
            retval.append(
                {
                    "id": code,
                    "ancestors": ancestors,
                    "description": object.description,
                    "count": self.get_count(code),
                    "children": None,
                }
            )
        return sorted(retval, key=lambda x: x["id"])

    def tier_1_search(
        self, ancestor_array: list[str], filter_string: str, lower_tier_nodes: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        filters = [Q(Q(Q(length=1) & Q(code__in=PSC_GROUPS[PscGroupLabels.SERVICE].terms)) | Q(length=2))]
        query = Q()
        if ancestor_array:
            parent = ancestor_array[0]
            psc_group = PSC_GROUPS.get(parent)
            pattern = psc_group.pattern if psc_group is not None else "(?!)"
            filters.append(Q(code__iregex=pattern))
        if lower_tier_nodes:
            lower_tier_codes = [node["id"][:-1] for node in lower_tier_nodes]
            lower_tier_codes = list(dict.fromkeys(lower_tier_codes))
            for code in lower_tier_codes:
                query |= Q(code=code)
        if filter_string:
            query |= Q(Q(code__icontains=filter_string) | Q(description__icontains=filter_string))
        filters.append(query)
        retval = []
        for object in PSC.objects.filter(*filters):
            ancestors = []
            if object.code.isdigit():
                ancestors.append(str(PscGroupLabels.PRODUCT))
            elif object.code[0] in PSC_GROUPS[PscGroupLabels.RESEARCH_AND_DEVELOPMENT].terms:
                ancestors.append(str(PscGroupLabels.RESEARCH_AND_DEVELOPMENT))
            else:
                ancestors.append(str(PscGroupLabels.SERVICE))
            retval.append(
                {
                    "id": object.code,
                    "ancestors": ancestors,
                    "description": object.description,
                    "count": self.get_count(object.code),
                    "children": None,
                }
            )
        return sorted(retval, key=lambda x: x["id"])

    def toptier_search(
        self, filter_string: str, tier1_nodes: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        retval = []
        if not filter_string and not tier1_nodes:
            return [
                {"id": key, "ancestors": [], "description": "", "count": self.get_count(key), "children": None}
                for key in PSC_GROUPS.keys()
            ]
        if tier1_nodes:
            toptier_codes = [node["id"][:1] for node in tier1_nodes]
            for key in PSC_GROUPS.keys():
                if set(toptier_codes).intersection(set(PSC_GROUPS[key].terms)):
                    retval.append(
                        {
                            "id": key,
                            "ancestors": [],
                            "description": "",
                            "count": self.get_count(key),
                            "children": None,
                        }
                    )
        return sorted(retval, key=lambda x: x["id"])

    def _combine_nodes(
        self, upper_tier: list[dict[str, Any]], lower_tier: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        for upper_node in upper_tier:
            children = []
            node_ids = [x["id"] for x in upper_node["children"]] if upper_node["children"] is not None else []
            for lower_node in lower_tier:
                if upper_node["id"] in lower_node["ancestors"] and lower_node["id"] not in node_ids:
                    children.append(lower_node)
            if len(children) > 0:
                upper_node["children"] = sorted(children, key=lambda x: x["id"])
        return upper_tier

    def _path_is_valid(self, path: list) -> bool:
        if len(path) > 1:
            if PSC_GROUPS.get(path[0]) is None or not re.match(PSC_GROUPS[path[0]].pattern, path[1]):
                return False
            for x in range(1, len(path) - 1):
                if not path[x + 1].startswith(path[x]):
                    return False
        return True

    def get_count(self, identifier: str) -> int:
        if identifier == PscGroupLabels.RESEARCH_AND_DEVELOPMENT:
            result = self.get_tier_count(4, r"^[A]", is_regex=True)
        elif identifier == PscGroupLabels.SERVICE:
            result = self.get_tier_count(4, r"^[B-Z]", is_regex=True)
        elif identifier == PscGroupLabels.PRODUCT:
            result = self.get_tier_count(4, r"^\d\d", is_regex=True)
        elif identifier.startswith("A") and len(identifier) == 3:
            # research and development data looks like AA10 instead of AA1
            result = self.get_tier_count(4, identifier, exclude_zero=True)
        else:
            result = self.get_tier_count(4, identifier)
        return result

    @staticmethod
    def get_tier_count(tier_length: int, identifier: str, is_regex: bool = False, exclude_zero: bool = False) -> int:
        if is_regex:
            filters = [Q(code__regex=identifier)]
        else:
            filters = [Q(code__startswith=identifier), ~Q(code=identifier)]
        if exclude_zero:
            filters.append(~Q(code__endswith=0))
        all_codes = PSC.objects.filter(*filters).annotate(code_prefix=Substr("code", 1, tier_length))
        return all_codes.values("code_prefix").distinct().count()
