import re

from django.db.models import Q
from string import ascii_uppercase, digits
from usaspending_api.references.models import PSC
from usaspending_api.references.v2.views.filter_tree.filter_tree import FilterTree


PSC_GROUPS = {
    # A
    "Research and Development": {
        "pattern": r"^A.$",
        "count_pattern": r"^A...$",
        "terms": ["A"],
        "expanded_terms": [["A"]],
    },
    # B - Z
    "Service": {
        "pattern": r"^[B-Z]$",
        "count_pattern": r"^[B-Z][A-Z0-9][A-Z0-9][A-Z0-9]$",
        "expanded_terms": [[letter] for letter in ascii_uppercase if letter != "A"],
        "terms": [letter for letter in ascii_uppercase if letter != "A"],
    },
    # 0 - 9
    "Product": {
        "pattern": r"^\d\d$",
        "count_pattern": r"^\d\d\d\d$",
        "expanded_terms": [[digit] for digit in digits],
        "terms": [digit for digit in digits],
    },
}


class PSCFilterTree(FilterTree):
    def raw_search(self, tiered_keys, child_layers, filter_string):
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

    def tier_3_search(self, ancestor_array, filter_string) -> list:
        filters = [Q(length=4)]
        if ancestor_array:
            parent = ancestor_array[-1]
            if len(parent) > 3:
                filters.append(Q(code__iregex=PSC_GROUPS.get(parent, {}).get("count_pattern") or "(?!)"))
            else:
                filters.append(
                    Q(
                        Q(code__startswith=parent)
                        & (
                            (
                                ~Q(code__endswith="0")
                                & Q(code__startswith=PSC_GROUPS["Research and Development"]["terms"][0])
                            )
                            | ~Q(code__startswith=PSC_GROUPS["Research and Development"]["terms"][0])
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
                ancestors.append("Product")
                ancestors.append(object.code[:2])
            elif object.code[0] in PSC_GROUPS["Research and Development"]["terms"]:
                ancestors.append("Research and Development")
                ancestors.append(object.code[:2])
                # `AU` is a special case, it skips the length=3 codes, unlike other R&D PSCs
                if object.code[:2] != "AU":
                    ancestors.append(object.code[:3])
            else:
                ancestors.append("Service")
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

    def tier_2_search(self, ancestor_array, filter_string, lower_tier_nodes=None) -> list:
        filters = [
            Q(
                Q(Q(length=2) & ~Q(code__startswith=PSC_GROUPS["Research and Development"]["terms"][0]))
                | Q(
                    Q(code__endswith="0")
                    & Q(code__startswith=PSC_GROUPS["Research and Development"]["terms"][0])
                    & Q(length=4)
                )
            )
        ]
        query = Q()
        if ancestor_array:
            parent = ancestor_array[-1]
            if len(parent) > 3:
                query |= Q(code__iregex=PSC_GROUPS.get(parent, {}).get("pattern") or "(?!)")
            else:
                query |= Q(code__startswith=parent)
        if lower_tier_nodes:
            lower_tier_codes = [
                (
                    node["id"][:2]
                    if node["id"][:2]
                    == "AU"  # `AU` is a special case, it skips the length=3 codes, unlike other R&D PSCs
                    or node["id"][0] not in PSC_GROUPS["Research and Development"]["terms"]
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
                ancestors.append("Product")
            elif object.code[0] in PSC_GROUPS["Research and Development"]["terms"]:
                ancestors.append("Research and Development")
                ancestors.append(object.code[:2])
                code = object.code[:3]
            else:
                ancestors.append("Service")
                ancestors.append(object.code[:1])
            retval.append(
                {
                    "id": code,
                    "ancestors": ancestors,
                    "description": object.description,
                    "count": self.get_count([object.code], object.code),
                    "children": None,
                }
            )
        return sorted(retval, key=lambda x: x["id"])

    def tier_1_search(self, ancestor_array, filter_string, lower_tier_nodes=None) -> list:
        filters = [Q(Q(Q(length=1) & Q(code__in=PSC_GROUPS["Service"]["terms"])) | Q(length=2))]
        query = Q()
        if ancestor_array:
            parent = ancestor_array[0]
            filters.append(Q(code__iregex=PSC_GROUPS.get(parent, {}).get("pattern") or "(?!)"))
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
                ancestors.append("Product")
            elif object.code[0] in PSC_GROUPS["Research and Development"]["terms"]:
                ancestors.append("Research and Development")
            else:
                ancestors.append("Service")
            retval.append(
                {
                    "id": object.code,
                    "ancestors": ancestors,
                    "description": object.description,
                    "count": self.get_count([object.code], object.code),
                    "children": None,
                }
            )
        return sorted(retval, key=lambda x: x["id"])

    def toptier_search(self, filter_string, tier1_nodes=None):
        retval = []
        if not filter_string and not tier1_nodes:
            return [
                {"id": key, "ancestors": [], "description": "", "count": self.get_count([], key), "children": None}
                for key in PSC_GROUPS.keys()
            ]
        if tier1_nodes:
            toptier_codes = [node["id"][:1] for node in tier1_nodes]
            for key in PSC_GROUPS.keys():
                if set(toptier_codes).intersection(set(PSC_GROUPS[key]["terms"])):
                    retval.append(
                        {
                            "id": key,
                            "ancestors": [],
                            "description": "",
                            "count": self.get_count([], key),
                            "children": None,
                        }
                    )
        return sorted(retval, key=lambda x: x["id"])

    def _combine_nodes(self, upper_tier, lower_tier):
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
            if PSC_GROUPS.get(path[0]) is None or not re.match(PSC_GROUPS[path[0]]["pattern"], path[1]):
                return False
            for x in range(1, len(path) - 1):
                if not path[x + 1].startswith(path[x]):
                    return False
        return True

    def get_count(self, tiered_keys: list, id) -> int:
        if len(tiered_keys) == 0:
            filters = [Q(code__iregex=PSC_GROUPS.get(id, {}).get("count_pattern") or "(?!)")]
            return PSC.objects.filter(*filters).count()
        else:
            filters = [
                Q(length=4),
                Q(code__startswith=id),
            ]
            return PSC.objects.filter(*filters).count()
