import re

from django.db.models import Q
from string import ascii_uppercase, digits
from usaspending_api.references.models import PSC
from usaspending_api.references.v2.views.filter_tree.filter_tree import UnlinkedNode, FilterTree


PSC_GROUPS = {
    # A
    "Research and Development": {"pattern": r"^A.$", "expanded_terms": [["A"]]},
    # B - Z
    "Service": {"pattern": r"^[B-Z]$", "expanded_terms": [[letter] for letter in ascii_uppercase if letter != "A"]},
    # 0 - 9
    "Product": {"pattern": r"^\d\d$", "expanded_terms": [[digit] for digit in digits]},
}

PSC_GROUPS_COUNT = {
    # A
    "Research and Development": {"pattern": r"^A...$", "expanded_terms": ["A"]},
    # B - Z
    "Service": {
        "pattern": r"^[B-Z][A-Z0-9][A-Z0-9][A-Z0-9]$",
        "expanded_terms": [letter for letter in ascii_uppercase if letter != "A"],
    },
    # 0 - 9
    "Product": {"pattern": r"^\d\d\d\d$", "expanded_terms": [digit for digit in digits]},
}


class PSCFilterTree(FilterTree):
    def raw_search(self, tiered_keys, child_layers, filter_string):
        if not self._path_is_valid(tiered_keys):
            return []
        if len(tiered_keys) == 0:
            if child_layers != 0:
                if child_layers == 2 or child_layers == -1:
                    tier3_nodes = self.tier_3_search(tiered_keys, filter_string)
                    tier2_nodes = self.tier_2_search(tiered_keys, filter_string, tier3_nodes)
                    tier2_nodes = self._combine_nodes(tier2_nodes, tier3_nodes)
                    tier1_nodes = self.tier_1_search(tiered_keys, filter_string, tier2_nodes)
                    tier1_nodes = self._combine_nodes(tier1_nodes, tier2_nodes)
                    toptier_nodes = self.toptier_search(filter_string, tier1_nodes)
                    toptier_nodes = self._combine_nodes(toptier_nodes, tier1_nodes)
                else:
                    tier1_nodes = self.tier_1_search(tiered_keys, filter_string)
                    toptier_nodes = self.toptier_search(filter_string, tier1_nodes)
                toptier_nodes = self._combine_nodes(toptier_nodes, tier1_nodes)
            else:
                toptier_nodes = self.toptier_search(filter_string)
            return toptier_nodes
        elif len(tiered_keys) == 1:
            if child_layers != 0:
                tier3_nodes = self.tier_3_search(tiered_keys, filter_string)
                tier2_nodes = self.tier_2_search(tiered_keys, filter_string, tier3_nodes)
                tier2_nodes = self._combine_nodes(tier2_nodes, tier3_nodes)
                tier1_nodes = self.tier_1_search(tiered_keys, filter_string, tier2_nodes)
                tier1_nodes = self._combine_nodes(tier1_nodes, tier2_nodes)
            else:
                tier1_nodes = self.tier_1_search(tiered_keys, filter_string)
            return tier1_nodes
        elif len(tiered_keys) == 2:
            if child_layers != 0:
                tier3_nodes = self.tier_3_search(tiered_keys, filter_string)
                tier2_nodes = self.tier_2_search(tiered_keys, filter_string, tier3_nodes)
                tier2_nodes = self._combine_nodes(tier2_nodes, tier3_nodes)
                return tier2_nodes
            else:
                if tiered_keys[0] == "Product":
                    return self.tier_3_search(tiered_keys, filter_string)
                return self.tier_2_search(tiered_keys, filter_string)
        else:
            return self.tier_3_search(tiered_keys, filter_string)

    def tier_3_search(self, ancestor_array, filter_string) -> list:
        desired_len = 4
        filters = [Q(length=desired_len)]
        if ancestor_array:
            parent = ancestor_array[-1]
            if len(parent) > 3:
                filters.append(Q(code__iregex=PSC_GROUPS_COUNT.get(parent, {}).get("pattern") or "(?!)"))
            else:
                filters.append(Q(code__startswith=parent))
        if filter_string:
            filters.append(Q(Q(code__icontains=filter_string) | Q(description__icontains=filter_string)))
        retval = []
        results = PSC.objects.filter(*filters)
        for object in results:
            ancestors = []
            if object.code.isdigit():
                ancestors.append("Product")
                ancestors.append(object.code[:2])
            elif object.code[0] == "A":
                ancestors.append("Research and Development")
                ancestors.append(object.code[:2])
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
        return retval

    def tier_2_search(self, ancestor_array, filter_string, lower_tier_nodes=None) -> list:
        filters = [Q(Q(Q(length=2) & ~Q(code__startswith="A")) | Q(Q(length=3) & Q(code__startswith="A")))]
        query = Q()
        if ancestor_array:
            parent = ancestor_array[-1]
            if len(parent) > 3:
                filters.append(Q(code__iregex=PSC_GROUPS.get(parent, {}).get("pattern") or "(?!)"))
            else:
                query |= Q(code__startswith=parent)
        if lower_tier_nodes:
            lower_tier_codes = [
                node["id"][:2] if node["id"][:2] == "AU" or node["id"][0] != "A" else node["id"][:3]
                for node in lower_tier_nodes
            ]
            lower_tier_codes = list(dict.fromkeys(lower_tier_codes))
            for code in lower_tier_codes:
                query |= Q(code=code)
        if filter_string:
            query |= Q(Q(code__icontains=filter_string) | Q(description__icontains=filter_string))
        if query != Q():
            filters.append(query)
        retval = []
        for object in PSC.objects.filter(*filters):
            ancestors = []
            if object.code.isdigit():
                ancestors.append("Product")
            elif object.code[0] == "A":
                ancestors.append("Research and Development")
                ancestors.append(object.code[:2])

            else:
                ancestors.append("Service")
                ancestors.append(object.code[:1])
            retval.append(
                {
                    "id": object.code,
                    "ancestors": ancestors,
                    "description": object.description,
                    "count": self.get_count([object.code], object.code),
                    "children": None,
                }
            )
        return retval

    def tier_1_search(self, ancestor_array, filter_string, lower_tier_nodes=None) -> list:
        filters = [Q(Q(Q(length=1) & ~Q(code="A")) | Q(length=2))]
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
        if query != Q():
            filters.append(query)
        retval = []
        for object in PSC.objects.filter(*filters):
            ancestors = []
            if object.code.isdigit():
                ancestors.append("Product")
            elif object.code[0] == "A":
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
        return retval

    def toptier_search(self, filter_string, tier1_nodes=None):
        retval = []
        if tier1_nodes:
            toptier_codes = [node["id"][:1] for node in tier1_nodes]
            for key in PSC_GROUPS.keys():
                if set(toptier_codes).intersection(set(PSC_GROUPS_COUNT[key]["expanded_terms"])):
                    if filter_string:
                        if filter_string.upper() in key.upper():
                            retval.append(
                                {
                                    "id": key,
                                    "ancestors": [],
                                    "description": "",
                                    "count": self.get_count([], key),
                                    "children": [],
                                }
                            )
        else:
            for key in PSC_GROUPS.keys():
                if filter_string:
                    if filter_string.upper() in key.upper():
                        retval.append(
                            {
                                "id": key,
                                "ancestors": [],
                                "description": "",
                                "count": self.get_count([], key),
                                "children": [],
                            }
                        )
                else:
                    retval = [
                        {
                            "id": key,
                            "ancestors": [],
                            "description": "",
                            "count": self.get_count([], key),
                            "children": [],
                        }
                        for key in PSC_GROUPS.keys()
                    ]
        return retval

    def _combine_nodes(self, upper_tier, lower_tier):
        for node in upper_tier:
            children = []
            for node1 in lower_tier:
                if node["id"] in node1["ancestors"]:
                    children.append(node1)
            sorted(children, key=lambda x: x["id"])
            if children:
                node["children"] = children
        return upper_tier

    def _path_is_valid(self, path: list) -> bool:
        if len(path) > 1:
            if PSC_GROUPS.get(path[0]) is None or not re.match(PSC_GROUPS[path[0]]["pattern"], path[1]):
                return False
            for x in range(1, len(path) - 1):
                if not path[x + 1].startswith(path[x]):
                    return False
        return True

    def _psc_from_group(self, group):
        # The default regex value will match nothing
        filters = [Q(code__iregex=PSC_GROUPS.get(group, {}).get("pattern") or "(?!)")]
        return [{"id": object.code, "description": object.description} for object in PSC.objects.filter(*filters)]

    def _psc_from_parent(self, parent, filter_string):
        # two out of three branches of the PSC tree "jump" over 3 character codes
        desired_len = len(parent) + 2 if len(parent) == 2 and (parent[0] != "A" or parent == "AU") else len(parent) + 1
        filters = [
            Q(length=desired_len),
            Q(code__startswith=parent),
        ]
        return [{"id": object.code, "description": object.description} for object in PSC.objects.filter(*filters)]

    def unlinked_node_from_data(self, ancestors: list, data) -> UnlinkedNode:
        if len(ancestors) == 0:  # A tier zero search is returning an agency dictionary
            return UnlinkedNode(id=data, ancestors=ancestors, description="")
        else:
            return UnlinkedNode(id=data["id"], ancestors=ancestors, description=data["description"])

    def get_count(self, tiered_keys: list, id) -> int:
        if len(tiered_keys) == 0:
            filters = [Q(code__iregex=PSC_GROUPS_COUNT.get(id, {}).get("pattern") or "(?!)")]
            return PSC.objects.filter(*filters).count()
        else:
            filters = [
                Q(length=4),
                Q(code__startswith=id),
            ]
            return PSC.objects.filter(*filters).count()
