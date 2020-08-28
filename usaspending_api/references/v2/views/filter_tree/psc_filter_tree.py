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


class PSCFilterTree(FilterTree):
    def raw_search(self, tiered_keys, filter_string=None):
        if not self._path_is_valid(tiered_keys):
            return []

        if len(tiered_keys) == 0:
            return self._toptier_search()
        elif len(tiered_keys) == 1:
            return self._psc_from_group(tiered_keys[0])
        else:
            return self._psc_from_parent(tiered_keys[-1], filter_string)

    def _path_is_valid(self, path: list) -> bool:
        if len(path) > 1:
            if PSC_GROUPS.get(path[0]) is None or not re.match(PSC_GROUPS[path[0]]["pattern"], path[1]):
                return False
            for x in range(1, len(path) - 1):
                if not path[x + 1].startswith(path[x]):
                    return False
        return True

    def _toptier_search(self):
        return PSC_GROUPS.keys()

    def _psc_from_group(self, group):
        # The default regex value will match nothing
        filters = [Q(code__iregex=PSC_GROUPS.get(group, {}).get("pattern") or "(?!)")]
        return [{"id": object.code, "description": object.description} for object in PSC.objects.filter(*filters)]

    def _psc_from_parent(self, parent, filter_string: str):
        # two out of three branches of the PSC tree "jump" over 3 character codes
        desired_len = len(parent) + 2 if len(parent) == 2 and parent[0] != "A" else len(parent) + 1
        filters = [
            Q(length=desired_len),
            Q(code__startswith=parent),
        ]
        if filter_string and desired_len == 4:
            filters.append(Q(Q(code__icontains=filter_string) | Q(description__icontains=filter_string)))
        return [{"id": object.code, "description": object.description} for object in PSC.objects.filter(*filters)]

    def unlinked_node_from_data(self, ancestors: list, data) -> UnlinkedNode:
        if len(ancestors) == 0:  # A tier zero search is returning an agency dictionary
            return UnlinkedNode(id=data, ancestors=ancestors, description="")
        else:
            return UnlinkedNode(id=data["id"], ancestors=ancestors, description=data["description"])
