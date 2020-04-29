import re

from usaspending_api.references.v2.views.filter_tree.filter_tree import UnlinkedNode, FilterTree
from usaspending_api.references.models import PSC

PSC_GROUPS = {"Research and Development": r"^A.$", "Service": r"^[B-Z]$", "Product": r"^\d\d$"}


class PSCFilterTree(FilterTree):
    def raw_search(self, tiered_keys):
        if not self._path_is_valid(tiered_keys):
            print(f"path {tiered_keys} is not valid")
            return []

        if len(tiered_keys) == 0:
            return self._toptier_search()
        elif len(tiered_keys) == 1:
            return self._psc_from_group(tiered_keys[0])
        else:
            return self._psc_from_parent(tiered_keys[-1])

    def _path_is_valid(self, path: list) -> bool:
        if len(path) < 2:
            return True
        if PSC_GROUPS[path[0]] is None or not re.match(PSC_GROUPS[path[0]], path[1]):
            print(f"{path[1]} doesn't start with {PSC_GROUPS[path[0]]}")
            return False
        for x in range(1, len(path) - 1):
            if not path[x + 1].startswith(path[x]):
                return False
        return True

    def _toptier_search(self):
        return PSC_GROUPS.keys()

    def _psc_from_group(self, group):
        return self._psc_from_regex(PSC_GROUPS.get(group, "(?!)"))  # The default regex value will match nothing

    def _psc_from_regex(self, regex):
        return [
            {"id": object.code, "description": object.description} for object in PSC.objects.filter(code__iregex=regex)
        ]  # normally very unsafe, but regexes are not being supplied by the user

    def _psc_from_parent(self, parent):
        # two out of three branches of the PSC tree "jump" over 3 character codes
        desired_len = len(parent) + 2 if len(parent) == 2 and parent[0] != "A" else len(parent) + 1
        return [
            {"id": object.code, "description": object.description}
            for object in PSC.objects.filter(length=desired_len, code__startswith=parent)
        ]

    def unlinked_node_from_data(self, ancestors: list, data) -> UnlinkedNode:
        if len(ancestors) == 0:  # A tier zero search is returning an agency dictionary
            return UnlinkedNode(id=data, ancestors=ancestors, description="")
        else:
            return UnlinkedNode(id=data["id"], ancestors=ancestors, description=data["description"])
