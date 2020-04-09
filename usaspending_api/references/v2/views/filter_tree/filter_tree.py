from abc import ABCMeta, abstractmethod
from dataclasses import dataclass

DEFAULT_CHILDREN = 0


@dataclass
class UnlinkedNode:
    id: str
    ancestors: list
    description: str


@dataclass
class Node:
    id: str
    ancestors: list
    description: str
    count: int
    children: list

    def to_JSON(self):
        return {
            "id": self.id,
            "ancestors": self.ancestors,
            "description": self.description,
            "count": self.count,
            "children": [elem.to_JSON() for elem in self.children] if self.children else [],
        }


class FilterTree(metaclass=ABCMeta):
    def search(self, tier1, tier2, tier3, child_layers, filter_string) -> list:
        if tier3:
            ancestor_array = [tier1, tier2, tier3]
        elif tier2:
            ancestor_array = [tier1, tier2]
        elif tier1:
            ancestor_array = [tier1]
        else:
            ancestor_array = []

        retval = [
            self._make_raw_node(ancestor_array, elem, filter_string, child_layers)
            for elem in self.raw_search(ancestor_array, filter_string)
        ]
        if filter_string:
            retval = [elem for elem in retval if self.matches_filter(elem, filter_string)]
        return retval

    def _make_raw_node(self, ancestor_array, data, filter_string, child_layers):
        retval = self.construct_node_from_raw(ancestor_array, data)
        if child_layers:
            children = [
                self._make_raw_node(ancestor_array + [retval.id], elem, filter_string, child_layers - 1)
                for elem in self.raw_search(ancestor_array + [retval.id], filter_string)
            ]
            return Node(
                id=retval.id,
                ancestors=retval.ancestors,
                description=retval.description,
                count=len(self.raw_search(ancestor_array + [retval.id], None)),
                children=children,
            )
        else:
            return Node(
                id=retval.id,
                ancestors=retval.ancestors,
                description=retval.description,
                count=len(self.raw_search(ancestor_array + [retval.id], None)),
                children=None,
            )

    @abstractmethod
    def raw_search(self, tiered_keys, filter_string) -> dict:
        pass

    @abstractmethod
    def construct_node_from_raw(self, ancestors: list, data) -> UnlinkedNode:
        pass

    def matches_filter(self, node: Node, filter_string):
        if (filter_string.lower() in node.id.lower()) or (filter_string.lower() in node.description.lower()):
            return True
        if node.children:
            return len([elem for elem in node.children if self.matches_filter(elem, filter_string)]) > 0
        return False
