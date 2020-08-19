from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any

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
            "children": [elem.to_JSON() for elem in self.children] if self.children is not None else None,
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
            self._linked_node_from_data(ancestor_array, elem, filter_string, child_layers)
            for elem in self.raw_search(ancestor_array, filter_string)
        ]
        if filter_string:
            retval = [elem for elem in retval if self.matches_filter(elem, filter_string)]
        return retval

    def _linked_node_from_data(self, ancestor_array, data, filter_string, child_layers):
        retval = self.unlinked_node_from_data(ancestor_array, data)
        raw_children = self.raw_search(ancestor_array + [retval.id], filter_string)
        temp_children = [
            self._linked_node_from_data(ancestor_array + [retval.id], elem, filter_string, child_layers - 1)
            for elem in raw_children
        ]

        if child_layers:
            children = temp_children
        else:
            children = None

        return Node(
            id=retval.id,
            ancestors=retval.ancestors,
            description=retval.description,
            count=sum([node.count if node.count else 1 for node in temp_children]),
            children=children,
        )

    @abstractmethod
    def raw_search(self, tiered_keys: list, filter_string: str) -> list:
        """
        Basic unit of searching, given the path to the parent and the filter string. Output can be a list of any type, and is
        only used by the unlinked_node_from_data abstract function.

        :param: tiered_keys - list
        :param: filter_string - string or null

        """
        pass

    @abstractmethod
    def unlinked_node_from_data(self, ancestors: list, data: Any) -> UnlinkedNode:
        """
        :param ancestors: list
        :param data: Single member of the list provided by raw_search
        :return: Unlinked Node
        """
        pass

    def matches_filter(self, node: Node, filter_string) -> bool:
        if (filter_string.lower() in node.id.lower()) or (filter_string.lower() in node.description.lower()):
            return True
        if node.children:
            node.children = [elem for elem in node.children if self.matches_filter(elem, filter_string)]
            return len(node.children) > 0
        return False
