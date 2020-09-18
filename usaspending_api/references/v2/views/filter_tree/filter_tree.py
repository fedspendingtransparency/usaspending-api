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

        retval = self.raw_search(ancestor_array, child_layers, filter_string)
        return retval

    @abstractmethod
    def tier_3_search(self) -> list:
        pass

    @abstractmethod
    def tier_2_search(self) -> list:
        pass

    @abstractmethod
    def tier_1_search(self) -> list:
        pass

    @abstractmethod
    def toptier_search(self) -> list:
        pass

    @abstractmethod
    def get_count(self, tiered_keys: list, id) -> list:
        pass

    @abstractmethod
    def raw_search(self, tiered_keys: list) -> list:
        """
        Basic unit of searching, given the path to the parent and the filter string. Output can be a list of any type, and is
        only used by the unlinked_node_from_data abstract function.

        :param: tiered_keys - list
        :param: filter_string - string or null

        """
        pass
