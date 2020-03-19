from abc import ABCMeta, abstractmethod
from dataclasses import dataclass

DEFAULT_CHILDREN = 0


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
            "children": self.children,
        }


class FilterTree(metaclass=ABCMeta):
    def search(self, tier1, tier2, tier3, populate_children) -> list:
        if tier3:
            return [
                self.construct_node_from_raw(3, [tier1, tier2, tier3], data, populate_children)
                for data in self.tier_three_search(tier3)
            ]
        elif tier2:
            return [
                self.construct_node_from_raw(2, [tier1, tier2], data, populate_children)
                for data in self.tier_two_search(tier2)
            ]
        elif tier1:
            return [
                self.construct_node_from_raw(1, [tier1], data, populate_children)
                for data in self.tier_one_search(tier1)
            ]
        else:
            return [self.construct_node_from_raw(0, [], data, populate_children) for data in self.toptier_search()]

    @abstractmethod
    def toptier_search(self):
        pass

    @abstractmethod
    def tier_one_search(self, key):
        pass

    @abstractmethod
    def tier_two_search(self, key):
        pass

    @abstractmethod
    def tier_three_search(self, key):
        pass

    @abstractmethod
    def construct_node_from_raw(self, tier: int, ancestors: list, data, poulate_children: bool) -> Node:
        pass
