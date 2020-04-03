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
    def search(self, tier1, tier2, tier3, child_layers) -> list:
        if tier3:
            return [
                self.construct_node_from_raw(3, [tier1, tier2, tier3], data, child_layers)
                for data in self.raw_search([tier1, tier2, tier3])
            ]
        elif tier2:
            return [
                self.construct_node_from_raw(2, [tier1, tier2], data, child_layers)
                for data in self.raw_search([tier1, tier2])
            ]
        elif tier1:
            return [self.construct_node_from_raw(1, [tier1], data, child_layers) for data in self.raw_search([tier1])]
        else:
            return [self.construct_node_from_raw(0, [], data, child_layers) for data in self.raw_search([])]

    @abstractmethod
    def raw_search(self, tiered_keys):
        pass

    @abstractmethod
    def construct_node_from_raw(self, tier: int, ancestors: list, data, poulate_children: bool) -> Node:
        pass
