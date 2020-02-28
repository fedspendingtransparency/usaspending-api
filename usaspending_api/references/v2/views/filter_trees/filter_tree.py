from abc import ABCMeta, abstractmethod
from dataclasses import dataclass


@dataclass
class Node:
    name: str
    ancestors: list
    description: str
    count: int
    children: list


class FilterTree(metaclass=ABCMeta):
    def basic_search(self, tier1, tier2, tier3) -> list:
        if tier3:
            return self.tier_three_search(tier3)
        elif tier2:
            return self.tier_two_search(tier2)
        elif tier1:
            return self.tier_one_search(tier1)
        else:
            return self.toptier_search()

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
