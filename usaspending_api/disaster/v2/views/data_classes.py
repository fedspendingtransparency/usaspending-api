from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import List, Dict


@dataclass_json
@dataclass
class Element:
    id: int
    code: str
    description: str
    count: int = 0
    obligation: float = 0
    outlay: float = 0
    total_budgetary_resources: float = 0


@dataclass_json
@dataclass
class Collation(Element):
    children: List[Element] = field(default_factory=list)

    def __hash__(self):
        return hash(f"{self.id}-{self.code}")

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.id == other.id
            and self.code == other.code
            and self.description == other.description
        )

    def include(self, val):
        self.children.append(val)


@dataclass_json
@dataclass
class FedAcctResults:
    _federal_accounts: Dict[Collation, Collation] = field(default_factory=dict)

    def __getitem__(self, key):
        return self._federal_accounts[key]

    def __len__(self):
        return len(self._federal_accounts)

    def rollup(self):
        for row in self._federal_accounts:
            for child in row.children:
                row.outlay += child.outlay
                row.obligation += child.obligation
                row.total_budgetary_resources = None  # += child.total_budgetary_resources
                row.count += child.count

    def add_if_missing(self, val):
        if val not in self._federal_accounts:
            self._federal_accounts[val] = val

    def sort(self, field, direction):
        for row in self._federal_accounts:
            row.children = self.sort_results(row.children, field, direction)

        self._federal_accounts = self.sort_results(self._federal_accounts, field, direction)

    def slice(self, start, end):
        results = []
        for i, fa in enumerate(self._federal_accounts):
            if i >= start and i < end:
                results.append(fa)
        return results

    def finalize(self, sort_key, sort_order, start, end):
        self.rollup()
        self.sort(sort_key, sort_order)
        return list(fa.to_dict() for fa in self.slice(start, end))

    @staticmethod
    def sort_results(items, field, direction="desc"):
        reverse = True
        if direction == "asc":
            reverse = False
        return sorted(items, key=lambda x: getattr(x, field), reverse=reverse)
