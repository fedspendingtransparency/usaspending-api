from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import List


@dataclass_json
@dataclass
class Element:
    id: int
    code: str
    description: str
    award_count: int = None
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
