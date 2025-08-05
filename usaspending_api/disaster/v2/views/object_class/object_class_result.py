from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from typing import Dict

from usaspending_api.common.data_classes import Pagination
from usaspending_api.disaster.v2.views.data_classes import Collation, Element


class ObjectClass(Element):
    """Renaming the original generic object to clearly be Object Class"""


class MajorClass(Collation):
    """Renaming the original generic object to clearly be Major Object Class"""


@dataclass_json
@dataclass
class ObjectClassResults:
    _object_classes: Dict[ObjectClass, ObjectClass] = field(default_factory=dict)

    def __getitem__(self, key):
        return self._object_classes.setdefault(key, key)

    def __len__(self):
        return len(self._object_classes)

    def rollup(self):
        for row in self._object_classes:
            for child in row.children:
                row.outlay += child.outlay
                row.obligation += child.obligation
                row.total_budgetary_resources += child.total_budgetary_resources
                if child.award_count is not None:
                    row.award_count += child.award_count
                else:
                    row.award_count = None

    def sort(self, field, direction):
        for row in self._object_classes:
            row.children = self.sort_results(row.children, field, direction)

        self._object_classes = self.sort_results(self._object_classes, field, direction)

    def slice(self, start, end):
        results = []
        for i, fa in enumerate(self._object_classes):
            if i >= start and i < end:
                results.append(fa)
        return results

    def finalize(self, pagination: Pagination, strip_total_budgetary_resources):
        self.rollup()
        self.sort(pagination.sort_key, pagination.sort_order)
        results = list(fa.to_dict() for fa in self.slice(pagination.lower_limit, pagination.upper_limit))
        if strip_total_budgetary_resources:
            for result in results:
                result.pop("total_budgetary_resources")
                for child in result["children"]:
                    child.pop("total_budgetary_resources")

        return results

    @staticmethod
    def sort_results(items, field, direction="desc"):
        reverse = True
        if direction == "asc":
            reverse = False

        if isinstance(items, list):
            return sorted(items, key=lambda x: (getattr(x, field), getattr(x, "id")), reverse=reverse)
        else:
            return {
                k: items[k] for k in sorted(items, key=lambda x: (getattr(x, field), getattr(x, "id")), reverse=reverse)
            }
