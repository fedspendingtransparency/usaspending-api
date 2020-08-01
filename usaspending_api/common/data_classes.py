from dataclasses import dataclass
from typing import Optional


@dataclass
class Pagination:
    page: int
    limit: int
    lower_limit: int
    upper_limit: int
    sort_key: Optional[str] = None
    sort_order: Optional[str] = None
    secondary_sort_key: Optional[str] = None

    @property
    def _sort_order_field_prefix(self):
        if self.sort_order == "desc":
            return "-"
        return ""

    @property
    def order_by(self):
        return f"{self._sort_order_field_prefix}{self.sort_key}"

    @property
    def robust_order_by_fields(self):
        return (self.order_by, f"{self._sort_order_field_prefix}{self.secondary_sort_key}")
