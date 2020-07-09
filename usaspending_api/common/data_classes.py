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

    @property
    def order_by(self):
        return self.sort_key if self.sort_order == "asc" else "-" + self.sort_key
