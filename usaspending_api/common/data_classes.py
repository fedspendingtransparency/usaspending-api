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
