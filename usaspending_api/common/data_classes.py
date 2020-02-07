from dataclasses import dataclass


@dataclass
class Pagination:
    page: int
    limit: int
    lower_limit: int
    upper_limit: int
