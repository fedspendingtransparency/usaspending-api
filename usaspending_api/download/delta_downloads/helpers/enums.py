from enum import Enum


class AwardCategory(str, Enum):
    ASSISTANCE = ("assistance", "Assistance")
    CONTRACT = ("contract", "Contracts")

    def __new__(cls, category: str, title: str):
        obj = str.__new__(cls, category)
        obj._value_ = category
        obj._title = title
        return obj

    @property
    def title(self) -> str:
        return self._title


class MonthlyType(str, Enum):
    DELTA = ("delta", "Delta")
    FULL = ("full", "Full")

    def __new__(cls, monthly_type: str, title: str):
        obj = str.__new__(cls, monthly_type)
        obj._value_ = monthly_type
        obj._title = title
        return obj

    @property
    def title(self) -> str:
        return self._title
