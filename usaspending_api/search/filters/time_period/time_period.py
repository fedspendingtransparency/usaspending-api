from abc import ABC, abstractmethod
from typing import List, Dict


class AbstractTimePeriod(ABC):
    """A composable class that can be used according to the Decorator software design pattern, to generate
    and provide time period filtering logic from a API request's time period filter value.

    Interface represents and encapsulates a
    [TimePeriodFilterObject] (https://vscode.dev/github/fedspendingtransparency/usaspending-api/blob/mod/DEV-9834-new-awards-only/usaspending_api/api_contracts/search_filters.md#L142)
    """

    @property
    @abstractmethod
    def filter_value(self) -> Dict[str, str]:
        pass

    @filter_value.setter
    @abstractmethod
    def filter_value(self, filter_value: Dict[str, str]):
        """Sets the instance's filter value.

        Args:
            filter_value: A single time period filter provided by the user. The dictionary should adhere
                to the format described in [TimePeriodFilterObject] (https://vscode.dev/github/fedspendingtransparency/usaspending-api/blob/mod/DEV-9834-new-awards-only/usaspending_api/api_contracts/search_filters.md#L142)
        """
        pass

    @abstractmethod
    def start_date(self) -> str:
        """Returns a start date from the instance's filter value.
        Various implementations may manipulate the start date before returning it.
        """
        pass

    @abstractmethod
    def end_date(self) -> str:
        """Returns a end date from the instance's filter value.
        Various implementations may manipulate the end date before returning it.
        """
        pass

    @abstractmethod
    def gte_date_type(self) -> str:
        """Returns a date type from the instance's filter value.
        Various implementations may manipulate the date type before returning it.
        The date type returned should be used in greater than or equal to range queries
        """
        pass

    @abstractmethod
    def lte_date_type(self) -> str:
        """Returns a date type from the instance's filter value.
        Various implementations may manipulate the date type before returning it.
        The date type returned should be used in less than or equal to range queries
        """
        pass

    @abstractmethod
    def gte_date_range(self) -> List[Dict[str, str]]:
        """Returns a nested dictionary indicating a column (the key) which should
        store a value greater than or equal to (as specified in a key in the nested dictionary) a
        date (the value in the nested dictionary).
        """
        pass

    @abstractmethod
    def lte_date_range(self) -> List[Dict[str, str]]:
        """Returns a nested dictionary indicating a column (the key) which should
        store a value less than or equal to (as specified in a key in the nested dictionary) a
        date (the value in the nested dictionary).
        """
        pass
