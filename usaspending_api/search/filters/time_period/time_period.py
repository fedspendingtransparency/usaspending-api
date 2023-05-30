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
        """
        Arguments:
            filter_value -- A single time period filter provided by the user.
        """
        pass

    @abstractmethod
    def start_date(self) -> str:
        """
        Returns:
            The start date from this instance's filter value.
            Can return None when start_date doesn't exist.
        """
        pass

    @abstractmethod
    def end_date(self) -> str:
        """
        Returns:
            The end date from this instance's filter value.
            Can return None when end_date doesn't exist.
        """
        pass

    @abstractmethod
    def gte_date_type(self) -> str:
        """
        Returns:
            A date type that indicates the name of a column in which
            stores the date values that start date must be greater than.
            Can return None when gte_date_type doesn't exist.
        """
        pass

    @abstractmethod
    def lte_date_type(self) -> str:
        """
        Returns:
            A date type that indicates the name of a column in which
            stores the date values that end date must be less than or equal to.
            Can return None when lte_date_type doesn't exist.
        """
        pass

    @abstractmethod
    def gte_date_range(self) -> List[Dict[str, str]]:
        """
        Returns:
            A nested dictionary indicating a column (the key) which should
            store a value greater than or equal to (as specified in a key in the nested dictionary) a
            date (the value in the nested dictionary).
        """
        pass

    @abstractmethod
    def lte_date_range(self) -> List[Dict[str, str]]:
        """
        Returns:
            A nested dictionary indicating a column (the key) which should
            store a value less than or equal to (as specified in a key in the nested dictionary) a
            date (the value in the nested dictionary).
        """
        pass
