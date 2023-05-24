from abc import ABC, abstractmethod


class ITimePeriod(ABC):
    """Time period interface for storing data related to a time period filter."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    @abstractmethod
    def filter_value(self):
        pass

    @filter_value.setter
    @abstractmethod
    def filter_value(self, filter_value):
        """
        Arguments:
            filter_value -- A single time period filter provided by the user.
        """
        pass

    @abstractmethod
    def start_date(self):
        """
        Returns:
            The start date from this instance's filter value.
            Can return None when start_date doesn't exist.
        """
        pass

    @abstractmethod
    def end_date(self):
        """
        Returns:
            The end date from this instance's filter value.
            Can return None when end_date doesn't exist.
        """
        pass

    @abstractmethod
    def gte_date_type(self):
        """
        Returns:
            A date type that indicates the name of a column in which
            stores the date values that start date must be greater than.
            Can return None when gte_date_type doesn't exist.
        """
        pass

    @abstractmethod
    def lte_date_type(self):
        """
        Returns:
            A date type that indicates the name of a column in which
            stores the date values that end date must be less than or equal to.
            Can return None when lte_date_type doesn't exist.
        """
        pass

    @abstractmethod
    def gte_date_range(self):
        """
        Returns:
            A nested dictionary indicating a column (the key) which should
            store a value greater than or equal to (as specified in a key in the nested dictionary) a
            date (the value in the nested dictionary).
        """
        pass

    @abstractmethod
    def lte_date_range(self):
        """
        Returns:
            A nested dictionary indicating a column (the key) which should
            store a value less than or equal to (as specified in a key in the nested dictionary) a
            date (the value in the nested dictionary).
        """
        pass
