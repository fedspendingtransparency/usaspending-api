from usaspending_api.search.filters.time_period import AbstractTimePeriod


class TransactionSearchTimePeriod(AbstractTimePeriod):
    """A time period implementation that's designed to suit the time period filters
    on transaction search.

    This concrete implementation supports the filter [TransactionSearchTimePeriodObject](#usaspending_api/api_contracts/search_filters.md#transaction-search-time-period-object).
    """

    def __init__(self, default_start_date: str, default_end_date: str):
        """Constructor to create TransactionSearchTimePeriod object.

        Parameters
        ----------
        default_start_date : str
            This is the start date value to use when start date is not present in the filter value.
            See start date in [TransactionSearchTimePeriodObject](#usaspending_api/api_contracts/search_filters.md#transaction-search-time-period-object)
            for definition.
        default_end_date : str
            This is the end date value to use when end date is not present in the filter value.
            See end date in [TransactionSearchTimePeriodObject](#usaspending_api/api_contracts/search_filters.md#transaction-search-time-period-object)
            for definition.
        """
        self._default_start_date = default_start_date
        self._default_end_date = default_end_date
        self._filter_value = {}
        # Key/Value pairs where the key is some column
        # and the value is the equivalent column as found in
        # transaction search table
        self._date_type_transaction_search_map = {"date_signed": "award_date_signed"}

    @property
    def filter_value(self):
        return self._filter_value

    @filter_value.setter
    def filter_value(self, filter_value):
        self._filter_value = filter_value

    def start_date(self):
        return self._filter_value.get("start_date") or self._default_start_date

    def end_date(self):
        return self._filter_value.get("end_date") or self._default_end_date

    def gte_date_type(self):
        return self._return_date_type()

    def lte_date_type(self):
        return self._return_date_type()

    def gte_date_range(self):
        return [{f"{self.gte_date_type()}": {"gte": self.start_date()}}]

    def lte_date_range(self):
        return [{f"{self.lte_date_type()}": {"lte": self.end_date()}}]

    def _return_date_type(self) -> str:
        ret_date_type = self._filter_value.get("date_type", "action_date")
        if ret_date_type in self._date_type_transaction_search_map:
            ret_date_type = self._date_type_transaction_search_map[ret_date_type]
        return ret_date_type
