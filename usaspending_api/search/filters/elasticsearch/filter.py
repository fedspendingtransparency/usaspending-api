from abc import abstractmethod, ABCMeta
from usaspending_api.common.exceptions import InvalidParameterException
from typing import Union, List
from enum import Enum
from elasticsearch_dsl import Q as ES_Q


class QueryType(Enum):
    TRANSACTIONS = "transactions"
    AWARDS = "awards"
    ACCOUNTS = "accounts"
    SUBAWARDS = "subawards"


class _Filter(metaclass=ABCMeta):
    """
    Represents a filter object used to currently query only Elasticsearch.
    """

    underscore_name = None

    @classmethod
    def generate_query(cls, filter_values: Union[str, list, dict], query_type: QueryType, **options) -> dict:

        if filter_values is None:
            raise InvalidParameterException(f"Invalid filter: {cls.underscore_name} has null as its value.")

        return cls.generate_elasticsearch_query(filter_values, query_type, **options)

    @classmethod
    @abstractmethod
    def generate_elasticsearch_query(
        cls, filter_values: Union[str, list, dict], query_type: QueryType, **options
    ) -> Union[ES_Q, List[ES_Q]]:
        """Returns a Q object used to query Elasticsearch."""
        pass
