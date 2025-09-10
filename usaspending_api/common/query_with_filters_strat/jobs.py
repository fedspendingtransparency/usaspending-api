from abc import ABC, abstractmethod
import logging
from django.core.management import call_command
from elasticsearch_dsl import Q as ES_Q
from usaspending_api.search.filters.elasticsearch.filter import QueryType

logger = logging.getLogger(__name__)

class _AbstractStrategy(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    def generate_elasticsearch_query(self, filter_name: str, filter_values):
        pass

class AwardsStrategy(_AbstractStrategy):
    @property
    def name(self) -> str:
        return "Awards"

    def generate_elasticsearch_query(self, filter_name: str, filter_values):
        try:

            call_command(filter_name, *filter_values)
        except Exception:
            logger.exception("Failed to execute filter")
            raise

class SubawardStrategy(_AbstractStrategy):
    @property
    def name(self) -> str:
        return "Subaward"

    def generate_elasticsearch_query(self, filter_name: str, filter_values):
        try:
            call_command(filter_name, *filter_values)
        except Exception:
            logger.exception("Failed to execute filter")
            raise

class TransactionStrategy(_AbstractStrategy):
    @property
    def name(self) -> str:
        return "Transaction"

    def generate_elasticsearch_query(self, filter_name: str, filter_values) -> ES_Q:
        try:

            call_command(filter_name, *filter_values)
        except Exception:
            logger.exception("Failed to execute filter")
            raise

class QueryWithFiltersJobs:
    def __init__(self, strategy: _AbstractStrategy):
        self.strategy = strategy

    @property
    def strategy(self) -> _AbstractStrategy:
        return self.strategy

    @strategy.setter
    def strategy(self, strategy: _AbstractStrategy) -> None:
        self.strategy = strategy

    def start(self, filter_name: str, filter_values):
        self.strategy.generate_elasticsearch_query(filter_name, filter_values)