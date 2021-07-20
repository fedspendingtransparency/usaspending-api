import itertools
import logging
from abc import ABCMeta, abstractmethod
from typing import Union, List

from django.conf import settings
from django.db.models import QuerySet
from elasticsearch_dsl import A

from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, TransactionSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.search.models import AwardSearchView, TransactionSearch as TransactionSearchModel

logger = logging.getLogger(__name__)


class _ElasticsearchDownload(metaclass=ABCMeta):
    _source_field = None
    _filter_query_func = None
    _search_type = None

    @classmethod
    def _get_download_ids_generator(cls, search: Union[AwardSearch, TransactionSearch], size: int):
        """
        Takes an AwardSearch or TransactionSearch object (that specifies the index, filter, and source) and returns
        a generator that yields list of IDs in chunksize SIZE.
        """
        max_retries = 10
        total = search.handle_count(retries=max_retries)
        if total is None:
            logger.error("Error retrieving total results. Max number of attempts reached.")
            return
        max_iterations = settings.MAX_DOWNLOAD_LIMIT // size
        req_iterations = (total // size) + 1
        num_iterations = min(max(1, req_iterations), max_iterations)

        # Setting the shard_size below works in this case because we are aggregating on a unique field. Otherwise, this
        # would not work due to the number of records. Other places this is set are in the different spending_by
        # endpoints which are either routed or contain less than 10k unique values, both allowing for the shard
        # size to be manually set to 10k.
        for iteration in range(num_iterations):
            aggregation = A(
                "terms",
                field=cls._source_field,
                include={"partition": iteration, "num_partitions": num_iterations},
                size=size,
                shard_size=size,
            )
            search.aggs.bucket("results", aggregation)
            response = search.handle_execute(retries=max_retries).to_dict()

            if response is None:
                raise Exception("Breaking generator, unable to reach cluster")
            results = []
            for bucket in response["aggregations"]["results"]["buckets"]:
                results.append(bucket["key"])

            yield results

    @classmethod
    def _get_download_ids(cls, filters: dict, size: int = 10000) -> QuerySet:
        """
        Takes a dictionary of the different download filters and returns a flattened list of ids.
        """
        filter_query = cls._filter_query_func(filters)
        search = cls._search_type().filter(filter_query).source([cls._source_field])
        ids = cls._get_download_ids_generator(search, size)
        flat_ids = list(itertools.chain.from_iterable(ids))
        logger.info(f"Found {len(flat_ids)} {cls._source_field} based on filters")
        return flat_ids

    @classmethod
    @abstractmethod
    def query(cls, filters: dict) -> QuerySet:
        pass


class AwardsElasticsearchDownload(_ElasticsearchDownload):
    _source_field = "award_id"
    _filter_query_func = QueryWithFilters.generate_awards_elasticsearch_query
    _search_type = AwardSearch

    @classmethod
    def query(cls, filters: dict, values: List[str] = None) -> QuerySet:
        base_queryset = AwardSearchView.objects.all()
        flat_ids = cls._get_download_ids(filters)
        queryset = base_queryset.extra(
            where=[f'"vw_award_search"."award_id" = ANY(SELECT UNNEST(ARRAY{flat_ids}::INTEGER[]))']
        )
        if values:
            queryset = queryset.values(*values)
        return queryset


class TransactionsElasticsearchDownload(_ElasticsearchDownload):
    _source_field = "transaction_id"
    _filter_query_func = QueryWithFilters.generate_transactions_elasticsearch_query
    _search_type = TransactionSearch

    @classmethod
    def query(cls, filters: dict) -> QuerySet:
        base_queryset = TransactionSearchModel.objects.all()
        flat_ids = cls._get_download_ids(filters)
        queryset = base_queryset.extra(
            where=[f'"transaction_normalized"."id" = ANY(SELECT UNNEST(ARRAY{flat_ids}::INTEGER[]))']
        )
        return queryset
