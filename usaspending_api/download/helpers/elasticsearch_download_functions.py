import itertools
import logging
import time

from abc import ABCMeta, abstractmethod
from datetime import datetime, timezone
from typing import Union

from django.conf import settings
from django.db.models import QuerySet
from elasticsearch_dsl import A

from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, TransactionSearch
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.models.download_job_lookup import DownloadJobLookup
from usaspending_api.download.helpers import write_to_download_log as write_to_log

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
    def _populate_download_lookups(cls, filters: dict, download_job: DownloadJob, size: int = 10000) -> None:
        """
        Takes a dictionary of the different download filters and returns a flattened list of ids.
        """
        filter_query = cls._filter_query_func(filters)
        search = cls._search_type().filter(filter_query).source([cls._source_field])
        ids = cls._get_download_ids_generator(search, size)
        lookup_id_type = cls._search_type.type_as_string()
        now = datetime.now(timezone.utc)
        download_lookup_obj_list = [
            DownloadJobLookup(
                created_at=now,
                download_job_id=download_job.download_job_id,
                lookup_id=es_id,
                lookup_id_type=lookup_id_type,
            )
            for es_id in itertools.chain.from_iterable(ids)
        ]
        write_to_log(
            message=f"Found {len(download_lookup_obj_list)} {cls._source_field} based on filters",
            download_job=download_job,
        )

        created_obj_list = DownloadJobLookup.objects.bulk_create(download_lookup_obj_list, batch_size=10000)
        number_of_created_objects = len(created_obj_list)
        write_to_log(
            message=f"Inserted {number_of_created_objects} rows into download_job_lookup",
            download_job=download_job,
        )

        # Wait for download lookup to be populated on the replica in the case of replication lag
        if number_of_created_objects != 0:
            # Waiting 10 minutes maximum for replication;
            # Should be enough time except for extreme cases of replication lag
            wait_start_time = time.time()
            time_to_wait_in_seconds = 60 * 10
            is_lookup_replicated = (
                DownloadJobLookup.objects.using("db_download")
                .filter(download_job_id=download_job.download_job_id)
                .exists()
            )
            while not is_lookup_replicated and time.time() - wait_start_time < time_to_wait_in_seconds:
                time.sleep(30)  # Wait 30 seconds before checking again; should catch majority of cases
                is_lookup_replicated = (
                    DownloadJobLookup.objects.using("db_download")
                    .filter(download_job_id=download_job.download_job_id)
                    .exists()
                )
                write_to_log(message=f"Waiting on replication for Download Lookup", download_job=download_job)

            if is_lookup_replicated:
                write_to_log(
                    message="Download Lookup records have been replicated (if applicable)", download_job=download_job
                )
            else:
                message = f"Download Lookup failed to replicate in under {wait_start_time} seconds"
                write_to_log(message=message, is_error=True, download_job=download_job)
                raise Exception(message)

    @classmethod
    @abstractmethod
    def query(cls, filters: dict, download_job: DownloadJob) -> QuerySet:
        pass


class AwardsElasticsearchDownload(_ElasticsearchDownload):
    _source_field = "award_id"
    _filter_query_func = QueryWithFilters.generate_awards_elasticsearch_query
    _search_type = AwardSearch

    @classmethod
    def query(cls, filters: dict, download_job: DownloadJob) -> QuerySet:
        base_queryset = Award.objects.all()
        cls._populate_download_lookups(filters, download_job)
        lookup_table_name = DownloadJobLookup._meta.db_table
        queryset = base_queryset.extra(
            where=[
                f'EXISTS(SELECT 1 FROM {lookup_table_name} WHERE download_job_id = {download_job.download_job_id} AND lookup_id = "awards"."id")'
            ]
        )

        return queryset


class TransactionsElasticsearchDownload(_ElasticsearchDownload):
    _source_field = "transaction_id"
    _filter_query_func = QueryWithFilters.generate_transactions_elasticsearch_query
    _search_type = TransactionSearch

    @classmethod
    def query(cls, filters: dict, download_job: DownloadJob) -> QuerySet:
        base_queryset = TransactionNormalized.objects.all()
        cls._populate_download_lookups(filters, download_job)
        lookup_table_name = DownloadJobLookup._meta.db_table
        queryset = base_queryset.extra(
            where=[
                f'EXISTS(SELECT 1 FROM {lookup_table_name} WHERE download_job_id = {download_job.download_job_id} AND lookup_id = "transaction_normalized"."id")'
            ]
        )

        return queryset
