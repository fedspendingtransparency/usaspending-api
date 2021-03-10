import logging

from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from math import ceil, floor
from multiprocessing.pool import ThreadPool
from threading import Lock, get_ident


_log = logging.getLogger(__name__)


class Command(BaseCommand):
    """Check whether an index has duplicate documents on different shards with the same ``_id`` value

    Assumptions:
        - The duplication is determined by two docs existing with the same value for meta field ``_id``
        - There is some non-default shard-routing on the index that would allow for two docs with the same ``_id``
          value to end up on different shards (by default, shard-routing is done using the ``_id`` field)
    """

    _es_client_config: dict = None
    _index: str = None
    _partition_size: int = None
    _num_partitions: int = None
    _lock = Lock()
    _completed_count: int = 0
    _percents = {p for p in range(1, 101)}
    _duplicated_doc_ids: dict = {}
    _duplicated_query_template = {
        "size": 0,
        "aggs": {
            "count_duplication": {
                "terms": {
                    "field": "_id",
                    "min_doc_count": 2,
                    "size": None,  # to be updated
                    "include": {"partition": None, "num_partitions": None},  # to be updated  # to be updated
                }
            }
        },
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--es-hostname",
            type=str,
            help="The HTTP(S) URL of the Elasticsearch cluster endpoint",
        )
        parser.add_argument(
            "--es-timeout",
            type=int,
            help="(default: 300) "
            "How many seconds the client will wait for the cluster to complete a query before timing out and "
            "retrying",
            default=300,
            metavar="<int>",
        )
        parser.add_argument(
            "--index",
            type=str,
            help="Provide name for the index to check. Wildcards and aliases allowed.",
        )
        parser.add_argument(
            "--parallelism",
            type=int,
            help="(default: 4, max: 256) "
            "Number of parallel threads to spin up, each performing a count of docs with an ID.",
            default=4,
            choices=range(1, 257),
            metavar="{1..256}",
        )
        parser.add_argument(
            "--partition-size",
            type=int,
            help="(default: 5000, max: 10000) "
            "The size of the aggregation, which yields the number of buckets (each for a unique _id) generated "
            "by each count query. Lower value yields a greater number of queries needing to be run in total; "
            "higher value yields fewer total queries run. Max is 10,000 (AWS ES Limit).",
            default=5000,
            choices=range(1, 10001),
            metavar="{1..10000}",
        )
        parser.add_argument(
            "--stop-after",
            type=int,
            help="When testing, or sampling, halt the checks after this many partitions have been looked at.",
            metavar="<int>",
        )

    def handle(self, *args, **options):
        self._es_client_config = {"hosts": options["es_hostname"], "timeout": options["es_timeout"]}
        self._partition_size = options["partition_size"]
        es = Elasticsearch(**self._es_client_config)
        self._index = options["index"]
        parallelism = options["parallelism"]
        doc_count = es.count(index=options["index"])["count"]
        _log.info(f"Found {doc_count:,} docs in index {self._index}")
        self._num_partitions = ceil(doc_count / self._partition_size)
        _log.info(
            f"Running a total of {self._num_partitions:,} agg queries, "
            f"each returning up to {self._partition_size:,} buckets "
            f"that capture the degree of duplication of a duplicated _id. "
            f"Queries will be distributed among {parallelism} parallel threads."
        )
        with ThreadPool(parallelism) as pool:
            num_partitions = self._num_partitions
            if options.get("stop_after"):
                num_partitions = options["stop_after"]
            pool.map(self.count_duplication, range(0, num_partitions))
        if self._duplicated_doc_ids:
            duped_id_count = len(self._duplicated_doc_ids)
            max_dupe = max(self._duplicated_doc_ids.values())
            p75 = sorted(self._duplicated_doc_ids.values())[int(ceil((duped_id_count * 75) / 100)) - 1]
            p95 = sorted(self._duplicated_doc_ids.values())[int(ceil((duped_id_count * 95) / 100)) - 1]
            _log.warning(
                f"Found {len(self._duplicated_doc_ids):,} _ids with more than one doc in the index. "
                f"Max duplication (p100) = {max_dupe}; p95 = {p95}; p75 = {p75}"
            )
        else:
            _log.info("No duplicate documents with the same _id field found.")

    def count_duplication(self, partition):
        es = Elasticsearch(**self._es_client_config)
        q = self._duplicated_query_template.copy()
        agg_name = next(iter(q["aggs"]))
        q["aggs"][agg_name]["terms"]["size"] = self._partition_size
        q["aggs"][agg_name]["terms"]["include"]["partition"] = partition
        q["aggs"][agg_name]["terms"]["include"]["num_partitions"] = self._num_partitions
        _log.debug(f"Thread#{get_ident()}: Running count query on partition {partition} ...")
        response = es.search(index=self._index, body=q)
        if response["timed_out"]:
            raise TimeoutError(f"Unable to complete ES search query in {self._es_client_config['timeout']} seconds")
        duplicated_docs = len(response["aggregations"][agg_name]["buckets"])
        msg = (
            f"Thread#{get_ident()}: Partition {partition} query took {int(response['took'])/1000:.2f}s "
            f"and found {duplicated_docs} _ids with more than 1 doc within partition"
        )
        if duplicated_docs == 0:
            _log.debug(msg)
            with self._lock:
                c = self._tally()
        else:
            _log.warning(msg)
            duped_doc_ids = {b["key"]: b["doc_count"] for b in response["aggregations"][agg_name]["buckets"]}
            with self._lock:
                self._duplicated_doc_ids.update(**duped_doc_ids)
                c = self._tally()
        if c % 10 == 0:
            _log.info(f"Completed {c} of {self._num_partitions} queries")

    def _tally(self):
        self._completed_count += 1
        percent_complete = floor((self._completed_count / self._num_partitions) * 100)
        if percent_complete in self._percents:
            self._percents.remove(percent_complete)
            _log.info("=" * ceil(percent_complete * 0.6) + f" {percent_complete}% complete")
        return self._completed_count
