import logging
import numpy as np

from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
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

    help = (
        "Check whether an index has duplicate documents on different shards with the same ``_id`` value. "
        "There are two approaches, and one needs to be chosen using either --query-type scroll, or --query-type "
        "partition. See arg info for --query-type for more info. There is no fast way to perform this check, "
        "but these two strategies provide a client-side-intense way to do it or a server-side intense way to do it, "
        "respectively. Each takes a very long time to process (e.g. 8hrs for 100M docs). Trial out performance and "
        "impact to client or server by testing with a small --stop-after value."
    )

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
            "count_duplication_by_partitions": {
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
            "--query-type",
            type=str,
            choices=("scroll", "partition"),
            help="The query strategy to use. scroll: will walk across ALL docs, querying --partition-size results at a "
            "time. The results are then aggregated in Python on the client-side, so it could be memory intensive"
            "on the client host on which it is run. partition: will run terms aggregations on disjoint "
            "partitions of the docs, returning --partition-size agg buckets with each call. The aggs being done "
            "concurrently on the server can cause server straing on CPU, memory, and induce request timeouts",
        )
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
            "When using a partition query, number of parallel threads to spin up, each performing an agg query with a "
            "distinct partition.",
            default=4,
            choices=range(1, 257),
            metavar="{1..256}",
        )
        parser.add_argument(
            "--partition-size",
            type=int,
            help="(default: 5000, max: 10000) "
            "For scroll query, the size of the query result set. For partition query, the size of the aggregation, "
            "which yields the number of buckets (each for a unique _id) generated "
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
        if options["query_type"] == "scroll":
            _log.info("Starting to gather doc _ids using 'scroll' query strategy")
            self.handle_with_scrolling(**options)
        elif options["query_type"] == "partition":
            _log.info("Starting to gather doc _ids using 'partition' query strategy")
            self.handle_with_partitioning(**options)
        else:
            raise RuntimeError("Invalid argument provided for required option --query-type.")

    def handle_with_scrolling(self, **options):
        self._es_client_config = {"hosts": options["es_hostname"], "timeout": options["es_timeout"]}
        self._partition_size = options["partition_size"]
        es = Elasticsearch(**self._es_client_config)
        self._index = options["index"]
        doc_count = es.count(index=options["index"])["count"]
        _log.info(f"Found {doc_count:,} docs in index {self._index}")
        self._num_partitions = ceil(doc_count / self._partition_size)

        # Instantiate a numpy array with the number of docs' _id fields we expect to touch
        all_ids = np.empty(doc_count, dtype=int)
        _log.info(f"Collecting _ids from all {doc_count:,} in scrolling batch queries of size {self._partition_size:,}")

        # Create a baseline query used in scrolling, which gets all docs, but doesn't return the doc _source.
        # Use this query in the scan helper, which performs the scrolling
        # preserve_order=True seems to provide better performance on our indices rather than sort=_doc (see docs)
        q = {"query": {"match_all": {}}, "_source": False}
        for idx, hit in enumerate(scan(es, index=self._index, query=q, size=self._partition_size, preserve_order=True)):
            all_ids[idx] = hit["_id"]
            doc_num = idx + 1
            if doc_num % self._partition_size == 0:
                _log.debug(f"Finished batch {int(doc_num / self._partition_size)} of {self._num_partitions}")
            percent_complete = floor((doc_num / doc_count) * 100)
            if percent_complete in self._percents:
                self._percents.remove(percent_complete)
                _log.info("=" * ceil(percent_complete * 0.6) + f" {percent_complete}% of scroll complete")
            if options.get("stop_after") and doc_num >= options["stop_after"]:
                _log.warning(f"Halting early after reaching --stop-after={options['stop_after']} docs")
                break

        # Gather stats from _id value array using numpy
        _log.info("Done scrolling all docs. Gathering statistics on _id values found.")
        unique, counts = np.unique(all_ids, return_counts=True)
        duped_ids = unique[counts > 1]  # type: np.ndarray
        duped_id_counts = counts[counts > 1]  # type: np.ndarray
        duplicated_doc_ids = dict(zip(duped_ids, duped_id_counts))
        if 0 in duplicated_doc_ids:
            duplicated_doc_ids.pop(0)

        if duplicated_doc_ids:
            duped_id_count = len(duplicated_doc_ids)
            max_dupe = np.amax(duped_id_counts)
            p75 = np.percentile(duped_id_counts, 75)
            p95 = np.percentile(duped_id_counts, 95)
            _log.warning(
                f"Found {duped_id_count:,} _ids with more than one doc in the index. "
                f"Max duplication (p100) = {max_dupe}; p95 = {p95}; p75 = {p75}"
            )
            header = "_id         | duplicated\n" + "-" * 12 + "|" + "-" * 12 + "\n"
            rows = "\n".join([str(k).ljust(12) + "|" + str(v).ljust(12) for k, v in duplicated_doc_ids.items()])
            _log.warning(header + rows)
        else:
            _log.info("No duplicate documents with the same _id field found.")

    def handle_with_partitioning(self, **options):
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
            pool.map(self.count_duplication_by_partitions, range(0, num_partitions))
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

    def count_duplication_by_partitions(self, partition):
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
            _log.debug(f"Completed {c} of {self._num_partitions} queries")

    def _tally(self):
        self._completed_count += 1
        percent_complete = floor((self._completed_count / self._num_partitions) * 100)
        if percent_complete in self._percents:
            self._percents.remove(percent_complete)
            _log.info("=" * ceil(percent_complete * 0.6) + f" {percent_complete}% complete")
        return self._completed_count
