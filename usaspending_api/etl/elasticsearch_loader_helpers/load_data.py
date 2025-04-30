import logging

from elasticsearch import Elasticsearch, helpers
from time import perf_counter
from typing import List, Tuple, Union

from usaspending_api.etl.elasticsearch_loader_helpers.delete_data import delete_docs_by_unique_key
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import TaskSpec, format_log


logger = logging.getLogger("script")

# Assuming >=4GB RAM per vCPU of the data nodes in the ES cluster, use below settings for calibrating the max
# batch size of JSON docs for each bulk indexing HTTP request to the ES cluster
# Indexing should get distributed among the cluster
# Mileage may vary - try out different batch sizes
# It may be better to see finer-grain metrics by using higher request rates with shorter index-durations,
# rather than a busier indexing process (i.e. err on the smaller batch sizes to get feedback)
# Aiming for a batch that yields each ES cluster data-node handling max 0.3-1MB per vCPU per batch request
# Ex: 3-data-node cluster of i3.large.elasticsearch = 2 vCPU * 3 nodes = 6 vCPU: .75MB*6 ~= 4.5MB batches
# Ex: 5-data-node cluster of i3.xlarge.elasticsearch = 4 vCPU * 5 nodes = 20 vCPU: .75MB*20 ~= 15MB batches
# A good test is use the number of desired docs below and pipe a curl result to a text file
# > curl localhost:9200/*awards/_search?size=4000 > out.json -> 15MB
# > curl localhost:9200/*transactions/_search?size=4000 > out.json -> 19.4MB
# Given these, leaning towards the high end of 1MB per vCPU
ES_MAX_BATCH_BYTES = 20 * 1024 * 1024
# Aiming for a batch that yields each ES cluster data-node handling max 100-400 doc entries per vCPU per request
# Ex: 3-data-node cluster of i3.large.elasticsearch = 2 vCPU * 3 nodes = 6 vCPU: 300*6 = 1800 doc batches
# Ex: 5-data-node cluster of i3.xlarge.elasticsearch = 4 vCPU * 5 nodes = 20 vCPU: 300*20 = 6000 doc batches
ES_BATCH_ENTRIES = 4000


def load_data(worker: TaskSpec, records: List[dict], client: Elasticsearch) -> Tuple[int, int]:
    start = perf_counter()
    logger.info(format_log(f"Starting Index operation", name=worker.name, action="Index"))
    success, failed = streaming_post_to_es(
        client, records, worker.index, worker.name, delete_before_index=worker.is_incremental, slices=worker.slices
    )
    logger.info(format_log(f"Index operation took {perf_counter() - start:.2f}s", name=worker.name, action="Index"))
    return success, failed


def streaming_post_to_es(
    client: Elasticsearch,
    chunk: list,
    index_name: str,
    job_name: str = None,
    delete_before_index: bool = True,
    delete_key: str = "_id",
    slices: Union[int, str] = "auto",
) -> Tuple[int, int]:
    """
    Pump data into an Elasticsearch index.

    Args:
        client: Elasticsearch client
        chunk (List[dict]): list of dictionary objects holding field_name:value data
        index_name (str): name of targetted index
        job_name (str): name of ES ETL job being run, used in logging
        delete_before_index (bool): When true, attempts to delete given documents by a unique key before indexing them.
            NOTE: For incremental loads, we must "delete-before-index" due to the fact that on many of our indices,
                we have different values for _id and routing key.
                Not doing this exposed a bug in our approach to expedite incremental UPSERTS aimed at allowing ES to
                overwrite documents when it encountered one already existing by a given _id. The problem is that the
                index operation uses the routing key to target only 1 shard for its index/overwrite. If the routing key
                value changes between two incremental loads of the same doc with the same _id, it may get routed to a
                different shard and won't overwrite the original doc, leaving duplicates across all shards in the index.
        delete_key (str): The column (field) name used for value lookup in the given chunk to derive documents to be
            deleted, if delete_before_index is True. Currently defaulting to "_id", taking advantage of the fact
            that we are explicitly setting "_id" in the documents to-be-indexed, which is a unique key for each doc
            (e.g. the PK of the DB row)
        slices (Union[int, str]): number of slices that should be used when performing a Scroll operation
            such as delete_by_query.

    Returns: (succeeded, failed) tuple, which counts successful index doc writes vs. failed doc writes
    """

    success, failed = 0, 0
    try:
        if delete_before_index:
            value_list = [doc[delete_key] for doc in chunk]
            delete_docs_by_unique_key(
                client, delete_key, value_list, job_name, index_name, refresh_after=False, slices=slices
            )
        for ok, item in helpers.streaming_bulk(
            client,
            actions=chunk,
            chunk_size=ES_BATCH_ENTRIES,
            max_chunk_bytes=ES_MAX_BATCH_BYTES,
            max_retries=10,
            index=index_name,
        ):
            if ok:
                success += 1
            else:
                failed += 1

    except Exception as e:
        logger.error(f"Error on partition {job_name}:\n\n{str(e)[:2000]}\n...\n{str(e)[-2000:]}\n")
        raise RuntimeError(f"{job_name}")

    logger.info(format_log(f"Success: {success:,} | Fail: {failed:,}", name=job_name, action="Index"))
    return success, failed
