import logging

from elasticsearch import helpers
from time import perf_counter

from usaspending_api.etl.elasticsearch_loader_helpers.delete_data import delete_docs_by_unique_key
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import format_log


logger = logging.getLogger("script")


def load_data(worker, records, client):
    start = perf_counter()
    logger.info(format_log(f"Starting Index operation", name=worker.name, action="Index"))
    streaming_post_to_es(client, records, worker.index, worker.name, delete_before_index=worker.is_incremental)
    logger.info(format_log(f"Index operation took {perf_counter() - start:.2f}s", name=worker.name, action="Index"))


def streaming_post_to_es(client, chunk, index_name: str, job_name=None, delete_before_index=True, delete_key="_id"):
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

    Returns: (succeeded, failed) tuple, which counts successful index doc writes vs. failed doc writes
    """
    success, failed = 0, 0
    try:
        if delete_before_index:
            value_list = [doc[delete_key] for doc in chunk]
            delete_docs_by_unique_key(client, delete_key, value_list, job_name, index_name)
        for ok, item in helpers.parallel_bulk(client, chunk, index=index_name):
            if ok:
                success += 1
            else:
                failed += 1

    except Exception as e:
        logger.error(f"Error on partition {job_name}:\n\n{str(e)[:2000]}\n...\n{str(e)[-2000:]}\n")
        raise RuntimeError(f"{job_name}")

    logger.info(format_log(f"Success: {success:,} | Fail: {failed:,}", name=job_name, action="Index"))
    return success, failed
