import logging

from elasticsearch import helpers
from time import perf_counter

from usaspending_api.etl.elasticsearch_loader_helpers.utilities import format_log


logger = logging.getLogger("script")


def load_data(worker, records, client):
    start = perf_counter()
    logger.info(format_log(f"Starting Index operation", job=worker.name, process="Index"))
    streaming_post_to_es(client, records, worker.index, worker.name)
    logger.info(format_log(f"Index operation took {perf_counter() - start:.2f}s", job=worker.name, process="Index"))


def streaming_post_to_es(client, chunk, index_name: str, job_id=None):
    success, failed = 0, 0
    try:
        for ok, item in helpers.parallel_bulk(client, chunk, index=index_name):
            success = [success, success + 1][ok]
            failed = [failed + 1, failed][ok]

    except Exception as e:
        logger.exception(f"Fatal error: \n\n{str(e)[:5000]}...\n\n{'*' * 80}")
        raise RuntimeError()

    logger.info(format_log(f"Success: {success:,} | Fail: {failed:,}", job=job_id, process="Index"))
    return success, failed


def create_index(index, client):
    try:
        does_index_exist = client.indices.exists(index)
    except Exception:
        logger.exception("Unable to query cluster for indices")
        raise SystemExit(1)
    if not does_index_exist:
        logger.info(format_log(f"Creating index '{index}'", process="Index"))
        client.indices.create(index=index)
        client.indices.refresh(index)
