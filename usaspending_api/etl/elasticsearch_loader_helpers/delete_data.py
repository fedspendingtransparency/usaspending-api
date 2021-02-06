import logging

import pandas as pd

from django.conf import settings
from time import perf_counter
from typing import Optional, Dict, Union, Any

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.mapping import Mapping

from usaspending_api.common.helpers.s3_helpers import retrieve_s3_bucket_object_list, access_s3_object
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import (
    ES_AWARDS_UNIQUE_KEY_FIELD,
    ES_TRANSACTIONS_UNIQUE_KEY_FIELD,
)
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    execute_sql_statement,
    format_log,
    chunks,
)

logger = logging.getLogger("script")


def delete_docs_by_unique_key(
    client: Elasticsearch,
    key: str,
    value_list: list,
    task_id: str,
    index,
    refresh_after: bool = True,
    delete_chunk_size: int = 4000,  # temp for testing
) -> int:
    """
    Bulk delete a batch of documents whose field identified by ``key`` matches any value provided in the
    ``values_list``.

    NOTE: This delete routine looks at just the index name given. If there are duplicate records across
    multiple indexes, an alias or wildcard should be provided for ``index`` param that covers multiple
    indices, or this will need to be run once per index.

    Args:
        client (Elasticsearch): elasticsearch-dsl client for making calls to an ES cluster
        key (str): name of field in targeted elasticsearch index that should have a unique value for
            every doc in the index. The field or sub-field provided MUST be of ``keyword`` type (or ``_id`` meta field)
        value_list (list): if key field has these values, the document will be deleted
        task_id (str): name of ES ETL job being run, used in logging
        index (str): name of index (or alias) to target for the ``_delete_by_query`` ES operation.
        refresh_after (bool): Whether to call ``_refresh`` on the index when all of the provided values in
            ``value_list`` have been processed for delete; defaults to ``True``. If many small deletes happen at a
            rapid rate, it may be best to set this ``False`` and await a deferred refresh afterward in the calling
            code. However it is important to not retry deletes that have already been deleted until _refresh is
            called, or a elasticsearch.exceptions.ConflictError will be raised due to an ES doc version_conflict
            error response. NOTE: This param will be ignored and a refresh will be attempted if this function
            errors-out during execution, in order to not leave un-refreshed deletes in the index.
        delete_chunk_size (int): the batch-size of terms value-array given to each _delete_by_query call. Needs to be
            less than 65536 (max values for any terms query), and less than index.max_results_window setting.

    Returns: Number of ES documents deleted
    """
    start = perf_counter()

    if len(value_list) == 0:
        logger.info(format_log("Nothing to delete", action="Delete", name=task_id))
        return 0

    logger.info(format_log(f"Deleting up to {len(value_list):,} document(s)", action="Delete", name=task_id))
    if not index:
        raise RuntimeError("index name must be provided")

    if not _is_allowed_key_field_type(client, key, index):
        msg = (
            f'Cannot perform deletes in index "{index}" by key field "{key}" because its type is not one of '
            f"the allowed field types, or the field was not found in that index."
        )
        logger.error(format_log(msg=msg, action="Delete", name=task_id))
        raise RuntimeError(msg)

    if delete_chunk_size > 65536:
        # 65,536 is max number of terms that can be added to an ES terms filter query
        msg = (
            f"{delete_chunk_size} is greater than 65,536, which is the max number of terms that can be added to an ES "
            f"terms filter query"
        )
        logger.error(format_log(msg=msg, action="Delete"))
        raise RuntimeError(msg)

    chunks_processed = 0
    deleted = 0
    is_error = False
    try:
        values_generator = chunks(value_list, delete_chunk_size)
        for chunk_of_values in values_generator:
            # Invoking _delete_by_query as per the elasticsearch-dsl docs:
            #   https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#delete-by-query
            # _refresh is deferred til the end of chunk processing
            q = Search(using=client, index=index).filter("terms", **{key: chunk_of_values})  # type: Search
            # params:
            # conflicts="proceed": Ignores version conflict errors if a doc delete is attempted more than once
            # slices="auto": Will create parallel delete batches per shard
            q = q.params(conflicts="proceed", slices="auto")
            response = q.delete()
            # Some subtle errors come back on the response
            if response["timed_out"]:
                msg = f"Delete request timed out on cluster after {int(response['took'])/1000:.2f}s"
                logger.error(format_log(msg=msg, action="Delete", name=task_id))
                raise RuntimeError(msg)
            if response["failures"]:
                fail_snippet = "\n\t\t" + "\n\t\t".join(map(str, response["failures"][0:4])) + "\n\t\t" + "..."
                msg = f"Some docs failed to delete on cluster:{fail_snippet}"
                logger.error(format_log(msg=msg, action="Delete", name=task_id))
                raise RuntimeError(msg)
            logger.info(
                format_log(
                    f"Deleted {response['deleted']:,} docs in ES from chunk of size {len(chunk_of_values):,} "
                    f"in {int(response['took'])/1000:.2f}s, "
                    f"and ignored {response['version_conflicts']:,} version conflicts",
                    action="Delete",
                    name=task_id,
                )
            )
            deleted += response["deleted"]
            chunks_processed += 1
    except Exception:
        is_error = True
        logger.exception(format_log("", name=task_id, action="Delete"))
        raise
    finally:
        if deleted > 0 and (refresh_after or is_error):
            if not is_error:
                refresh_msg = "Refreshing index so deletes take effect"
            else:
                refresh_msg = "Attempting index refresh while handling error so deletes take effect"
            logger.info(format_log(refresh_msg, action="Delete", name=task_id))
            client.indices.refresh(index=index)
        if chunks_processed > 1 or is_error:
            # This log becomes redundant unless to log the sum of multiple chunks' deletes (or error)
            error_text = " before encountering an error" if is_error else ""
            duration = perf_counter() - start
            docs = f"document{'s' if deleted != 1 else ''}"
            msg = f"Delete operation took {duration:.2f}s. Removed {deleted:,} total {docs}{error_text}"
            logger.info(format_log(msg, action="Delete", name=task_id))

    return deleted


def _is_allowed_key_field_type(client: Elasticsearch, key_field: str, index: str) -> bool:
    """Return ``True`` if the given field's mapping in the given index is in our allowed list of ES types
    compatible with term(s) queries

    This is mainly to prevent use of ``text`` fields in terms queries, which give bad results because Elasticsearch
    changes the values of text fields during analysis.
    """
    if key_field == "_id":
        # Special case. It is a reserved field, without a type, but can effectively be treated as a keyword field
        return True

    # Get true index name from alias, if provided an alias
    response = client.indices.get(index)
    aliased_index_name = list(response.keys())[0]
    es_field_type = Mapping().from_es(using=client, index=aliased_index_name).resolve_field(key_field)
    # This is the allowed types whitelist. More can be added as-needed if compatible with terms(s) queries.
    if es_field_type and es_field_type.name in ["keyword", "integer"]:
        return True
    return False


def _lookup_deleted_award_keys(
    client: Elasticsearch,
    lookup_key: str,
    value_list: list,
    config: dict,
    index: Optional[str] = None,
    lookup_chunk_size: int = 50000,
) -> list:
    """Derive a list of award keys given a target index, Lookup field, and lookup values

    This returns a list of all unique award keys, which  arecompiled from the ``ES_AWARDS_UNIQUE_KEY_FIELD`` field of
    any document in the given ``index`` that matches the query. The matching query is a terms query that will return
    the doc if its ``lookup_key`` field has any value provided in ``value_list``.

    Args:
        client (Elasticsearch): elasticsearch-dsl client for making calls to an ES cluster
        lookup_key (str): name of field in targeted elasticsearch index by which we are looking up docs. The field or
            sub-field provided MUST be of ``keyword`` type (or ``_id`` meta field)
        value_list (list): if lookup_key field has any of these values, the document will be returned from the lookup
        config (dict): collection of key-value pairs that encapsulates runtime arguments for this ES management task
        index (str): Optional name, alias, or pattern of index this query will target. Looks up via config if not
            provided
        lookup_chunk_size (int): the batch-size of terms value-array to be looked-up. Needs to be less
            than 65536 (max values for any terms query), and less than config["max_query_size"]

    Returns: list of values for the ES_AWARDS_UNIQUE_KEY_FIELD fields in the looked-up documents.
    """
    if index is None:
        index = f"{config['query_alias_prefix']}-*"

    if not _is_allowed_key_field_type(client, lookup_key, index):
        msg = (
            f'Cannot perform lookups in index "{index}" with key field "{lookup_key}" because its type is not one of '
            f"the allowed field types, or the field was not found in that index."
        )
        logger.error(format_log(msg=msg, action="Delete"))
        raise RuntimeError(msg)

    if lookup_chunk_size > 65536:
        # 65,536 is max number of terms that can be added to an ES terms filter query
        msg = (
            f"{lookup_chunk_size} is greater than 65,536, which is the max number of terms that can be added to an ES "
            f"terms filter query"
        )
        logger.error(format_log(msg=msg, action="Delete"))
        raise RuntimeError(msg)

    if lookup_chunk_size > config["max_query_size"]:
        # Some keys would be left undiscovered if our chunk was cut short by the query only returning a lesser subset
        msg = (
            f"{lookup_chunk_size} is greater {config['max_query_size']}, which is the max number of query "
            f"results returnable from this index. Use a smaller chunk or increase max_result_window for this index."
        )
        logger.error(format_log(msg=msg, action="Delete"))
        raise RuntimeError(msg)

    award_key_list = []
    values_generator = chunks(value_list, lookup_chunk_size)
    for chunk_of_values in values_generator:
        q = Search(using=client, index=index).filter("terms", **{lookup_key: chunk_of_values})  # type: Search
        q.update_from_dict({"size": config["max_query_size"]})
        response = q.execute()
        if response["hits"]["total"]["value"] != 0:
            award_key_list += [x["_source"][ES_AWARDS_UNIQUE_KEY_FIELD] for x in response["hits"]["hits"]]
    return award_key_list


def delete_awards(client: Elasticsearch, config: dict, task_id: str = "Sync DB Deletes") -> int:
    """Delete all awards in the Elasticsearch awards index that were deleted in the source database.

    This performs the deletes of award documents in ES in a series of batches, as there could be many. Millions of
    awards deleted may take a prohibitively long time, and it could be better to just re-index all documents from
    the DB instead.

    This requires looking-up the awards-to-delete by finding the unique-key of each parent award to any deleted
    transaction, and then getting the distinct list of unique-award-keys that are NOT present in the database; then
    deleting those in the ES awards index.
    - The deleted transactions are recorded in a CSV delete log file in S3.
    - NOTE!! This order of operations therefore requires that ES award deletes be processed BEFORE transaction
      ES deletes are (both deletes cannot run in parallel).

    Args:
        client (Elasticsearch): elasticsearch-dsl client for making calls to an ES cluster
        config (dict): collection of key-value pairs that encapsulates runtime arguments for this ES management task
        task_id (str): label for this sub-step of the ETL

    Returns: Number of ES docs deleted in the index
    """
    deleted_tx_keys = _gather_deleted_transaction_keys(config)
    # While extracting unique award keys, the lookup is on transactions and must match against the unique transaction id
    award_keys = _lookup_deleted_award_keys(
        client,
        ES_TRANSACTIONS_UNIQUE_KEY_FIELD,
        [*deleted_tx_keys],
        config,
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX + "-*",
    )
    award_keys = list(set(award_keys))  # get unique list of keys
    award_keys_len = len(award_keys)
    if award_keys_len == 0:
        logger.info(
            format_log(
                f"No related awards found for deletion. Zero transaction docs found from which to derive awards.",
                action="Delete",
                name=task_id,
            )
        )
        return 0
    logger.info(
        format_log(f"Derived {award_keys_len} award keys from transactions in ES", action="Delete", name=task_id)
    )

    deleted_award_kvs = _check_awards_for_deletes(award_keys)
    deleted_award_kvs_len = len(deleted_award_kvs)
    if deleted_award_kvs_len == 0:
        # In this case it could be an award's transaction was deleted, but not THE LAST transaction of that award.
        # i.e. the deleted transaction's "siblings" are still in the DB and therefore the parent award should remain
        logger.info(
            format_log(
                f"No related awards found will be deleted. All derived awards are still in the DB.",
                action="Delete",
                name=task_id,
            )
        )
        return 0
    logger.info(
        format_log(
            f"{deleted_award_kvs_len} awards no longer in the DB will be removed from ES", action="Delete", name=task_id
        )
    )

    values_list = [v for d in deleted_award_kvs for v in d.values()]
    return delete_docs_by_unique_key(
        client,
        key=config["unique_key_field"],
        value_list=values_list,
        task_id=task_id,
        index=config["index_name"],
    )


def delete_transactions(client: Elasticsearch, config: dict, task_id: str = "Sync DB Deletes") -> int:
    """Delete all transactions in the Elasticsearch transactions index that were deleted in the source database.

    This performs the deletes of transaction documents in ES in a series of batches, as there could be many. Millions of
    transactions deleted may take a prohibitively long time, and it could be better to just re-index all documents from
    the DB instead.

    Side Effects:
        The index from which docs were deleted will be refreshed if the delete was successful
        and removed more than 0 docs.

    Args:
        client (Elasticsearch): elasticsearch-dsl client for making calls to an ES cluster
        config (dict): collection of key-value pairs that encapsulates runtime arguments for this ES management task
        task_id (str): label for this sub-step of the ETL


    Returns: Number of ES docs deleted in the index
    """
    deleted_tx_keys = _gather_deleted_transaction_keys(config)
    return delete_docs_by_unique_key(
        client,
        key=config["unique_key_field"],
        value_list=[*deleted_tx_keys],
        task_id="Sync DB Deletes",
        index=config["index_name"],
    )


def _gather_deleted_transaction_keys(config: dict) -> Optional[Dict[Union[str, Any], Dict[str, Any]]]:
    """
    Connect to S3 and gather all of the transaction ids stored in CSV files
    generated by the broker when transactions are removed from the DB.
    """

    if not config["process_deletes"]:
        logger.info(format_log(f"Skipping the S3 CSV fetch for deleted transactions", action="Delete"))
        return None

    logger.info(format_log(f"Gathering all deleted transactions from S3", action="Delete"))
    start = perf_counter()

    bucket_objects = retrieve_s3_bucket_object_list(bucket_name=config["s3_bucket"])
    logger.info(format_log(f"{len(bucket_objects):,} files found in bucket '{config['s3_bucket']}'", action="Delete"))

    if config["verbose"]:
        logger.info(format_log(f"CSV data from {config['starting_date']} to now", action="Delete"))

    filtered_csv_list = [
        x
        for x in bucket_objects
        if (x.key.endswith(".csv") and not x.key.startswith("staging") and x.last_modified >= config["starting_date"])
    ]

    if config["verbose"]:
        logger.info(format_log(f"Found {len(filtered_csv_list)} csv files", action="Delete"))

    deleted_keys = {}

    for obj in filtered_csv_list:
        object_data = access_s3_object(bucket_name=config["s3_bucket"], obj=obj)

        # Ingests the CSV into a dataframe. pandas thinks some ids are dates, so disable parsing
        data = pd.read_csv(object_data, dtype=str)

        if "detached_award_proc_unique" in data:
            new_ids = ["CONT_TX_" + x.upper() for x in data["detached_award_proc_unique"].values]
        elif "afa_generated_unique" in data:
            new_ids = ["ASST_TX_" + x.upper() for x in data["afa_generated_unique"].values]
        else:
            msg = f"[Missing valid CSV col] in {obj.key}"
            logger.error(format_log(msg, action="Delete"))
            raise RuntimeError(msg)

        for uid in new_ids:
            if uid in deleted_keys:
                if deleted_keys[uid]["timestamp"] < obj.last_modified:
                    deleted_keys[uid]["timestamp"] = obj.last_modified
            else:
                deleted_keys[uid] = {"timestamp": obj.last_modified}

    if config["verbose"]:
        for uid, deleted_dict in deleted_keys.items():
            logger.info(format_log(f"id: {uid} last modified: {deleted_dict['timestamp']}", action="Delete"))

    logger.info(
        format_log(
            f"Gathered {len(deleted_keys):,} deleted transactions from {len(filtered_csv_list)} files in "
            f"increment in {perf_counter() - start:.2f}s",
            action="Delete",
        )
    )
    return deleted_keys


def _check_awards_for_deletes(id_list: list) -> list:
    """Takes a list of award key values and returns them if they are NOT found in the awards DB table"""
    formatted_value_ids = ""
    for x in id_list:
        formatted_value_ids += "('" + x + "'),"

    sql = """
        SELECT x.generated_unique_award_id
        FROM (values {ids}) AS x(generated_unique_award_id)
        LEFT JOIN awards a ON a.generated_unique_award_id = x.generated_unique_award_id
        WHERE a.generated_unique_award_id IS NULL"""

    return execute_sql_statement(sql.format(ids=formatted_value_ids[:-1]), results=True)
