import logging
from datetime import datetime
from time import perf_counter
from typing import Dict, List, Optional, Union

import pandas as pd
from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.mapping import Mapping
from psycopg2 import sql as psycopg2_sql


from usaspending_api.broker.helpers.last_load_date import (
    get_last_load_date,
    get_latest_load_date,
)
from usaspending_api.common.helpers.s3_helpers import (
    access_s3_object,
    retrieve_s3_bucket_object_list,
)
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import (
    ES_AWARDS_UNIQUE_KEY_FIELD,
    ES_TRANSACTIONS_UNIQUE_KEY_FIELD,
)
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    chunks,
    execute_sql_statement,
    format_log,
)

logger = logging.getLogger("script")


def delete_docs_by_unique_key(
    client: Elasticsearch,
    key: str,
    value_list: list,
    task_id: str,
    index,
    refresh_after: bool = True,
    delete_chunk_size: int = 1000,
    slices: int | str = "auto",
    verbose: bool = False,
) -> int:
    """
    Bulk delete a batch of documents whose field identified by ``key`` matches any value provided in the
    ``values_list``.

    NOTE: This delete routine looks at just the index name given. If there are duplicate records across
    multiple indexes, an alias or wildcard should be provided for ``index`` param that covers multiple
    indices, or this will need to be run once per index.

    Args:
        client: elasticsearch-dsl client for making calls to an ES cluster
        key: name of field in targeted elasticsearch index that should have a unique value for
            every doc in the index. The field or sub-field provided MUST be of ``keyword`` type (or ``_id`` meta field)
        value_list: if key field has these values, the document will be deleted
        task_id (str): name of ES ETL job being run, used in logging
        index (str): name of index (or alias) to target for the ``_delete_by_query`` ES operation.
        refresh_after: Whether to call ``_refresh`` on the index when all of the provided values in
            ``value_list`` have been processed for delete; defaults to ``True``. If many small deletes happen at a
            rapid rate, it may be best to set this ``False`` and await a deferred refresh afterward in the calling
            code. NOTE: This param will be ignored and a refresh will be attempted if this function
            errors-out during execution, in order to not leave un-refreshed deletes in the index.
        delete_chunk_size: the batch-size of terms value-array given to each _delete_by_query call. Needs to be
            less than 65536 (max values for any terms query), and less than index.max_results_window setting. Ideally
            use ``config["partition_size"]`` (derived from --partition-size) to set this to a calibrated value. If not
            provided, uses 1000 as a safe default (10,000 resulted in some timeouts on a busy cluster).
        slices: the number of slices to use across the different shards when deleting
        verbose: determines if additional logging should be captured

    Returns: Number of ES documents deleted
    """
    start = perf_counter()

    if len(value_list) == 0:
        logger.info(format_log("Nothing to delete", action="Delete", name=task_id))
        return 0

    logger.info(
        format_log(
            f"Deleting up to {len(value_list):,} document(s)",
            action="Delete",
            name=task_id,
        )
    )
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
            base_search = Search(using=client, index=index).filter("terms", **{key: chunk_of_values})  # type: Search

            # Log the records that are returned by the Search and will ideally be deleted
            if verbose:
                response = base_search.source([key]).execute()
                ids_to_delete_list = sorted([getattr(hit, key) for hit in response])
                for ids_to_delete in ids_to_delete_list:
                    logger.info(format_log(f"- {ids_to_delete}", action="Delete", name=task_id))
            # Invoking _delete_by_query as per the elasticsearch-dsl docs:
            #   https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#delete-by-query
            # _refresh is deferred until the end of chunk processing
            # params:
            # conflicts="proceed": Ignores version conflict errors if a doc delete is attempted more than once
            # slices="auto": Will create parallel delete batches per shard
            q = base_search.params(conflicts="proceed", slices=slices)
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
) -> list[str]:
    """Derive a list of award keys given a target index, Lookup field, and lookup values

    This returns a list of all unique award keys, which are compiled from the ``ES_AWARDS_UNIQUE_KEY_FIELD`` field of
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

    award_key_set = set()
    values_generator = chunks(value_list, lookup_chunk_size)
    for chunk_of_values in values_generator:
        q = Search(using=client, index=index).filter("terms", **{lookup_key: chunk_of_values})  # type: Search
        q.update_from_dict({"size": config["max_query_size"]})
        response = q.execute()
        if response["hits"]["total"]["value"] != 0:
            award_key_set.update({x["_source"][ES_AWARDS_UNIQUE_KEY_FIELD] for x in response["hits"]["hits"]})

    if len(award_key_set) == 0:
        logger.info(
            format_log(
                "No related awards found for deletion. Zero transaction docs found from which to derive awards.",
                action="Delete",
            )
        )
    else:
        logger.info(
            format_log(
                f"Derived {len(award_key_set)} award keys from transactions in ES",
                action="Delete",
            )
        )
        if config["verbose"]:
            sorted_results = sorted(award_key_set)
            for award_id in sorted_results:
                logger.info(format_log(f"- {award_id}", action="Delete"))

    return list(award_key_set)


def delete_awards(
    client: Elasticsearch,
    config: dict,
    task_id: str = "Sync DB Deletes",
    fabs_external_data_load_date_key: str = "transaction_fabs",
    fpds_external_data_load_date_key: str = "transaction_fpds",
    spark: "pyspark.sql.SparkSession" = None,  # noqa
) -> int:
    """Delete all awards in the Elasticsearch awards index that were deleted in the source database and awards
    the were recently modified and no longer have an `action_date` on or after FY2008 (2007-10-01), since we
    don't support searching awards before this date.

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
        fabs_external_data_load_date_key (str): the key used to lookup the ``ExternalDataLoadDate`` model entry for fabs
        fpds_external_data_load_date_key: str = the key used to lookup the ``ExternalDataLoadDate`` model entry for fpds
        spark (pyspark.sql.SparkSession): provided SparkSession to be used in a Spark Cluster runtime to interact with Delta Lake
            tables as the basis of discovering award records that are no longer in the table and should be deleted.
            Presence of this variable is a marker for whether the Postgres awards table or Delta awards table should
            be interrogated.

    Returns: Number of ES docs deleted in the index
    """
    delete_window_start = get_last_load_date(
        "es_deletes", format_func=(lambda log_msg: format_log(log_msg, action="Delete"))
    )
    deleted_tx_keys = _gather_deleted_transaction_keys(
        config, delete_window_start, fabs_external_data_load_date_key, fpds_external_data_load_date_key
    )
    awards_to_delete = []

    # Recently updated awards with an `action_date` before 2007-10-01
    updated_awards_pre_fy2008 = _check_awards_for_pre_fy2008(config, delete_window_start, spark)
    awards_to_delete.extend(updated_awards_pre_fy2008)

    # While extracting unique award keys, the lookup is on transactions and must match against the unique transaction id
    award_keys = _lookup_deleted_award_keys(
        client,
        ES_TRANSACTIONS_UNIQUE_KEY_FIELD,
        deleted_tx_keys,
        config,
        settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX + "-*",
    )

    deleted_award_ids = _check_awards_for_deletes(config, award_keys, spark)
    awards_to_delete.extend(deleted_award_ids)

    if len(awards_to_delete) == 0:
        logger.info(format_log("Nothing to delete", action="Delete", name=task_id))
        return 0

    return delete_docs_by_unique_key(
        client,
        key=config["unique_key_field"],
        value_list=awards_to_delete,
        task_id=task_id,
        index=config["index_name"],
        delete_chunk_size=config["partition_size"],
        slices=config["slices"],
        verbose=config["verbose"],
    )


def delete_transactions(
    client: Elasticsearch,
    config: dict,
    task_id: str = "Sync DB Deletes",
    fabs_external_data_load_date_key: str = "transaction_fabs",
    fpds_external_data_load_date_key: str = "transaction_fpds",
    spark: "pyspark.sql.SparkSession" = None,  # noqa
) -> int:
    """Delete all transactions in the Elasticsearch transactions index that were deleted in the source database and
    transactions that were recently modified and no longer have an `action_date` on or after FY2008 (2007-10-01), since
    we don't support searching transactions before this date.

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
        fabs_external_data_load_date_key (str): the key used to lookup the ``ExternalDataLoadDate`` model entry for fabs
        fpds_external_data_load_date_key: str = the key used to lookup the ``ExternalDataLoadDate`` model entry for fpds


    Returns: Number of ES docs deleted in the index
    """

    tx_keys_to_delete = []

    delete_window_start = get_last_load_date(
        "es_deletes", format_func=(lambda log_msg: format_log(log_msg, action="Delete"))
    )

    deleted_tx_keys = _gather_deleted_transaction_keys(
        config, delete_window_start, fabs_external_data_load_date_key, fpds_external_data_load_date_key
    )
    tx_keys_to_delete.extend(deleted_tx_keys)

    pre_fy2008_transactions = _gather_modified_transactions_pre_fy2008(config, delete_window_start, spark)
    tx_keys_to_delete.extend(pre_fy2008_transactions)

    if len(tx_keys_to_delete) == 0:
        logger.info(format_log("Nothing to delete", action="Delete"))
        return 0

    return delete_docs_by_unique_key(
        client,
        key=config["unique_key_field"],
        value_list=tx_keys_to_delete,
        task_id=task_id,
        index=config["index_name"],
        delete_chunk_size=config["partition_size"],
        slices=config["slices"],
        verbose=config["verbose"],
    )


def _gather_deleted_transaction_keys(
    config: dict,
    delete_window_start: datetime,
    fabs_external_data_load_date_key: str = "fabs",
    fpds_external_data_load_date_key: str = "fpds",
) -> list[str] | None:
    """
    Connect to S3 and gather all of the transaction ids stored in CSV files
    generated by the broker when transactions are removed from the DB.

    Args:
        config (dict): collection of key-value pairs that encapsulates runtime arguments for this ES management task
        fabs_external_data_load_date_key (str): the key used to lookup the ``ExternalDataLoadDate`` model entry for fabs
        fpds_external_data_load_date_key: str = the key used to lookup the ``ExternalDataLoadDate`` model entry for fpds
    """

    if not config["process_deletes"]:
        logger.info(format_log("Skipping the S3 CSV fetch for deleted transactions", action="Delete"))
        return None

    logger.info(format_log("Gathering all deleted transactions from S3", action="Delete"))
    start = perf_counter()

    bucket_objects = retrieve_s3_bucket_object_list(bucket_name=config["s3_bucket"])
    logger.info(
        format_log(
            f"{len(bucket_objects):,} files found in bucket '{config['s3_bucket']}'",
            action="Delete",
        )
    )

    # An `end_date` is used, so we don't try to delete records from ES that have not yet
    # been deleted in postgres by the fabs/fpds loader
    end_date = get_latest_load_date(
        [fabs_external_data_load_date_key, fpds_external_data_load_date_key],
        format_func=(lambda log_msg: format_log(log_msg, action="Delete")),
    )

    logger.info(format_log(f"CSV data from {delete_window_start} to {end_date}", action="Delete"))

    filtered_csv_list = [
        x
        for x in bucket_objects
        if (
            x.key.endswith(".csv")
            and not x.key.startswith("staging")
            and delete_window_start <= x.last_modified <= end_date
        )
    ]

    logger.info(format_log(f"Found {len(filtered_csv_list)} CSV files in the date range", action="Delete"))

    deleted_keys = {}

    for obj in filtered_csv_list:
        if config["verbose"]:
            logger.info(format_log(f"- {obj.key}", action="Delete"))
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
                    deleted_keys[uid]["obj_key"] = obj.key
            else:
                deleted_keys[uid] = {"timestamp": obj.last_modified, "obj_key": obj.key}

    if config["verbose"]:
        obj_keys = set()
        sorted_keys = sorted(deleted_keys, key=lambda val: (deleted_keys[val]["obj_key"], val))
        logger.info(format_log("Most recently modified IDs found in the files above:", action="Delete"))
        for uid in sorted_keys:
            obj_key = deleted_keys[uid]["obj_key"]
            timestamp = deleted_keys[uid]["timestamp"]
            if obj_key not in obj_keys:
                obj_keys.add(obj_key)
                logger.info(format_log(f"- Object: {obj_key} | Last Modified: {timestamp}", action="Delete"))
            logger.info(format_log(f"   - ID: '{uid}'", action="Delete"))

    logger.info(
        format_log(
            f"Gathered {len(deleted_keys):,} deleted transactions from {len(filtered_csv_list)} files in "
            f"increment in {perf_counter() - start:.2f}s",
            action="Delete",
        )
    )
    return list(deleted_keys.keys())


def _gather_modified_transactions_pre_fy2008(
    config: dict,
    delete_window_start: datetime,
    spark: "pyspark.sql.SparkSession" = None,  # noqa
    transactions_table: str = "rpt.transaction_search",
) -> Union[List[Dict], List]:
    """Find all transactions that have been modified in the last `days_delta` day(s) that have an `action_date` prior to
        2007-10-01 (FY 2008) and delete them from Elasticsearch if they're present.
    This is for the cases where a transaction was originally created with a valid `action_date` value
        (2007-10-01 or later), but has since been updated to have an invalid `action_date` (pre 2007-10-01)

    Args:
        config: Collection of key-value pairs that encapsulates runtime arguments for this ES management task.
        spark: Spark session. Defaults to None.
        transactions_table: Database table with transactions. Defaults to "transaction_search".
        days_delta: How many days to go back when checking for "recently" updated transactions. Defaults to 3.

    Returns:
        List of dictionaries in the format of {"generated_unique_transaction_id": <unique transaction id>} for
        Transactions that have been updated in the past `days_delta` days or an empty list.
    """

    results = []

    if not config["process_deletes"]:
        logger.info(format_log("Skipping the FY2008 check for modified transactions", action="Delete"))
        return results

    pre_fy2008_transactions_sql = f"""
        SELECT
            CASE
                WHEN detached_award_proc_unique IS NOT NULL
                    THEN concat('CONT_TX_', detached_award_proc_unique)
                WHEN afa_generated_unique IS NOT NULL
                    THEN CONCAT('ASST_TX_', afa_generated_unique)
            END AS generated_unique_transaction_id
        FROM
            {transactions_table}
        WHERE
            etl_update_date > '{delete_window_start}'
            AND
            action_date < '2007-10-01'
        """

    if spark:
        results = [row.asDict() for row in spark.sql(pre_fy2008_transactions_sql).collect()]
    else:
        results = execute_sql_statement(
            pre_fy2008_transactions_sql,
            results=True,
        )

    results = [list(record.values())[0] for record in results]

    if len(results) > 0:
        logger.info(
            format_log(
                f"{len(results)} transactions with an action_date prior to FY 2008; these will be deleted if in ES",
                action="Delete",
            )
        )
        if config["verbose"]:
            sorted_results = sorted(results)
            for transaction_id in sorted_results:
                logger.info(format_log(f"- {transaction_id}", action="Delete"))
    else:
        logger.info(
            format_log(
                "No recently updated transactions have an action_date before FY 2008.",
                action="Delete",
            )
        )

    return results


def _check_awards_for_deletes(
    config: dict,
    id_list: list,
    spark: "pyspark.sql.SparkSession" = None,  # noqa
    awards_table: str = "vw_awards",
) -> list[str]:
    """Takes a list of award key values and returns them if they are NOT found in the awards DB table"""

    if len(id_list) == 0:
        return []

    pre_format_sql = """
        SELECT x.generated_unique_award_id
        FROM {{from_sql}} AS x(generated_unique_award_id)
        LEFT JOIN {awards_table} a ON a.generated_unique_award_id = x.generated_unique_award_id
        WHERE a.generated_unique_award_id IS NULL"""

    if spark:  # then use spark against a Delta Table
        awards_table = "int.awards"
        sql = pre_format_sql.format(awards_table=awards_table)
        df = spark.createDataFrame([[val] for val in id_list])
        results = [row.asDict() for row in spark.sql(sql, from_sql=df).collect()]
    else:
        sql = pre_format_sql.format(awards_table=awards_table).format(from_sql="(values ({ids}))")
        sql = psycopg2_sql.SQL(sql).format(ids=psycopg2_sql.SQL("), (").join(map(psycopg2_sql.Literal, id_list)))
        results = execute_sql_statement(sql, results=True)

    award_ids = [list(record.values())[0] for record in results]

    if len(award_ids) == 0:
        # In this case it could be an award's transaction was deleted, but not THE LAST transaction of that award.
        # i.e. the deleted transaction's "siblings" are still in the DB and therefore the parent award should remain
        logger.info(
            format_log(
                "No related awards found will be deleted. All derived awards are still in the DB.",
                action="Delete",
            )
        )
    else:
        logger.info(
            format_log(
                f"{len(award_ids)} awards no longer in the DB will be removed from ES if found",
                action="Delete",
            )
        )
        if config["verbose"]:
            sorted_results = sorted(award_ids)
            for award_id in sorted_results:
                logger.info(format_log(f"- {award_id}", action="Delete"))

    return award_ids


def _check_awards_for_pre_fy2008(
    config: dict,
    delete_window_start: datetime,
    spark: "pyspark.sql.SparkSession" = None,  # noqa
    awards_table: str = "rpt.award_search",
) -> list[str]:
    """
    Find all awards that have been modified since the last pipeline run that have an 'action_date' prior to
        2007-10-01 (FY 2008) and delete them from Elasticsearch if they're present.
    This is for the cases where an award was originally created with a valid `action_date` value (2007-10-01 or later),
        but has since been updated to have an invalid `action_date` (pre 2007-10-01)

    Args:
        spark: Spark session. Defaults to None.
        awards_table: Database table with awards. Defaults to "award_search".

    Returns:
        List of dictionaries in the format of {"generated_unique_award_id": <unique award id>} for Awards that have been
        updated in the past `days_delta` days or an empty list.
    """
    pre_fy2008_awards_sql = f"""
        SELECT
            generated_unique_award_id
        FROM
            {awards_table}
        WHERE
            update_date > '{delete_window_start}'
            AND
            action_date < '2007-10-01'
        """

    if spark:
        results = [row.asDict() for row in spark.sql(pre_fy2008_awards_sql).collect()]
    else:
        results = execute_sql_statement(pre_fy2008_awards_sql, results=True)

    results = [list(record.values())[0] for record in results]

    if len(results) == 0:
        logger.info(
            format_log(
                "None of the recently updated awards have an action_date prior to FY 2008.",
                action="Delete",
            )
        )
    else:
        logger.info(
            format_log(
                f"{len(results)} awards with an action_date prior to FY 2008; these will be deleted if in ES",
                action="Delete",
            )
        )
        if config["verbose"]:
            sorted_results = sorted(results)
            for transaction_id in sorted_results:
                logger.info(format_log(f"- {transaction_id}", action="Delete"))

    return results
