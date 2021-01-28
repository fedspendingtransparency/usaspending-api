import json
import logging
import pandas as pd

from collections import defaultdict
from django.conf import settings
from time import perf_counter
from typing import Tuple, List, Optional

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q as ES_Q

from usaspending_api.common.helpers.s3_helpers import retrieve_s3_bucket_object_list, access_s3_object
from usaspending_api.etl.elasticsearch_loader_helpers.index_config import (
    ES_AWARDS_UNIQUE_KEY_FIELD,
    ES_TRANSACTIONS_UNIQUE_KEY_FIELD,
)
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    execute_sql_statement,
    format_log,
    chunks,
    filter_query,
)

logger = logging.getLogger("script")


def _delete_query(response: dict) -> dict:
    return {"query": {"ids": {"values": [i["_id"] for i in response["hits"]["hits"]]}}}


def _delete_from_es(
    client: Elasticsearch,
    id_list: List[dict],
    index: str,
    max_query_size: int,
    use_aliases: bool = False,
    task_id: Optional[Tuple[int, str]] = None,
) -> None:
    """
        id_list = [
            {key: 'key1', col: 'transaction_id'},
            {key: 'key2', col: 'generated_unique_transaction_id'},
            ...
        ]
        - or -
        id_list = [
            {key: 'key1', col: 'award_id'},
            {key: 'key2', col: 'generated_unique_award_id'},
            ...
        ]

    """
    start = perf_counter()
    msg = f"Deleting up to {len(id_list):,} document{'s' if len(id_list) != 1 else ''}"
    logger.info(format_log(msg, name=task_id, action="Delete"))

    if use_aliases:
        index = f"{index}-*"
    start_ = client.count(index=index)["count"]
    logger.info(format_log(f"Starting amount of indices ----- {start_:,}", name=task_id, action="Delete"))
    col_to_items_dict = defaultdict(list)
    for l in id_list:
        col_to_items_dict[l["col"]].append(l["key"])

    for column, values in col_to_items_dict.items():
        logger.info(format_log(f"Deleting {len(values):,} of '{column}'", name=task_id, action="Delete"))
        values_generator = chunks(values, 1000)
        for v in values_generator:
            # IMPORTANT: This delete routine looks at just 1 index at a time. If there are duplicate records across
            # multiple indexes, those duplicates will not be caught by this routine. It is left as is because at the
            # time of this comment, we are migrating to using a single index.
            body = filter_query(column, v)
            response = client.search(index=index, body=json.dumps(body), size=max_query_size)
            delete_body = _delete_query(response)
            try:
                client.delete_by_query(index=index, body=json.dumps(delete_body), refresh=True, size=max_query_size)
            except Exception:
                logger.exception(format_log("", name=task_id, action="Delete"))
                raise SystemExit(1)

    end_ = client.count(index=index)["count"]
    record_count = start_ - end_
    duration = perf_counter() - start
    msg = f"Delete operation took {duration:.2f}s. Removed {record_count:,} document{'s' if record_count != 1 else ''}"
    logger.info(format_log(msg, name=task_id, action="Delete"))
    return


def delete_docs_by_unique_key(client: Elasticsearch, key: str, value_list: list, task_id: str, index) -> int:
    """
    Bulk delete a batch of documents whose field identified by ``key`` matches any value provided in the
    ``values_list``.

    Args:
        client (Elasticsearch): elasticsearch-dsl client for making calls to an ES cluster
        key (str): name of filed in targeted elasticsearch index that should have a unique value for
            every doc in the index. Ideally the field or sub-field provided is of ``keyword`` type.
        value_list (list): if key field has these values, the document will be deleted
        task_id (str): name of ES ETL job being run, used in logging
        index (str): name of index (or alias) to target for the ``_delete_by_query`` ES operation.

            NOTE: This delete routine looks at just the index name given. If there are duplicate records across
            multiple indexes, an alias or wildcard should be provided for ``index`` param that covers multiple
            indices, or this will need to be run once per index.

    Returns: Number of ES documents deleted
    """
    start = perf_counter()

    if len(value_list) == 0:
        logger.info(format_log("Nothing to delete", action="Delete", name=task_id))
        return 0

    logger.info(format_log(f"Deleting up to {len(value_list):,} document(s)", action="Delete", name=task_id))
    if not index:
        raise RuntimeError("index name must be provided")

    deleted = 0
    is_error = False
    try:
        # 65,536 is max number of terms that can be added to an ES terms filter query
        values_generator = chunks(value_list, 50000)
        for chunk_of_values in values_generator:
            # Creates an Elasticsearch query criteria for the _delete_by_query call
            q = ES_Q("terms", **{key: chunk_of_values})
            # Invoking _delete_by_query as per the elasticsearch-dsl docs:
            #   https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html#delete-by-query
            response = Search(using=client, index=index).filter(q).delete()
            chunk_deletes = response["deleted"]
            deleted += chunk_deletes
    except Exception:
        is_error = True
        logger.exception(format_log("", name=task_id, action="Delete"))
        raise SystemExit(1)
    finally:
        error_text = " before encountering an error" if is_error else ""
        duration = perf_counter() - start
        docs = f"document{'s' if deleted != 1 else ''}"
        msg = f"Delete operation took {duration:.2f}s. Removed {deleted:,} {docs}{error_text}"
        logger.info(format_log(msg, action="Delete", name=task_id))

    return deleted


def _lookup_deleted_award_ids(client: Elasticsearch, id_list: list, config: dict, index: Optional[str] = None) -> list:
    """Lookup deleted transactions to derive parent awards to be deleted

    This fetches a list of all unique award keys compiled from the ``ES_AWARDS_UNIQUE_KEY_FIELD`` field of
    any document in the transaction index that matches the query, which looks up deleted transaction ES
    documents by their ``ES_TRANSACTIONS_UNIQUE_KEY_FIELD`` field.

    Args:
        client (Elasticsearch): elasticsearch-dsl client for making calls to an ES cluster
        id_list (list): A list of dictionaries, each having two keys, in this format::

            id_list = [
                {key:'<value_of_col>', col:'<unique_key_col_name>'},
                {key:'<value_of_col>', col:'<unique_key_col_name>'},
                ...,
            ]

        config (dict): collection of key-value pairs that encapsulates runtime arguments for this ES management task
        index (str): Optional name, alias, or pattern of index this query will target. Looks up via config if not
                     provided

    Returns: None
    """
    if index is None:
        index = f"{config['query_alias_prefix']}-*"
    col_to_items_dict = defaultdict(list)
    for l in id_list:
        col_to_items_dict[l["col"]].append(l["key"])
    awards = []
    for column, values in col_to_items_dict.items():
        values_generator = chunks(values, 1000)
        for v in values_generator:
            body = filter_query(column, v)
            response = client.search(index=index, body=json.dumps(body), size=config["max_query_size"])
            if response["hits"]["total"]["value"] != 0:
                awards += [x["_source"][ES_AWARDS_UNIQUE_KEY_FIELD] for x in response["hits"]["hits"]]
    return awards


def deleted_awards(client: Elasticsearch, config: dict) -> None:
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

    Returns: None
    """
    deleted_ids = _gather_deleted_ids(config)
    # While extracting unique award keys, the lookup is on transactions and must match against the unique transaction id
    id_list = [{"key": deleted_id, "col": ES_TRANSACTIONS_UNIQUE_KEY_FIELD} for deleted_id in deleted_ids]
    award_ids = _lookup_deleted_award_ids(client, id_list, config, settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX + "-*")
    if (len(award_ids)) == 0:
        logger.info(format_log(f"No related awards require deletion", action="Delete"))
        return

    deleted_award_ids = _check_awards_for_deletes(award_ids)
    if len(deleted_award_ids) == 0:
        logger.info(format_log(f"No related awards require deletion", action="Delete"))
        return

    award_id_list = [
        {"key": deleted_award[config["unique_key_field"]], "col": config["unique_key_field"]}
        for deleted_award in deleted_award_ids
    ]
    _delete_from_es(
        client,
        award_id_list,
        index=config["query_alias_prefix"],
        max_query_size=config["max_query_size"],
        use_aliases=True,
    )

    return


def deleted_transactions(client: Elasticsearch, config: dict) -> None:
    deleted_ids = _gather_deleted_ids(config)
    id_list = [{"key": deleted_id, "col": config["unique_key_field"]} for deleted_id in deleted_ids]
    _delete_from_es(
        client, id_list, index=config["query_alias_prefix"], max_query_size=config["max_query_size"], use_aliases=True
    )


def _gather_deleted_ids(config: dict) -> list:
    """
    Connect to S3 and gather all of the transaction ids stored in CSV files
    generated by the broker when transactions are removed from the DB.
    """

    if not config["process_deletes"]:
        logger.info(format_log(f"Skipping the S3 CSV fetch for deleted transactions", action="Delete"))
        return

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

    deleted_ids = {}

    for obj in filtered_csv_list:
        object_data = access_s3_object(bucket_name=config["s3_bucket"], obj=obj)

        # Ingests the CSV into a dataframe. pandas thinks some ids are dates, so disable parsing
        data = pd.read_csv(object_data, dtype=str)

        if "detached_award_proc_unique" in data:
            new_ids = ["CONT_TX_" + x.upper() for x in data["detached_award_proc_unique"].values]
        elif "afa_generated_unique" in data:
            new_ids = ["ASST_TX_" + x.upper() for x in data["afa_generated_unique"].values]
        else:
            logger.info(format_log(f"[Missing valid col] in {obj.key}", action="Delete"))

        for uid in new_ids:
            if uid in deleted_ids:
                if deleted_ids[uid]["timestamp"] < obj.last_modified:
                    deleted_ids[uid]["timestamp"] = obj.last_modified
            else:
                deleted_ids[uid] = {"timestamp": obj.last_modified}

    if config["verbose"]:
        for uid, deleted_dict in deleted_ids.items():
            logger.info(format_log(f"id: {uid} last modified: {deleted_dict['timestamp']}", action="Delete"))

    logger.info(
        format_log(
            f"Gathering {len(deleted_ids):,} deleted transactions took {perf_counter() - start:.2f}s", action="Delete",
        )
    )
    return deleted_ids


def _check_awards_for_deletes(id_list: list) -> list:
    formatted_value_ids = ""
    for x in id_list:
        formatted_value_ids += "('" + x + "'),"

    sql = """
        SELECT x.generated_unique_award_id
        FROM (values {ids}) AS x(generated_unique_award_id)
        LEFT JOIN awards a ON a.generated_unique_award_id = x.generated_unique_award_id
        WHERE a.generated_unique_award_id IS NULL"""

    return execute_sql_statement(sql.format(ids=formatted_value_ids[:-1]), results=True)
