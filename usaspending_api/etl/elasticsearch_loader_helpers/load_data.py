import json
import logging

from django.conf import settings
from elasticsearch import helpers
from time import perf_counter

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import INDEX_ALIASES_TO_AWARD_TYPES

from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    format_log,
    convert_postgres_json_array_to_list,
)


logger = logging.getLogger("script")


def streaming_post_to_es(client, chunk, index_name: str, type: str, job_id=None):
    success, failed = 0, 0
    try:
        for ok, item in helpers.parallel_bulk(client, chunk, index=index_name):
            success = [success, success + 1][ok]
            failed = [failed + 1, failed][ok]

    except Exception as e:
        logger.exception(f"Fatal error: \n\n{str(e)[:5000]}...\n\n{'*' * 80}")
        raise RuntimeError()

    logger.info(format_log(f"Success: {success:,} | Fail: {failed:,}", job=job_id, process="ES Index"))
    return success, failed


def put_alias(client, index, alias_name, alias_body):
    client.indices.put_alias(index, alias_name, body=alias_body)


def create_aliases(client, index, load_type, silent=False):
    for award_type, award_type_codes in INDEX_ALIASES_TO_AWARD_TYPES.items():
        if load_type == "awards":
            prefix = settings.ES_AWARDS_QUERY_ALIAS_PREFIX
        else:
            prefix = settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX

        alias_name = f"{prefix}-{award_type}"
        if silent is False:
            logger.info(
                format_log(
                    f"Putting alias '{alias_name}' on {index} with award codes {award_type_codes}",
                    process="ES Alias Put",
                )
            )
        alias_body = {"filter": {"terms": {"type": award_type_codes}}}
        put_alias(client, index, alias_name, alias_body)

    # ensure the new index is added to the alias used for incremental loads.
    # If the alias is on multiple indexes, the loads will fail!
    write_alias = settings.ES_AWARDS_WRITE_ALIAS if load_type == "awards" else settings.ES_TRANSACTIONS_WRITE_ALIAS
    logger.info(format_log(f"Putting alias '{write_alias}' on {index}", process="ES Alias Put"))
    put_alias(
        client, index, write_alias, {},
    )


def set_final_index_config(client, index):
    es_settingsfile = str(settings.APP_DIR / "etl" / "es_config_objects.json")
    with open(es_settingsfile) as f:
        settings_dict = json.load(f)
    final_index_settings = settings_dict["final_index_settings"]

    current_settings = client.indices.get(index)[index]["settings"]["index"]

    client.indices.put_settings(final_index_settings, index)
    client.indices.refresh(index)
    for setting, value in final_index_settings.items():
        message = f'Changing "{setting}" from {current_settings.get(setting)} to {value}'
        logger.info(format_log(message, process="ES Settings"))


def swap_aliases(client, index, load_type):
    if client.indices.get_alias(index, "*"):
        logger.info(format_log(f"Removing old aliases for index '{index}'", process="ES Alias Drop"))
        client.indices.delete_alias(index, "_all")
    if load_type == "awards":
        alias_patterns = settings.ES_AWARDS_QUERY_ALIAS_PREFIX + "*"
    else:
        alias_patterns = settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX + "*"
    old_indexes = []

    try:
        old_indexes = list(client.indices.get_alias("*", alias_patterns).keys())
        for old_index in old_indexes:
            client.indices.delete_alias(old_index, "_all")
            logger.info(format_log(f"Removing aliases from '{old_index}'", process="ES Alias Drop"))
    except Exception:
        logger.exception(format_log(f"No aliases found for {alias_patterns}", process="ES Alias Drop"))

    create_aliases(client, index, load_type=load_type)

    try:
        if old_indexes:
            client.indices.delete(index=old_indexes, ignore_unavailable=False)
            logger.info(format_log(f"Deleted index(es) '{old_indexes}'", process="ES Alias Drop"))
    except Exception:
        logger.exception(format_log(f"Unable to delete indexes: {old_indexes}", process="ES Alias Drop"))


def transform_data(worker, records):
    logger.info(format_log(f"Transforming data", job=worker.name, process="ES Index"))
    start = perf_counter()

    # Need a specific converter to handle converting strings to correct data types (e.g. string -> array)
    converters = {
        # "business_categories": convert_postgres_array_as_string_to_list,
        # "tas_paths": convert_postgres_array_as_string_to_list,
        # "tas_components": convert_postgres_array_as_string_to_list,
        "federal_accounts": convert_postgres_json_array_to_list,
        # "disaster_emergency_fund_codes": convert_postgres_array_as_string_to_list,
    }

    for record in records:
        for field, converter in converters.items():
            record[field] = converter(record[field])
        record["routing"] = record[settings.ES_ROUTING_FIELD]
        record["_id"] = record[f"{'award' if worker.load_type == 'awards' else 'transaction'}_id"]
    # TODO: convert special fields to correct format

    # Route all documents with the same recipient to the same shard
    # This allows for accuracy and early-termination of "top N" recipient category aggregation queries
    # Recipient is are highest-cardinality category with over 2M unique values to aggregate against,
    # and this is needed for performance
    # ES helper will pop any "meta" fields like "routing" from provided data dict and use them in the action
    # df["routing"] = df[settings.ES_ROUTING_FIELD]

    # Explicitly setting the ES _id field to match the postgres PK value allows
    # bulk index operations to be upserts without creating duplicate documents

    logger.info(
        format_log(f"Data Transformation took {perf_counter() - start:.2f}s", job=worker.name, process="ES Index")
    )
    return records


def load_data(worker, records, client):
    start = perf_counter()
    logger.info(format_log(f"Starting Index operation", job=worker.name, process="ES Index"))
    streaming_post_to_es(client, records, worker.index, worker.load_type, worker.name)
    logger.info(format_log(f"Index operation took {perf_counter() - start:.2f}s", job=worker.name, process="ES Index"))


def create_index(index, client):
    try:
        does_index_exist = client.indices.exists(index)
    except Exception as e:
        logger.exception(e)
        raise SystemExit(1)
    if not does_index_exist:
        logger.info(format_log(f"Creating index '{index}'", process="ES Index"))
        client.indices.create(index=index)
        client.indices.refresh(index)
