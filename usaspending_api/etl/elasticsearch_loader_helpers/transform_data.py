import logging

from django.conf import settings
from time import perf_counter

from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    convert_postgres_json_array_to_list,
    format_log,
)


logger = logging.getLogger("script")


def transform_award_data(worker, records):
    converters = {}
    return transform_data(worker, records, converters)


def transform_transaction_data(worker, records):
    converters = {
        "federal_accounts": convert_postgres_json_array_to_list,
    }
    return transform_data(worker, records, converters)


def transform_data(worker, records, converters):
    logger.info(format_log(f"Transforming data", name=worker.name, action="Index"))
    start = perf_counter()

    for record in records:
        for field, converter in converters.items():
            record[field] = converter(record[field])

        # Route all documents with the same recipient to the same shard
        # This allows for accuracy and early-termination of "top N" recipient category aggregation queries
        # Recipient is are highest-cardinality category with over 2M unique values to aggregate against,
        # and this is needed for performance
        # ES helper will pop any "meta" fields like "routing" from provided data dict and use them in the action
        record["routing"] = record[settings.ES_ROUTING_FIELD]

        # Explicitly setting the ES _id field to match the postgres PK value allows
        # bulk index operations to be upserts without creating duplicate documents
        record["_id"] = record[worker.primary_key]

        # TODO: convert special fields to correct format ????

    logger.info(format_log(f"Data Transformation took {perf_counter() - start:.2f}s", name=worker.name, action="Index"))
    return records
