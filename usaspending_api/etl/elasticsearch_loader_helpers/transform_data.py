import logging

from django.conf import settings
from time import perf_counter

from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    convert_postgres_json_array_to_list,
    format_log,
    create_agg_key,
)


logger = logging.getLogger("script")


def transform_award_data(worker, records):
    converters = {}
    agg_keys = [
        "cfda_agg_key",
        "funding_subtier_agency_agg_key",
        "funding_toptier_agency_agg_key",
        "pop_county_agg_key",
        "pop_congressional_agg_key",
        "pop_state_agg_key",
        "recipient_agg_key",
        "recipient_location_county_agg_key",
        "recipient_location_congressional_agg_key",
        "recipient_location_state_agg_key",
    ]
    return transform_data(worker, records, converters, agg_keys)


def transform_transaction_data(worker, records):
    converters = {
        "federal_accounts": convert_postgres_json_array_to_list,
    }
    agg_keys = [
        "awarding_subtier_agency_agg_key",
        "awarding_toptier_agency_agg_key",
        "cfda_agg_key",
        "funding_subtier_agency_agg_key",
        "funding_toptier_agency_agg_key",
        "naics_agg_key",
        "pop_county_agg_key",
        "pop_congressional_agg_key",
        "pop_state_agg_key",
        "pop_country_agg_key",
        "psc_agg_key",
        "recipient_agg_key",
        "recipient_location_county_agg_key",
        "recipient_location_congressional_agg_key",
        "recipient_location_state_agg_key",
    ]
    return transform_data(worker, records, converters, agg_keys)


def transform_data(worker, records, converters, agg_keys):
    logger.info(format_log(f"Transforming data", job=worker.name, process="Index"))
    start = perf_counter()

    for record in records:
        for field, converter in converters.items():
            record[field] = converter(record[field])
        for key in agg_keys:
            record[key] = create_agg_key(key, record)

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

    logger.info(format_log(f"Data Transformation took {perf_counter() - start:.2f}s", job=worker.name, process="Index"))
    return records
