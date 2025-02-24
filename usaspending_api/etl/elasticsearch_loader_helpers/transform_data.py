import logging
from time import perf_counter
from typing import Callable, Dict, List, Optional

from django.conf import settings

from usaspending_api.etl.elasticsearch_loader_helpers import aggregate_key_functions as funcs
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    TaskSpec,
    convert_json_array_to_list_of_str,
    convert_json_data_to_dict,
    format_log,
)

logger = logging.getLogger("script")


def transform_award_data(worker: TaskSpec, records: List[dict]) -> List[dict]:
    converters = {
        "covid_spending_by_defc": convert_json_data_to_dict,
        "iija_spending_by_defc": convert_json_data_to_dict,
        "federal_accounts": convert_json_array_to_list_of_str,
        "program_activities": convert_json_data_to_dict,
    }
    # TODO: Move some of the 1:1 agg_keys that match a field already on Elasticsearch
    agg_key_creations = {
        "awarding_subtier_agency_agg_key": lambda x: x["awarding_subtier_agency_code"],
        "awarding_toptier_agency_agg_key": lambda x: x["awarding_toptier_agency_code"],
        "funding_subtier_agency_agg_key": lambda x: x["funding_subtier_agency_code"],
        "funding_toptier_agency_agg_key": lambda x: x["funding_toptier_agency_code"],
        "naics_agg_key": lambda x: x["naics_code"],
        "psc_agg_key": lambda x: x["product_or_service_code"],
        "defc_agg_key": lambda x: x["disaster_emergency_fund_codes"],
        "cfda_agg_key": lambda x: x["cfda_number"],
        "pop_congressional_agg_key": funcs.pop_congressional_agg_key,
        "pop_congressional_cur_agg_key": funcs.pop_congressional_cur_agg_key,
        "pop_county_agg_key": funcs.pop_county_agg_key,
        "pop_state_agg_key": lambda x: x["pop_state_code"],
        "pop_country_agg_key": lambda x: x["pop_country_code"],
        "recipient_agg_key": funcs.award_recipient_agg_key,
        "recipient_location_congressional_agg_key": funcs.recipient_location_congressional_agg_key,
        "recipient_location_congressional_cur_agg_key": funcs.recipient_location_congressional_cur_agg_key,
        "recipient_location_county_agg_key": funcs.recipient_location_county_agg_key,
        "recipient_location_state_agg_key": lambda x: x["recipient_location_state_code"],
        "recipient_location_country_agg_key": lambda x: x["recipient_location_country_code"],
    }
    drop_fields = [
        "recipient_levels",
        "funding_toptier_agency_id",
        "funding_subtier_agency_id",
        "recipient_location_state_name",
        "recipient_location_state_fips",
        "recipient_location_state_population",
        "recipient_location_county_population",
        "recipient_location_congressional_population",
        "pop_state_name",
        "pop_state_fips",
        "pop_state_population",
        "pop_county_population",
        "pop_congressional_population",
    ]
    return transform_data(worker, records, converters, agg_key_creations, drop_fields, settings.ES_ROUTING_FIELD)


def transform_transaction_data(worker: TaskSpec, records: List[dict]) -> List[dict]:
    converters = {
        "federal_accounts": convert_json_array_to_list_of_str,
        "program_activities": convert_json_data_to_dict,
    }
    # TODO: Move some of the 1:1 agg_keys that match a field already on Elasticsearch
    agg_key_creations = {
        "recipient_agg_key": funcs.transaction_recipient_agg_key,
        "awarding_subtier_agency_agg_key": lambda x: x["awarding_sub_tier_agency_c"],
        "awarding_toptier_agency_agg_key": lambda x: x["awarding_agency_code"],
        "funding_subtier_agency_agg_key": lambda x: x["funding_sub_tier_agency_co"],
        "funding_toptier_agency_agg_key": lambda x: x["funding_agency_code"],
        "naics_agg_key": lambda x: x["naics_code"],
        "psc_agg_key": lambda x: x["product_or_service_code"],
        "defc_agg_key": lambda x: x["disaster_emergency_fund_codes"],
        "cfda_agg_key": lambda x: x["cfda_number"],
        "pop_country_agg_key": lambda x: x["pop_country_code"],
        "pop_state_agg_key": lambda x: x["pop_state_code"],
        "pop_county_agg_key": funcs.pop_county_agg_key,
        "pop_congressional_agg_key": funcs.pop_congressional_agg_key,
        "pop_congressional_cur_agg_key": funcs.pop_congressional_cur_agg_key,
        "recipient_location_state_agg_key": lambda x: x["recipient_location_state_code"],
        "recipient_location_congressional_agg_key": funcs.recipient_location_congressional_agg_key,
        "recipient_location_congressional_cur_agg_key": funcs.recipient_location_congressional_cur_agg_key,
        "recipient_location_county_agg_key": funcs.recipient_location_county_agg_key,
        "recipient_location_country_agg_key": lambda x: x["recipient_location_country_code"],
    }
    drop_fields = [
        "pop_state_name",
        "pop_state_fips",
        "pop_state_population",
        "pop_county_population",
        "pop_congressional_population",
        "recipient_location_state_name",
        "recipient_location_state_fips",
        "recipient_location_state_population",
        "recipient_location_county_population",
        "recipient_location_congressional_population",
        "recipient_levels",
        "funding_toptier_agency_id",
    ]
    return transform_data(worker, records, converters, agg_key_creations, drop_fields, settings.ES_ROUTING_FIELD)


def transform_subaward_data(worker: TaskSpec, records: List[dict]) -> List[dict]:
    converters = {
        "program_activities": convert_json_data_to_dict,
    }
    agg_key_creations = {
        "sub_pop_congressional_cur_agg_key": funcs.pop_congressional_cur_agg_key,
        "sub_pop_county_agg_key": funcs.pop_county_agg_key,
        "sub_recipient_location_congressional_cur_agg_key": funcs.recipient_location_congressional_cur_agg_key,
        "sub_recipient_location_county_agg_key": funcs.recipient_location_county_agg_key,
    }
    drop_fields = []

    return transform_data(worker, records, converters, agg_key_creations, drop_fields, None)


def transform_data(
    worker: TaskSpec,
    records: List[dict],
    converters: Dict[str, Callable],
    agg_key_creations: Dict[str, Callable],
    drop_fields: List[str],
    routing_field: Optional[str] = None,
) -> List[dict]:
    logger.info(format_log("Transforming data", name=worker.name, action="Transform"))

    start = perf_counter()

    for record in records:
        for field, converter in converters.items():
            record[field] = converter(record[field])
        for key, transform_func in agg_key_creations.items():
            record[key] = transform_func(record)

        # Route all documents with the same recipient to the same shard
        # This allows for accuracy and early-termination of "top N" recipient category aggregation queries
        # Recipient is are highest-cardinality category with over 2M unique values to aggregate against,
        # and this is needed for performance
        # ES helper will pop any "meta" fields like "routing" from provided data dict and use them in the action
        if routing_field:
            record["routing"] = record[routing_field]

        # Explicitly setting the ES _id field to match the postgres PK value allows
        # bulk index operations to be upserts without creating duplicate documents
        # IF and ONLY IF a routing meta field is not also provided (one whose value differs
        # from the doc _id field). If explicit routing is done, UPSERTs may cause duplicates,
        # so docs must be deleted before UPSERTed. (More info in streaming_post_to_es(...))
        record["_id"] = record[worker.field_for_es_id]

        # Removing data which were used for creating aggregate keys and aren't necessary standalone
        for key in drop_fields:
            record.pop(key)

    duration = perf_counter() - start
    logger.info(format_log(f"Transformation operation took {duration:.2f}s", name=worker.name, action="Transform"))
    return records
