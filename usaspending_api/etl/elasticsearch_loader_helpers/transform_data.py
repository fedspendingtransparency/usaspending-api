import logging
import pandas as pd

from django.conf import settings
from time import perf_counter
from typing import Callable, Dict, List, Optional

from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    convert_postgres_json_array_to_list,
    format_log,
    TaskSpec,
)


logger = logging.getLogger("script")


def transform_award_data(worker: TaskSpec, records: List[dict]) -> List[dict]:
    converters = {}
    return transform_data(worker, records, converters, settings.ES_ROUTING_FIELD)


def transform_transaction_data(worker: TaskSpec, records: List[dict]) -> List[dict]:
    converters = {
        "federal_accounts": convert_postgres_json_array_to_list,
    }
    return transform_data(worker, records, converters, settings.ES_ROUTING_FIELD)


def transform_covid19_faba_data(worker: TaskSpec, records: List[dict]) -> List[dict]:
    # converters = {}
    # return transform_data(worker, records, converters, None)

    group_by_fields = [
        "financial_account_distinct_award_key",
        "award_id",
        "award_type",
        "generated_unique_award_id",
        "total_loan_value",
    ]

    fields_in_group = [col for col in records.columns if col not in group_by_fields]

    df = records.where(pd.notnull(records), None)
    df = df.groupby(group_by_fields, dropna=False)[fields_in_group].apply(lambda x: x.to_dict(orient="records"))
    df = df.reset_index()
    df = df.where(pd.notnull(df), None)
    df = df.rename(columns={0: "financial_accounts_by_award"})

    return df.to_dict(orient="records")


def transform_data(
    worker: TaskSpec, records: List[dict], converters: Dict[str, Callable], routing_field: Optional[str] = None
) -> List[dict]:
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
        if routing_field:
            record["routing"] = record[routing_field]

        # Explicitly setting the ES _id field to match the postgres PK value allows
        # bulk index operations to be upserts without creating duplicate documents
        # IF and ONLY IF a routing meta field is not also provided (one whose value differs
        # from the doc _id field). If explicit routing is done, UPSERTs may cause duplicates,
        # so docs must be deleted before UPSERTed. (More info in streaming_post_to_es(...))
        record["_id"] = record[worker.field_for_es_id]

    duration = perf_counter() - start
    logger.info(format_log(f"Transformation operation took {duration:.2f}s", name=worker.name, action="Index"))
    return records
