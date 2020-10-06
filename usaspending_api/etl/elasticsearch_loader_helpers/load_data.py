import json
import logging
import os
import pandas as pd

from datetime import datetime
from django.conf import settings
from django.core.management import call_command
from elasticsearch import helpers, TransportError
from time import perf_counter, sleep

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import INDEX_ALIASES_TO_AWARD_TYPES

from usaspending_api.etl.elasticsearch_loader_helpers.delete_data import delete_docs_by_unique_key
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import (
    format_log,
    convert_postgres_array_as_string_to_list,
    convert_postgres_json_array_as_string_to_list,
)


logger = logging.getLogger("script")

VIEW_COLUMNS = [
    "transaction_id",
    "detached_award_proc_unique",
    "afa_generated_unique",
    "generated_unique_transaction_id",
    "display_award_id",
    "update_date",
    "modification_number",
    "generated_unique_award_id",
    "award_id",
    "piid",
    "fain",
    "uri",
    "award_description",
    "product_or_service_code",
    "product_or_service_description",
    "psc_agg_key",
    "naics_code",
    "naics_description",
    "naics_agg_key",
    "type_description",
    "award_category",
    "recipient_unique_id",
    "recipient_name",
    "recipient_hash",
    "recipient_agg_key",
    "parent_recipient_unique_id",
    "parent_recipient_name",
    "parent_recipient_hash",
    "action_date",
    "fiscal_action_date",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "ordering_period_end_date",
    "transaction_fiscal_year",
    "award_fiscal_year",
    "award_amount",
    "transaction_amount",
    "face_value_loan_guarantee",
    "original_loan_subsidy_cost",
    "generated_pragmatic_obligation",
    "awarding_agency_id",
    "funding_agency_id",
    "awarding_toptier_agency_name",
    "funding_toptier_agency_name",
    "awarding_subtier_agency_name",
    "funding_subtier_agency_name",
    "awarding_toptier_agency_abbreviation",
    "funding_toptier_agency_abbreviation",
    "awarding_subtier_agency_abbreviation",
    "funding_subtier_agency_abbreviation",
    "awarding_toptier_agency_agg_key",
    "funding_toptier_agency_agg_key",
    "awarding_subtier_agency_agg_key",
    "funding_subtier_agency_agg_key",
    "cfda_number",
    "cfda_title",
    "cfda_agg_key",
    "type_of_contract_pricing",
    "type_set_aside",
    "extent_competed",
    "type",
    "pop_country_code",
    "pop_country_name",
    "pop_state_code",
    "pop_county_code",
    "pop_county_name",
    "pop_zip5",
    "pop_congressional_code",
    "pop_city_name",
    "pop_county_agg_key",
    "pop_congressional_agg_key",
    "pop_state_agg_key",
    "pop_country_agg_key",
    "recipient_location_country_code",
    "recipient_location_country_name",
    "recipient_location_state_code",
    "recipient_location_county_code",
    "recipient_location_county_name",
    "recipient_location_zip5",
    "recipient_location_congressional_code",
    "recipient_location_city_name",
    "recipient_location_county_agg_key",
    "recipient_location_congressional_agg_key",
    "recipient_location_state_agg_key",
    "tas_paths",
    "tas_components",
    "federal_accounts",
    "business_categories",
    "disaster_emergency_fund_codes",
]
AWARD_VIEW_COLUMNS = [
    "award_id",
    "generated_unique_award_id",
    "display_award_id",
    "category",
    "type",
    "type_description",
    "piid",
    "fain",
    "uri",
    "total_obligation",
    "description",
    "award_amount",
    "total_subsidy_cost",
    "total_loan_value",
    "update_date",
    "recipient_name",
    "recipient_hash",
    "recipient_agg_key",
    "recipient_unique_id",
    "parent_recipient_unique_id",
    "business_categories",
    "action_date",
    "fiscal_year",
    "last_modified_date",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "date_signed",
    "ordering_period_end_date",
    "original_loan_subsidy_cost",
    "face_value_loan_guarantee",
    "awarding_agency_id",
    "funding_agency_id",
    "awarding_toptier_agency_name",
    "funding_toptier_agency_name",
    "awarding_subtier_agency_name",
    "funding_subtier_agency_name",
    "awarding_toptier_agency_code",
    "funding_toptier_agency_code",
    "awarding_subtier_agency_code",
    "funding_subtier_agency_code",
    "funding_toptier_agency_agg_key",
    "funding_subtier_agency_agg_key",
    "recipient_location_country_code",
    "recipient_location_country_name",
    "recipient_location_state_code",
    "recipient_location_county_code",
    "recipient_location_county_name",
    "recipient_location_congressional_code",
    "recipient_location_zip5",
    "recipient_location_city_name",
    "recipient_location_county_agg_key",
    "recipient_location_congressional_agg_key",
    "recipient_location_state_agg_key",
    "pop_country_code",
    "pop_country_name",
    "pop_state_code",
    "pop_county_code",
    "pop_county_name",
    "pop_zip5",
    "pop_congressional_code",
    "pop_city_name",
    "pop_city_code",
    "pop_county_agg_key",
    "pop_congressional_agg_key",
    "pop_state_agg_key",
    "cfda_number",
    "cfda_title",
    "cfda_agg_key",
    "sai_number",
    "type_of_contract_pricing",
    "extent_competed",
    "type_set_aside",
    "product_or_service_code",
    "product_or_service_description",
    "naics_code",
    "naics_description",
    "tas_paths",
    "tas_components",
    "disaster_emergency_fund_codes",
    "total_covid_obligation",
    "total_covid_outlay",
]

COUNT_FY_SQL = """
SELECT COUNT(*) AS count
FROM {view}
WHERE {fiscal_year_field}={fy} AND update_date >= '{update_date}'
"""

COUNT_SQL = """
SELECT COUNT(*) AS count
FROM {view}
WHERE update_date >= '{update_date}'
"""

COPY_SQL = """"COPY (
    SELECT *
    FROM {view}
    WHERE {fiscal_year_field}={fy} AND update_date >= '{update_date}'
) TO STDOUT DELIMITER ',' CSV HEADER" > '{filename}'
"""

# ==============================================================================
# Other Globals
# ==============================================================================

AWARD_DESC_CATEGORIES = {
    "loans": "loans",
    "grant": "grants",
    "insurance": "other",
    "other": "other",
    "contract": "contracts",
    "direct payment": "directpayments",
}


def csv_chunk_gen(filename, chunksize, job_id, load_type):
    logger.info(format_log(f"Opening {filename} (batch size = {chunksize:,})", job=job_id, process="ES Index"))
    # Need a specific converter to handle converting strings to correct data types (e.g. string -> array)
    converters = {
        "business_categories": convert_postgres_array_as_string_to_list,
        "tas_paths": convert_postgres_array_as_string_to_list,
        "tas_components": convert_postgres_array_as_string_to_list,
        "federal_accounts": convert_postgres_json_array_as_string_to_list,
        "disaster_emergency_fund_codes": convert_postgres_array_as_string_to_list,
    }
    # Panda's data type guessing causes issues for Elasticsearch. Explicitly cast using dictionary
    column_list = AWARD_VIEW_COLUMNS if load_type == "awards" else VIEW_COLUMNS
    dtype = {k: str for k in column_list if k not in converters}
    for file_df in pd.read_csv(filename, dtype=dtype, converters=converters, header=0, chunksize=chunksize):
        file_df = file_df.where(cond=(pd.notnull(file_df)), other=None)
        # Route all documents with the same recipient to the same shard
        # This allows for accuracy and early-termination of "top N" recipient category aggregation queries
        # Recipient is are highest-cardinality category with over 2M unique values to aggregate against,
        # and this is needed for performance
        # ES helper will pop any "meta" fields like "routing" from provided data dict and use them in the action
        file_df["routing"] = file_df[settings.ES_ROUTING_FIELD]

        # Explicitly setting the ES _id field to match the postgres PK value allows
        # bulk index operations to be upserts without creating duplicate documents
        file_df["_id"] = file_df[f"{'award' if load_type == 'awards' else 'transaction'}_id"]
        yield file_df.to_dict(orient="records")


def es_data_loader(client, fetch_jobs, done_jobs, config):
    if config["create_new_index"]:
        # ensure template for index is present and the latest version
        call_command("es_configure", "--template-only", f"--load-type={config['load_type']}")
    while True:
        if not done_jobs.empty():
            job = done_jobs.get_nowait()
            if job.name is None:
                break

            logger.info(format_log(f"Starting new job", job=job.name, process="ES Index"))
            post_to_elasticsearch(client, job, config)
            if os.path.exists(job.csv):
                os.remove(job.csv)
        else:
            logger.info(format_log(f"No Job. Sleeping {config['ingest_wait']}s", process="ES Index"))
            sleep(int(config["ingest_wait"]))

    logger.info(format_log(f"Completed Elasticsearch data load", process="ES Index"))
    return


def streaming_post_to_es(
    client, chunk, index_name: str, type: str, job_id=None, delete_before_index=True, delete_key="_id"
):
    """
    Called this repeatedly with successive chunks of data to pump into an Elasticsearch index.

    Args:
        client: Elasticsearch client
        chunk (List[dict]): list of dictionary objects holding field_name:value data
        index_name (str): name of targetted index
        type (str): indexed data type (e.g. awards or transactions)
        job_id (str): name of ES ETL job being run, used in logging
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
            delete_docs_by_unique_key(client, delete_key, value_list, job_id, index_name)
        for ok, item in helpers.parallel_bulk(client, chunk, index=index_name):
            success = [success, success + 1][ok]
            failed = [failed + 1, failed][ok]

    except Exception as e:
        logger.exception(f"Fatal error: \n\n{str(e)[:5000]}...\n\n{'*' * 80}")
        raise SystemExit(1)

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
        message = f'Changed "{setting}" from {current_settings.get(setting)} to {value}'
        logger.info(format_log(message, process="ES Settings"))


def toggle_refresh_off(client, index):
    client.indices.put_settings({"refresh_interval": "-1"}, index)
    message = (
        f'Set "refresh_interval": "-1" to turn auto refresh off during incremental load. Manual refreshes will '
        f"occur for each batch completion."
    )
    logger.info(format_log(message, process="ES Settings"))


def toggle_refresh_on(client, index):
    response = client.indices.get(index)
    aliased_index_name = list(response.keys())[0]
    current_refresh_interval = response[aliased_index_name]["settings"]["index"]["refresh_interval"]
    es_settingsfile = str(settings.APP_DIR / "etl" / "es_config_objects.json")
    with open(es_settingsfile) as f:
        settings_dict = json.load(f)
    final_refresh_interval = settings_dict["final_index_settings"]["refresh_interval"]
    client.indices.put_settings({"refresh_interval": final_refresh_interval}, index)
    message = f'Changed "refresh_interval" from {current_refresh_interval} to {final_refresh_interval}'
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


def post_to_elasticsearch(client, job, config, chunksize=250000):
    logger.info(format_log(f"Populating ES Index '{job.index}'", job=job.name, process="ES Index"))
    start = perf_counter()
    try:
        does_index_exist = client.indices.exists(job.index)
    except Exception as e:
        print(e)
        raise SystemExit(1)
    if not does_index_exist:
        logger.info(format_log(f"Creating index '{job.index}'", job=job.name, process="ES Index"))
        client.indices.create(index=job.index)
        client.indices.refresh(job.index)

    csv_generator = csv_chunk_gen(job.csv, chunksize, job.name, config["load_type"])
    for count, chunk in enumerate(csv_generator):
        if len(chunk) == 0:
            logger.info(format_log(f"No documents to add/delete for chunk #{count}", job=job.name, process="ES Index"))
            continue

        # Only delete before adding/inserting/indexing new docs on incremental loads, not full reindexes
        is_incremental = config["is_incremental_load"] and str(config["is_incremental_load"]).lower() == "true"

        iteration = perf_counter()
        current_rows = f"({count * chunksize + 1:,}-{count * chunksize + len(chunk):,})"
        logger.info(
            format_log(f"ES Stream #{count} rows [{current_rows}/{job.count:,}]", job=job.name, process="ES Index")
        )
        streaming_post_to_es(
            client, chunk, job.index, config["load_type"], job.name, delete_before_index=is_incremental
        )
        if is_incremental:
            # refresh_interval is off during incremental loads.
            # Manually refresh after delete + insert complete for search consistency
            client.indices.refresh(job.index)
        logger.info(
            format_log(
                f"Iteration group #{count} took {perf_counter() - iteration:.2f}s", job=job.name, process="ES Index"
            )
        )

    logger.info(
        format_log(f"Elasticsearch Index loading took {perf_counter() - start:.2f}s", job=job.name, process="ES Index")
    )


def take_snapshot(client, index, repository):
    snapshot_name = f"{index}-{str(datetime.now().date())}"
    try:
        client.snapshot.create(repository, snapshot_name, body={"indices": index})
        logger.info(
            format_log(
                f"Taking snapshot INDEX: '{index}' SNAPSHOT: '{snapshot_name}' REPO: '{repository}'",
                process="ES Snapshot",
            )
        )
    except TransportError:
        logger.exception(format_log(f"SNAPSHOT FAILED", process="ES Snapshot"))
        raise SystemExit(1)
