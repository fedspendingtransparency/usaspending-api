import json
import logging
import os
import pandas as pd

from collections import defaultdict
from datetime import datetime
from django.conf import settings
from django.core.management import call_command
from elasticsearch import helpers, TransportError
from time import perf_counter, sleep
from typing import Optional

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import INDEX_ALIASES_TO_AWARD_TYPES
from usaspending_api.common.helpers.s3_helpers import retrieve_s3_bucket_object_list, access_s3_object
from usaspending_api.etl.elasticsearch_loader_helpers.es_etl_utils import execute_sql_statement, format_log

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

UNIVERSAL_TRANSACTION_ID_NAME = "generated_unique_transaction_id"
UNIVERSAL_AWARD_ID_NAME = "generated_unique_award_id"


class DataJob:
    def __init__(self, *args):
        self.name = args[0]
        self.index = args[1]
        self.fy = args[2]
        self.csv = args[3]
        self.count = None


def convert_postgres_array_as_string_to_list(array_as_string: str) -> Optional[list]:
    """
        Postgres arrays are stored in CSVs as strings. Elasticsearch is able to handle lists of items, but needs to
        be passed a list instead of a string. In the case of an empty array, return null.
        For example, "{this,is,a,postgres,array}" -> ["this", "is", "a", "postgres", "array"].
    """
    return array_as_string[1:-1].split(",") if len(array_as_string) > 2 else None


def convert_postgres_json_array_as_string_to_list(json_array_as_string: str) -> Optional[dict]:
    """
        Postgres JSON arrays (jsonb) are stored in CSVs as strings. Since we want to avoid nested types
        in Elasticsearch the JSON arrays are converted to dictionaries to make parsing easier and then
        converted back into a formatted string.
    """
    if json_array_as_string is None or len(json_array_as_string) == 0:
        return None
    result = []
    json_array = json.loads(json_array_as_string)
    for j in json_array:
        for key, value in j.items():
            j[key] = "" if value is None else str(j[key])
        result.append(json.dumps(j, sort_keys=True))
    return result


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


def streaming_post_to_es(client, chunk, index_name: str, type: str, job_id=None):
    success, failed = 0, 0
    try:
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

        iteration = perf_counter()
        current_rows = f"({count * chunksize + 1:,}-{count * chunksize + len(chunk):,})"
        logger.info(
            format_log(f"ES Stream #{count} rows [{current_rows}/{job.count:,}]", job=job.name, process="ES Index")
        )
        streaming_post_to_es(client, chunk, job.index, config["load_type"], job.name)
        logger.info(
            format_log(
                f"Iteration group #{count} took {perf_counter() - iteration:.2f}s", job=job.name, process="ES Index"
            )
        )

    logger.info(
        format_log(f"Elasticsearch Index loading took {perf_counter() - start:.2f}s", job=job.name, process="ES Index")
    )


def deleted_transactions(client, config):
    deleted_ids = gather_deleted_ids(config)
    id_list = [{"key": deleted_id, "col": UNIVERSAL_TRANSACTION_ID_NAME} for deleted_id in deleted_ids]
    delete_from_es(client, id_list, None, config, None)


def deleted_awards(client, config):
    """
    so we have to find all the awards connected to these transactions,
    if we can't find the awards in the database, then we have to delete them from es
    """
    deleted_ids = gather_deleted_ids(config)
    id_list = [{"key": deleted_id, "col": UNIVERSAL_TRANSACTION_ID_NAME} for deleted_id in deleted_ids]
    award_ids = get_deleted_award_ids(client, id_list, config, settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX + "-*")
    if (len(award_ids)) == 0:
        logger.info(format_log(f"No related awards require deletion", process="ES Delete"))
        return
    deleted_award_ids = check_awards_for_deletes(award_ids)
    if len(deleted_award_ids) != 0:
        award_id_list = [
            {"key": deleted_award["generated_unique_award_id"], "col": UNIVERSAL_AWARD_ID_NAME}
            for deleted_award in deleted_award_ids
        ]
        delete_from_es(client, award_id_list, None, config, None)
    else:
        logger.info(format_log(f"No related awards require deletion", process="ES Delete"))
    return


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


def gather_deleted_ids(config):
    """
    Connect to S3 and gather all of the transaction ids stored in CSV files
    generated by the broker when transactions are removed from the DB.
    """

    if not config["process_deletes"]:
        logger.info(format_log(f"Skipping the S3 CSV fetch for deleted transactions", process="ES Delete"))
        return

    logger.info(format_log(f"Gathering all deleted transactions from S3", process="ES Delete"))
    start = perf_counter()

    bucket_objects = retrieve_s3_bucket_object_list(bucket_name=config["s3_bucket"])
    logger.info(
        format_log(f"{len(bucket_objects):,} files found in bucket '{config['s3_bucket']}'", process="ES Delete")
    )

    if config["verbose"]:
        logger.info(format_log(f"CSV data from {config['starting_date']} to now", process="ES Delete"))

    filtered_csv_list = [
        x
        for x in bucket_objects
        if (x.key.endswith(".csv") and not x.key.startswith("staging") and x.last_modified >= config["starting_date"])
    ]

    if config["verbose"]:
        logger.info(format_log(f"Found {len(filtered_csv_list)} csv files", process="ES Delete"))

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
            logger.info(format_log(f"[Missing valid col] in {obj.key}", process="ES Delete"))

        for uid in new_ids:
            if uid in deleted_ids:
                if deleted_ids[uid]["timestamp"] < obj.last_modified:
                    deleted_ids[uid]["timestamp"] = obj.last_modified
            else:
                deleted_ids[uid] = {"timestamp": obj.last_modified}

    if config["verbose"]:
        for uid, deleted_dict in deleted_ids.items():
            logger.info(format_log(f"id: {uid} last modified: {deleted_dict['timestamp']}", process="ES Delete"))

    logger.info(
        format_log(
            f"Gathering {len(deleted_ids):,} deleted transactions took {perf_counter() - start:.2f}s",
            process="ES Delete",
        )
    )
    return deleted_ids


def filter_query(column, values, query_type="match_phrase"):
    queries = [{query_type: {column: str(i)}} for i in values]
    return {"query": {"bool": {"should": [queries]}}}


def delete_query(response):
    return {"query": {"ids": {"values": [i["_id"] for i in response["hits"]["hits"]]}}}


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def delete_from_es(client, id_list, job_id, config, index=None):
    """
    id_list = [{key:'key1',col:'tranaction_id'},
               {key:'key2',col:'generated_unique_transaction_id'}],
               ...]
    or
    id_list = [{key:'key1',col:'award_id'},
               {key:'key2',col:'generated_unique_award_id'}],
               ...]
    """
    start = perf_counter()

    logger.info(format_log(f"Deleting up to {len(id_list):,} document(s)", job=job_id, process="ES Delete"))

    if index is None:
        index = f"{config['root_index']}-*"
    start_ = client.count(index=index)["count"]
    logger.info(format_log(f"Starting amount of indices ----- {start_:,}", job=job_id, process="ES Delete"))
    col_to_items_dict = defaultdict(list)
    for l in id_list:
        col_to_items_dict[l["col"]].append(l["key"])

    for column, values in col_to_items_dict.items():
        logger.info(format_log(f"Deleting {len(values):,} of '{column}'", job=job_id, process="ES Delete"))
        values_generator = chunks(values, 1000)
        for v in values_generator:
            # IMPORTANT: This delete routine looks at just 1 index at a time. If there are duplicate records across
            # multiple indexes, those duplicates will not be caught by this routine. It is left as is because at the
            # time of this comment, we are migrating to using a single index.
            body = filter_query(column, v)
            response = client.search(index=index, body=json.dumps(body), size=config["max_query_size"])
            delete_body = delete_query(response)
            try:
                client.delete_by_query(
                    index=index, body=json.dumps(delete_body), refresh=True, size=config["max_query_size"]
                )
            except Exception:
                logger.exception(format_log(f"", job=job_id, process="ES Delete"))

    end_ = client.count(index=index)["count"]
    msg = f"ES Deletes took {perf_counter() - start:.2f}s. Deleted {start_ - end_:,} records"
    logger.info(format_log(msg, job=job_id, process="ES Delete"))
    return


def get_deleted_award_ids(client, id_list, config, index=None):
    """
        id_list = [{key:'key1',col:'transaction_id'},
                   {key:'key2',col:'generated_unique_transaction_id'}],
                   ...]
     """
    if index is None:
        index = f"{config['root_index']}-*"
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
                awards = [x["_source"]["generated_unique_award_id"] for x in response["hits"]["hits"]]
    return awards


def check_awards_for_deletes(id_list):
    formatted_value_ids = ""
    for x in id_list:
        formatted_value_ids += "('" + x + "'),"

    sql = """
        SELECT x.generated_unique_award_id FROM (values {ids}) AS x(generated_unique_award_id)
        LEFT JOIN awards a ON a.generated_unique_award_id = x.generated_unique_award_id
        WHERE a.generated_unique_award_id is null"""
    results = execute_sql_statement(sql.format(ids=formatted_value_ids[:-1]), results=True)
    return results
