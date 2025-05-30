import json
import logging
import time

from django.conf import settings

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import INDEX_ALIASES_TO_AWARD_TYPES
from usaspending_api.broker.helpers.last_load_date import get_earliest_load_date, get_last_load_date
from usaspending_api.etl.elasticsearch_loader_helpers.utilities import format_log, is_snapshot_running

logger = logging.getLogger("script")

ES_AWARDS_UNIQUE_KEY_FIELD = "generated_unique_award_id"
ES_TRANSACTIONS_UNIQUE_KEY_FIELD = "generated_unique_transaction_id"
ES_RECIPIENT_UNIQUE_KEY_FIELD = "recipient_hash"
ES_LOCATION_UNIQUE_KEY_FIELD = ""
ES_SUBAWARD_UNIQUE_KEY_FIELD = "broker_subaward_id"


def create_index(index, client):
    try:
        does_index_exist = client.indices.exists(index)
    except Exception:
        logger.exception("Unable to query cluster for indices")
        raise SystemExit(1)
    if not does_index_exist:
        logger.info(format_log(f"Creating index '{index}'", action="Index"))
        client.indices.create(index=index)
        client.indices.refresh(index)


def put_alias(client, index, alias_name, alias_body):
    client.indices.put_alias(index, alias_name, body=alias_body)


def create_award_type_aliases(client, config):
    for award_type, award_type_codes in INDEX_ALIASES_TO_AWARD_TYPES.items():
        alias_name = f"{config['query_alias_prefix']}-{award_type}"
        if config["verbose"]:
            msg = f"Putting alias '{alias_name}' on {config['index_name']} with award codes {award_type_codes}"
            logger.info(format_log(msg, action="ES Alias"))
        alias_body = {"filter": {"terms": {"type": award_type_codes}}}
        put_alias(client, config["index_name"], alias_name, alias_body)


def create_read_alias(client, config):
    alias_name = config["query_alias_prefix"]
    logger.info(format_log(f"Putting alias '{alias_name}' on {config['index_name']}", action="ES Alias"))
    put_alias(client, config["index_name"], alias_name, {})


def create_load_alias(client, config):
    # ensure the new index is added to the alias used for incremental loads.
    # If the alias is on multiple indexes, the loads will fail!
    logger.info(format_log(f"Putting alias '{config['write_alias']}' on {config['index_name']}", action="ES Alias"))
    put_alias(client, config["index_name"], config["write_alias"], {})


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
        logger.info(format_log(message, action="ES Settings"))


def swap_aliases(client, config):
    if client.indices.get_alias(config["index_name"], "*"):
        logger.info(format_log(f"Removing old aliases for index '{config['index_name']}'", action="ES Alias"))
        client.indices.delete_alias(config["index_name"], "_all")

    alias_patterns = config["query_alias_prefix"] + "*"
    old_indexes = []

    try:
        old_indexes = list(client.indices.get_alias("*", alias_patterns).keys())
        for old_index in old_indexes:
            client.indices.delete_alias(old_index, "_all")
            logger.info(format_log(f"Removing aliases from '{old_index}'", action="ES Alias"))
    except Exception:
        logger.exception(format_log(f"No aliases found for {alias_patterns}", action="ES Alias"))

    if config["create_award_type_aliases"]:
        create_award_type_aliases(client, config)
    else:
        create_read_alias(client, config)

    create_load_alias(client, config)

    try:
        if old_indexes:
            max_wait_time = 15  # in minutes
            start_wait_time = time.time()
            is_snapshot_conflict = is_snapshot_running(client, old_indexes)
            if is_snapshot_conflict:
                logger.info(
                    format_log(
                        f"Snapshot in-progress prevents delete; waiting up to {max_wait_time} minutes",
                        action="ES Alias",
                    )
                )
            while (time.time() - start_wait_time) < (max_wait_time * 60) and is_snapshot_conflict:
                logger.info(format_log("Waiting while snapshot is in-progress", action="ES Alias"))
                time.sleep(90)
                is_snapshot_conflict = is_snapshot_running(client, old_indexes)
            if is_snapshot_conflict:
                config["raise_status_code_3"] = True
                logger.error(
                    format_log(
                        f"Unable to delete index(es) '{old_indexes}' due to in-progress snapshot", action="ES Alias"
                    )
                )
            else:
                client.indices.delete(index=old_indexes, ignore_unavailable=False)
                logger.info(format_log(f"Deleted index(es) '{old_indexes}'", action="ES Alias"))
    except Exception:
        logger.exception(format_log(f"Unable to delete indexes: {old_indexes}", action="ES Alias"))


def toggle_refresh_off(client, index):
    client.indices.put_settings({"refresh_interval": "-1"}, index)
    message = 'Set "refresh_interval": "-1" to turn auto refresh off'
    logger.info(format_log(message, action="ES Settings"))


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
    logger.info(format_log(message, action="ES Settings"))


def check_new_index_name_is_ok(provided_name: str, suffix: str) -> None:
    if not provided_name.endswith(suffix):
        raise SystemExit(f"new index name doesn't end with the expected pattern: '{suffix}'")


def check_pipeline_dates(load_type: str) -> None:
    if load_type in ("award", "transaction"):
        es_deletes_date = get_last_load_date("es_deletes")
        earliest_transaction_date = get_earliest_load_date(
            ["awards", "transaction_fabs", "transaction_fpds", "transaction_normalized"]
        )
        if es_deletes_date < earliest_transaction_date:
            msg = (
                f"The earliest Transaction / Award load date of {earliest_transaction_date} is later than the value"
                f" of 'es_deletes' ({es_deletes_date}). To reduce the amount of data loaded in the next incremental"
                " load the values of 'es_deletes', 'es_awards', and 'es_transactions' should most likely be updated to"
                f" {earliest_transaction_date} before proceeding. This recommendation assumes that the 'rpt' tables are"
                f" up to date. Optionally, this can be bypassed with the '--skip-date-check' option."
            )
            logger.error(format_log(msg, action="ES Settings"))
            raise SystemExit(msg)
