import logging

from django.conf import settings
from django.core.management import call_command
from math import ceil
from multiprocessing import Pool
from random import choice

from usaspending_api.broker.helpers.last_load_date import update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.etl.elasticsearch_loader_helpers import (
    deleted_transactions,
    deleted_awards,
    format_log,
    set_final_index_config,
    swap_aliases,
    count_of_records_to_process,
    WorkerNode,
    gen_random_name,
    EXTRACT_SQL,
    extract_records,
    transform_data,
    load_data,
    create_index,
)

logger = logging.getLogger("script")


class Controller:
    def __init__(self, config, elasticsearch_client):
        """Set values based on env vars and when the script started"""
        self.config = config

    def prepare_for_etl(self):
        logger.info(format_log("Assessing data to process"))
        self.record_count = count_of_records_to_process(self.config, True)
        self.partition_size = self.calculate_partition_size(self.record_count, self.config["batch_size"])
        self.number_of_jobs = ceil(self.record_count / self.partition_size)

        self.config["workers"] = min(self.config["workers"], self.number_of_jobs)

        logger.info(
            format_log(
                f"Hailing {self.number_of_jobs:,} heros"
                f" to handle {self.record_count:,} {self.config['load_type']} records"
                f" in squad{'s' if self.config['workers'] > 1 else ' size'} of {self.config['workers']:,}"
            )
        )

        self.workers = [self.create_worker(j) for j in range(self.number_of_jobs)]

        if self.config["create_new_index"]:
            # ensure template for index is present and the latest version
            call_command("es_configure", "--template-only", f"--load-type={self.config['load_type']}")
            create_index(self.config["index_name"], instantiate_elasticsearch_client())

    def launch_workers(self):
        with Pool(self.config["workers"]) as pool:
            pool.map(self.extract_transform_load, self.workers)

    @staticmethod
    def extract_transform_load(worker):
        random_verb = choice(
            ["skillfully", "deftly", "expertly", "readily", "quickly", "nimbly", "agilely", "casually", "easily"]
        )
        logger.info(format_log(f"{worker.name.upper()} {random_verb} enters the arena", job=worker.name))

        client = instantiate_elasticsearch_client()
        try:
            records = transform_data(worker, extract_records(worker))
            load_data(worker, records, client)
        except Exception as e:
            logger.exception(format_log(f"{worker.name} was lost in battle.", job=worker.name))
            raise e
        else:
            msg = f"{worker.name} completed the mission like a {choice(['pro', 'champ', 'boss'])}."
            logger.info(format_log(msg, job=worker.name))

    def complete_process(self) -> None:
        if self.config["create_new_index"]:
            set_final_index_config(self.elasticsearch_client, self.config["index_name"])
            if self.config["skip_delete_index"]:
                logger.info(format_log("Skipping deletion of old indices"))
            else:
                logger.info(format_log("Closing old indices and adding aliases"))
                swap_aliases(self.elasticsearch_client, self.config["index_name"], self.config["load_type"])

        if self.config["is_incremental_load"]:
            logger.info(
                format_log(f"Storing datetime {self.config['processing_start_datetime']} for next incremental load")
            )
            update_last_load_date(f"es_{self.config['load_type']}", self.config["processing_start_datetime"])

    @staticmethod
    def calculate_partition_size(record_count: int, max_size: int) -> int:
        """Create partion size with max size of max_size"""
        return ceil(record_count / ceil(record_count / max_size))

    def create_worker(self, number: int) -> WorkerNode:
        if self.config["load_type"] == "awards":
            id_col = "award_id"
            view = settings.ES_AWARDS_ETL_VIEW_NAME
            transform_func = None
        else:
            id_col = "transaction_id"
            view = settings.ES_TRANSACTIONS_ETL_VIEW_NAME
            transform_func = None

        sql = EXTRACT_SQL.format(
            id_col=id_col,
            view=view,
            update_date=self.config["starting_date"],
            divisor=self.number_of_jobs,
            remainder=number,
        )

        return WorkerNode(
            index=self.config["index_name"],
            load_type=self.config["load_type"],
            name=next(gen_random_name()),
            sql=sql,
            transform_func=transform_func,
        )
