import logging
from math import ceil
from time import perf_counter
from typing import List, Tuple

from django.conf import settings
from pyspark.sql import SparkSession

from usaspending_api.broker.helpers.last_load_date import get_earliest_load_date, update_last_load_date
from usaspending_api.common.elasticsearch.client import instantiate_elasticsearch_client
from usaspending_api.common.etl.spark import build_ref_table_name_list, create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import clean_postgres_sql_for_spark_sql
from usaspending_api.etl.elasticsearch_loader_helpers import (
    TaskSpec,
    count_of_records_to_process_in_delta,
    delete_awards,
    delete_transactions,
    format_log,
    load_data,
    obtain_extract_all_partitions_sql,
)
from usaspending_api.etl.elasticsearch_loader_helpers.controller import AbstractElasticsearchIndexerController

logger = logging.getLogger("script")


class DeltaLakeElasticsearchIndexerController(AbstractElasticsearchIndexerController):
    """Controller for Spark-based Elasticsearch ETL that extracts data from Delta Lake"""

    def __init__(self, config: dict, spark: SparkSession, spark_created_by_command: bool = False):
        super(DeltaLakeElasticsearchIndexerController, self).__init__(config)
        self.spark = spark
        self.spark_created_by_command = spark_created_by_command

    def ensure_view_exists(self, sql_view_name: str, force_recreate=True) -> None:
        view_exists = len(list(self.spark.sql(f"show views like '{sql_view_name}'").collect())) == 1
        if view_exists and not force_recreate:
            return

        # Ensure reference tables the TEMP VIEW may depend on exist
        create_ref_temp_views(self.spark)

        view_file_path = settings.APP_DIR / "database_scripts" / "etl" / f"{sql_view_name}.sql"

        view_sql = view_file_path.read_text()

        # Find/replace SQL strings in Postgres-based SQL to make it Spark SQL compliant
        # WARNING: If the SQL changes, it must be tested to still be Spark SQL compliant, and changes here may be needed
        temp_view_select_sql = view_sql.replace(f"DROP VIEW IF EXISTS {sql_view_name};", "")

        identifier_replacements = {}
        if self.config["load_type"] == "transaction":
            identifier_replacements["transaction_search"] = "rpt.transaction_search"
        elif self.config["load_type"] == "award":
            identifier_replacements["award_search"] = "rpt.award_search"
        elif self.config["load_type"] == "subaward":
            identifier_replacements["toptier_agency"] = "global_temp.toptier_agency"
        elif self.config["load_type"] == "recipient":
            identifier_replacements = None
        elif self.config["load_type"] == "location":
            identifier_replacements["~"] = "rlike"
            identifier_replacements["jsonb_build_object"] = "map"
            identifier_replacements["to_jsonb"] = "to_json"
            identifier_replacements["state_data"] = "global_temp.state_data"
            identifier_replacements["ref_country_code"] = "global_temp.ref_country_code"
            identifier_replacements["ref_city_county_state_code"] = "global_temp.ref_city_county_state_code"
            identifier_replacements["zips_grouped"] = "global_temp.zips_grouped"

        else:
            raise ValueError(
                f"Unrecognized load_type {self.config['load_type']}, or this function does not yet support it"
            )

        temp_view_select_sql = clean_postgres_sql_for_spark_sql(
            temp_view_select_sql, build_ref_table_name_list(), identifier_replacements
        )

        self.spark.sql(
            f"""
        {temp_view_select_sql}
        """
        )

    def _count_of_records_to_process(self, config) -> Tuple[int, int, int]:
        return count_of_records_to_process_in_delta(config, self.spark)

    def determine_partitions(self) -> int:
        """Simple strategy to divide total record count by ideal partition size"""
        return ceil(self.record_count / self.config["partition_size"])

    def get_id_range_for_partition(self, partition_number: int) -> Tuple[int, int]:
        raise NotImplementedError(
            "Delta Lake data is extracted into a Spark DataFrame that is partitioned. No need to get each partition "
            "by ID ranges."
        )

    def prepare_for_etl(self) -> None:
        spark_executors = self.spark.sparkContext.defaultParallelism
        logger.info(
            format_log(
                f"Overriding specified --processes and setting to configured executors "
                f"on the Spark cluster = {spark_executors}"
            )
        )
        self.config["processes"] = self.spark.sparkContext.defaultParallelism
        super().prepare_for_etl()

    def configure_task(self, partition_number: int, task_name: str, is_null_partition: bool = False) -> TaskSpec:
        # Spark-based approach maps indexing functions to partitions of data, rather than extracting and processing
        # the data, so extract sql is not used
        return self._construct_task_spec(partition_number, task_name, extract_sql_str=None)

    def dispatch_tasks(self) -> None:
        extract_sql = obtain_extract_all_partitions_sql(self.config)
        extract_sql = clean_postgres_sql_for_spark_sql(extract_sql)
        logger.info(format_log(f"Using extract_sql:\n{extract_sql}", action="Extract"))
        df = self.spark.sql(extract_sql)
        df_record_count = df.count()  # safe to doublecheck the count of the *actual* data being processed
        if not df_record_count:
            logger.info(format_log("No records found. Index will not be updated."))
            return
        if self.config["extra_null_partition"]:
            # Data which may have a "NULL Partition" is parent-child grouped data, where child records are grouped by
            # the config["primary_key"], which is the PK field of the parent records.
            # It is imperative that only 1 indexing operation per parent document (per primary_key value) happen,
            # and encompass all of its nested child documents, otherwise subsequent indexing of that parent document
            # will overwrite the prior documents.
            # For this to happen, ALL data for a parent document (primar_key) must exist in the same partition of
            # data being transformed and loaded. This can be achieved by providing the grouping to repartition,
            # so all records with that same value will end up in the same partition
            partition_field = self.config["primary_key"]
            msg = (
                f"Repartitioning {df_record_count} records from {df.rdd.getNumPartitions()} partitions into "
                f"{self.config['partitions']} partitions. Repartitioning by the {partition_field} field so that all "
                f"records that share the same value for this field will be in the same partition to support nesting "
                f"all child documents under the parent record. Partitions should generally be less than "
                f"{self.config['partition_size']} records. Exceptions are if a parent has more than "
                f"{self.config['partition_size']} child documents, and the 'NULL partition', which is all child "
                f"documents that have NULL for the {partition_field} field. Then handing each partition to available "
                f"executors for processing."
            )
            logger.info(format_log(msg))
            logger.info(
                format_log(
                    "Partition-processing task logs will be embedded in executor stderr logs, and not appear here."
                )
            )
            df = df.repartition(self.config["partitions"], partition_field)
        else:
            msg = (
                f"Repartitioning {df_record_count} records from {df.rdd.getNumPartitions()} partitions into "
                f"{self.config['partitions']} partitions to evenly balance no more than "
                f"{self.config['partition_size']} "
                f"records per partition. Then handing each partition to available executors for processing."
            )
            logger.info(format_log(msg))
            logger.info(
                format_log(
                    "Partition-processing task logs will be embedded in executor stderr logs, and not appear here."
                )
            )
            df = df.repartition(self.config["partitions"])

        # Create a clean/detached copy of this dict. Referencing self within the lambda will attempt to pickle the
        # self object, which has a reference to the SparkContext. SparkContext references CANNOT be pickled
        task_dict = {**self.tasks}

        # Map the indexing function to each of the created partitions of the DataFrame
        success_fail_stats = df.rdd.mapPartitionsWithIndex(
            lambda partition_idx, partition_data: transform_and_load_partition(
                partition_data=partition_data,
                task=task_dict[partition_idx],
            ),
            preservesPartitioning=True,
        ).collect()

        successes, failures = 0, 0
        for sf in success_fail_stats:
            successes += sf[0]
            failures += sf[1]
        msg = f"Total documents indexed: {successes}, total document fails: {failures}"
        logger.info(format_log(msg))

    def _run_award_deletes(self):
        client = instantiate_elasticsearch_client()
        delete_awards(
            client=client,
            config=self.config,
            fabs_external_data_load_date_key="transaction_fabs",
            fpds_external_data_load_date_key="transaction_fpds",
            spark=self.spark,
        )

    def _run_transaction_deletes(self):
        client = instantiate_elasticsearch_client()
        delete_transactions(
            client=client,
            config=self.config,
            fabs_external_data_load_date_key="transaction_fabs",
            fpds_external_data_load_date_key="transaction_fpds",
            spark=self.spark,
        )
        # Use the lesser of the fabs/fpds load dates as the es_deletes load date. This
        # ensures all records deleted since either job was run are taken into account
        # Using the loaded-from-DELTA-tables dates here, not the postgres table load dates
        last_db_delete_time = get_earliest_load_date(["transaction_fabs", "transaction_fpds"])
        update_last_load_date("es_deletes", last_db_delete_time)

    def cleanup(self) -> None:
        if self.spark_created_by_command:
            self.spark.stop()


def transform_and_load_partition(task: TaskSpec, partition_data) -> List[Tuple[int, int]]:
    start = perf_counter()
    msg = f"Started processing on partition #{task.partition_number}: {task.name}"
    logger.info(format_log(msg, name=task.name))

    client = instantiate_elasticsearch_client()
    try:
        records = [row.asDict(recursive=True) for row in partition_data]
        if task.transform_func is not None:
            records = task.transform_func(task, records)
        if len(records) > 0:
            success, fail = load_data(task, records, client)
        else:
            logger.info(format_log("No records to index", name=task.name))
            success, fail = 0, 0
    except Exception as exc:
        logger.exception(format_log(f"{task.name} failed!", name=task.name), exc)
        raise exc
    else:
        msg = f"Partition #{task.partition_number} was successfully processed in {perf_counter() - start:.2f}s"
        logger.info(format_log(msg, name=task.name))
    return [(success, fail)]
