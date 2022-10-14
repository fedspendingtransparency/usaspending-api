import copy
import logging

from contextlib import contextmanager
from datetime import datetime, timezone

from django.core.management import BaseCommand
from django.db import connection
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType

from usaspending_api.broker.helpers.get_business_categories import (
    get_business_categories_fabs,
    get_business_categories_fpds,
)
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    get_active_spark_session,
    configure_spark_session,
    get_jvm_logger,
)
from usaspending_api.config import CONFIG
from usaspending_api.transactions.delta_models.transaction_fabs import TRANSACTION_FABS_COLUMN_INFO
from usaspending_api.transactions.delta_models.transaction_fpds import TRANSACTION_FPDS_COLUMN_INFO


class Command(BaseCommand):

    help = """
        This command reads transaction data from source / bronze tables in delta and creates the delta silver tables
        specified via the "etl_level" argument. Each "etl_level" uses an exclusive value for "last_load_date" from the
        "external_data_load_date" table in Postgres to determine the subset of transactions to load. For a full
        pipeline run the "award_ids" and "transaction_ids" levels should be run first in order to populate the
        lookup tables. These lookup tables are used to keep track of PK values across the different silver tables.

        *****NOTE*****: Before running this command for the first time a one-time script should be run to populate the
        lookup tables, set the value of the sequences, and set the value of "last_load_date" for each level.
        Script: "<PLACEHOLDER>"
    """

    etl_level: str
    logger: logging.Logger
    spark: SparkSession
    sql_views: dict

    def add_arguments(self, parser):
        parser.add_argument(
            "--etl-level",
            type=str,
            required=True,
            help="The silver delta table that should be updated from the bronze delta data.",
            choices=[
                "awards",
                "award_id_lookup",
                "transaction_fabs",
                "transaction_fpds",
                "transaction_id_lookup",
                "transaction_normalized",
            ],
        )

    def handle(self, *args, **options):
        with self.prepare_spark():
            # TODO: This should be removed in favor of preparing the environment outside of this script
            self.initial_run()

            self.etl_level = options["etl_level"]

            self.logger.info(f"Deleting old records from '{self.etl_level}'")
            self.spark.sql(self.delete_records_sql())

            # Capture start of the ETL to update the "last_load_date" after completion
            etl_start = datetime.now(timezone.utc)

            last_etl_date = str(get_last_load_date(self.etl_level, lookback_minutes=15))

            # Build the SQL VIEW definitions
            sql_views = {
                "vw_fabs_from_source": self.from_source_sql("fabs", last_etl_date),
                "vw_fpds_from_source": self.from_source_sql("fpds", last_etl_date),
            }

            # Build the SQL MERGE INTO definitions
            sql_merge_operations = {
                "transaction_fabs": self.merge_into_sql("fabs"),
                "transaction_fpds": self.merge_into_sql("fpds"),
            }

            self.logger.info(f"Running SQL for '{self.etl_level}' ETL")
            if self.etl_level == "transaction_id_lookup":
                self.load_transaction_lookup_ids(last_etl_date)
            elif self.etl_level == "transaction_fabs":
                self.spark.sql(sql_views["vw_fabs_from_source"])
                self.spark.sql(sql_merge_operations["transaction_fabs"])
            elif self.etl_level == "transaction_fpds":
                self.spark.sql(sql_views["vw_fpds_from_source"])
                self.spark.sql(sql_merge_operations["transaction_fpds"])
            elif self.etl_level == "transaction_normalized":
                self.spark.sql(sql_views["vw_fabs_from_source"])
                self.spark.sql(sql_views["vw_fpds_from_source"])
                self.spark.sql(sql_merge_operations["transaction_normalized"])

            update_last_load_date(self.etl_level, etl_start)

    @contextmanager
    def prepare_spark(self):
        extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }

        # Create the Spark Session
        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)  # type: SparkSession

        # Setup Logger
        self.logger = get_jvm_logger(self.spark, __name__)

        # Create UDF for Business Categories
        self.spark.udf.register(
            name="get_business_categories_fabs", f=get_business_categories_fabs, returnType=ArrayType(StringType())
        )
        self.spark.udf.register(
            name="get_business_categories_fpds", f=get_business_categories_fpds, returnType=ArrayType(StringType())
        )

        create_ref_temp_views(self.spark)

        yield  # Going to wait for the Django command to complete then stop the spark session if needed

        if spark_created_by_command:
            self.spark.stop()

    def from_source_sql(self, transaction_type, last_etl_date):
        if transaction_type == "fabs":
            bronze_table_name = "raw.published_fabs"
            unique_id = "published_fabs_id"
            col_info = copy.copy(TRANSACTION_FABS_COLUMN_INFO)
        elif transaction_type == "fpds":
            bronze_table_name = "raw.detached_award_procurement"
            unique_id = "detached_award_procurement_id"
            col_info = copy.copy(TRANSACTION_FPDS_COLUMN_INFO)
        else:
            raise ValueError(f"Invalid value for 'transaction_type'; must select either: 'fabs' or 'fpds'")

        select_cols = []
        additional_joins = ""
        omitted_cols = ["transaction_id"]
        for col in list(filter(lambda x: x.silver_name not in omitted_cols, col_info)):
            if col.is_cast:
                select_cols.append(
                    f"CAST({bronze_table_name}.{col.bronze_name} AS {col.delta_type}) AS {col.silver_name}"
                )
            else:
                select_cols.append(f"{bronze_table_name}.{col.bronze_name} AS {col.silver_name}")

        # Additional conditional columns / JOINs needed to support int.transaction_normalized
        if self.etl_level == "transaction_normalized":
            if transaction_type == "fabs":
                select_cols.extend(
                    [
                        # business_categories
                        # funding_amount
                        f"""CAST(
                            COALESCE({bronze_table_name}.federal_action_obligation, 0)
                            + COALESCE({bronze_table_name}.non_federal_funding_amount, 0)
                        AS NUMERIC(23, 2)) AS funding_amount
                        """,
                        # is_fpds
                        "FALSE AS is_fpds",
                    ]
                )
            else:
                select_cols.extend(
                    [
                        # business_categories
                        # is_fpds
                        "TRUE AS is_fpds"
                    ]
                )
            select_cols.extend(["awarding_agency.id AS awarding_agency_id,", "funding_agency.id AS funding_agency_id"])
            additional_joins = f"""
                LEFT OUTER JOIN global_temp.subtier_agency AS funding_subtier_agency ON (
                    funding_subtier_agency.subtier_code = {bronze_table_name}.awarding_sub_tier_agency_c
                )
                LEFT OUTER JOIN global_temp.agency AS funding_agency ON (
                    funding_agency.subtier_agency_id = funding_subtier_agency.subtier_agency_id
                )
                LEFT OUTER JOIN global_temp.subtier_agency AS awarding_subtier_agency ON (
                    awarding_subtier_agency.subtier_code = {bronze_table_name}.funding_sub_tier_agency_co
                )
                LEFT OUTER JOIN global_temp.agency AS awarding_agency ON (
                    awarding_agency.subtier_agency_id = awarding_subtier_agency.subtier_agency_id
                )
            """

        sql = f"""
            CREATE OR REPLACE TEMPORARY VIEW vw_{transaction_type}_from_source AS (
                SELECT
                    transaction_id_lookup.id AS transaction_id,
                    {", ".join(select_cols)}
                FROM {bronze_table_name}
                LEFT OUTER JOIN transaction_id_lookup ON (
                    transaction_id_lookup.{unique_id} = {bronze_table_name}.{unique_id}
                )
                {additional_joins}
                WHERE
                    {bronze_table_name}.updated_at >= '{last_etl_date}'
                    {"AND is_active IS TRUE" if transaction_type == "fabs" else ""}
            )
        """
        return sql

    def load_transaction_lookup_ids(self, last_etl_date):
        self.logger.info("Getting the next transaction_id from transaction_id_seq")
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('transaction_id_seq')")
            # Subtract 1 since the value of "RANK()" will start at 1 and be added to the "next_id" value
            next_id = cursor.fetchone()[0] - 1

        self.logger.info("Creating new 'transaction_id_lookup' records for new transactions")
        self.spark.sql(
            f"""
            INSERT INTO int.transaction_id_lookup
            SELECT
                {next_id} + ROW_NUMBER() OVER (
                    PARTITION BY all_new_transactions.assign_new_id
                    ORDER BY all_new_transactions.transaction_unique_id
                ),
                all_new_transactions.detached_award_procurement_id,
                all_new_transactions.published_fabs_id,
                all_new_transactions.transaction_unique_id
            FROM (
                (
                    SELECT
                        detached_award_procurement_id,
                        NULL AS published_fabs_id,
                        detached_award_proc_unique AS transaction_unique_id,
                        -- A value of "true" is assigned across all rows for use in creating a partition
                        -- in ROW_NUMBER() assigning a unique, incrementing value to each row
                        TRUE AS assign_new_id
                    FROM
                        raw.detached_award_procurement
                    WHERE
                        updated_at >= '{last_etl_date}'
                        AND NOT EXISTS(
                            SELECT *
                            FROM transaction_id_lookup
                            WHERE transaction_id_lookup.detached_award_procurement_id = detached_award_procurement.detached_award_procurement_id
                        )
                )
                UNION
                (
                    SELECT
                        NULL AS detached_award_procurement_id,
                        published_fabs_id,
                        afa_generated_unique AS transaction_unique_id,
                        -- A value of "true" is assigned across all rows for use in creating a partition
                        -- in ROW_NUMBER() assigning a unique, incrementing value to each row
                        TRUE AS assign_new_id
                    FROM
                        raw.published_fabs
                    WHERE
                        is_active IS TRUE
                        AND updated_at >= '{last_etl_date}'
                        AND NOT EXISTS(
                            SELECT *
                            FROM transaction_id_lookup
                            WHERE transaction_id_lookup.published_fabs_id = published_fabs.published_fabs_id
                        )
                )
            ) AS all_new_transactions
        """
        )

        self.logger.info("Updating transaction_id_seq to the new max_id value")
        max_id = self.spark.sql("SELECT MAX(id) AS max_id FROM int.transaction_id_lookup").collect()[0]["max_id"]
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT setval('transaction_id_seq', {max_id})")

    @staticmethod
    def merge_into_sql(transaction_type):
        if transaction_type == "fabs":
            col_info = copy.copy(TRANSACTION_FABS_COLUMN_INFO)
        elif transaction_type == "fpds":
            col_info = copy.copy(TRANSACTION_FPDS_COLUMN_INFO)
        else:
            raise ValueError(f"Invalid value for 'transaction_type'; must select either: 'fabs' or 'fpds'")

        set_cols = [f"silver_table.{col.silver_name} = source_view.{col.silver_name}" for col in col_info]
        silver_table_cols = ", ".join([col.silver_name for col in col_info])

        sql = f"""
            MERGE INTO int.transaction_{transaction_type} AS silver_table
            USING vw_{transaction_type}_from_source AS source_view
            ON silver_table.transaction_id = source_view.transaction_id
            WHEN MATCHED
                THEN UPDATE SET
                    {", ".join(set_cols)}
            WHEN NOT MATCHED
                THEN INSERT
                    ({silver_table_cols})
                    VALUES ({silver_table_cols})
        """
        return sql

    def delete_records_sql(self):
        if self.etl_level == "transaction_id_lookup":
            id_col = "id"
            subquery = f"""
                SELECT id AS id_to_remove
                FROM int.transaction_id_lookup
                WHERE
                    (
                        transaction_id_lookup.detached_award_procurement_id IS NOT NULL
                        AND NOT EXISTS (
                            SELECT *
                            FROM raw.detached_award_procurement
                            WHERE transaction_id_lookup.detached_award_procurement_id = detached_award_procurement.detached_award_procurement_id
                        )
                    )
                    OR
                    (
                        transaction_id_lookup.published_fabs_id IS NOT NULL
                        AND NOT EXISTS (
                            SELECT *
                            FROM raw.published_fabs
                            WHERE transaction_id_lookup.published_fabs_id = published_fabs.published_fabs_id
                        )
                    )
            """
        else:
            id_col = "id" if self.etl_level == "transaction_normalized" else "transaction_id"
            subquery = f"""
                SELECT {id_col} AS id_to_remove
                FROM int.{self.etl_level}
                WHERE
                    NOT EXISTS (
                        SELECT *
                        FROM int.transaction_id_lookup
                        WHERE transaction_id_lookup.id = {self.etl_level}.{id_col}
                    )
            """

        sql = f"""
            MERGE INTO int.{self.etl_level}
            USING (
                {subquery}
            ) AS deleted_records
            ON {self.etl_level}.{id_col} = deleted_records.id_to_remove
            WHEN MATCHED
            THEN DELETE
        """
        return sql

    def initial_run(self):
        """
        TODO:
            - Still need to create migration that creates the sequence in Postgres
            - External Data values need to be added for all etl_levels (including transaction_ids)
            - Script to backfill all IDs in the lookup table
        """
        self.spark.sql("USE int")

        # This CREATE TABLE statement should be moved out of this; left in place for now during local development
        spark_s3_bucket = CONFIG.SPARK_S3_BUCKET
        delta_lake_s3_bucket = CONFIG.DELTA_LAKE_S3_PATH
        destination_database = "int"
        destination_table = "transaction_id_lookup"
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {destination_table} (
                id LONG NOT NULL,
                detached_award_procurement_id INTEGER,
                published_fabs_id INTEGER,
                transaction_unique_id STRING NOT NULL
            )
            USING DELTA
            LOCATION 's3a://{spark_s3_bucket}/{delta_lake_s3_bucket}/{destination_database}/{destination_table}'
        """
        )
