import copy
import logging

from contextlib import contextmanager
from datetime import datetime, timezone

from django.core.management import BaseCommand, call_command
from django.db import connection
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType

from usaspending_api.broker.helpers.build_business_categories_boolean_dict import (
    fpds_boolean_columns
)
from usaspending_api.broker.helpers.get_business_categories import (
    get_business_categories_fabs,
    get_business_categories_fpds,
)
from usaspending_api.broker.helpers.last_load_date import get_last_load_date, update_last_load_date
from usaspending_api.common.helpers.spark_helpers import (
    get_active_spark_session,
    configure_spark_session,
    get_jvm_logger,
)
from usaspending_api.config import CONFIG
from usaspending_api.transactions.delta_models.transaction_fabs import (
    TRANSACTION_FABS_COLUMN_INFO,
    FABS_TO_NORMALIZED_COLUMN_INFO
)
from usaspending_api.transactions.delta_models.transaction_fpds import (
    TRANSACTION_FPDS_COLUMN_INFO,
    DAP_TO_NORMALIZED_COLUMN_INFO
)

class Command(BaseCommand):

    help = """
        This command reads transaction data from source / bronze tables in delta and creates the delta silver tables
        specified via the "etl_level" argument. Each "etl_level" uses an exclusive value for "last_load_date" from the
        "external_data_load_date" table in Postgres to determine the subset of transactions to load. For a full
        pipeline run the "award_id_lookup" and "transaction_id_lookup" levels should be run first in order to populate the
        lookup tables. These lookup tables are used to keep track of PK values across the different silver tables.

        *****NOTE*****: Before running this command for the first time on a usual basis, it should be run with the
            "etl_level" set to "initial_run" to set up the needed lookup tables and populate the needed sequences and
            "last_load_date" values for the lookup tables.
    """

    etl_level: str
    last_etl_date: str
    spark_s3_bucket: str
    logger: logging.Logger
    spark: SparkSession

    def add_arguments(self, parser):
        parser.add_argument(
            "--etl-level",
            type=str,
            required=True,
            help="The silver delta table that should be updated from the bronze delta data.",
            choices=[
                "award_id_lookup",
                "initial_run",
                "transaction_fabs",
                "transaction_fpds",
                "transaction_id_lookup",
                "transaction_normalized"
            ],
        )
        parser.add_argument(
            "--spark-s3-bucket",
            type=str,
            required=False,
            default=CONFIG.SPARK_S3_BUCKET,
            help="The destination bucket in S3 for creating the tables.",
        )
        parser.add_argument(
            "--initial-copy",
            type=bool,
            required=False,
            default=True,
            help="Whether to copy tables from the 'raw' database to the 'int' database during initial_run.",
        )

    def handle(self, *args, **options):
        with self.prepare_spark():
            self.etl_level = options["etl_level"]
            self.spark_s3_bucket = options["spark_s3_bucket"]
            self.initial_copy = options["initial_copy"]

            if self.etl_level == "initial_run":
                self.logger.info("Running initial setup for transaction_id_lookup and award_id_lookup tables")
                self.initial_run()
                return

            # Capture start of the ETL to update the "last_load_date" after completion
            etl_start = datetime.now(timezone.utc)

            self.logger.info(f"Running delete SQL for '{self.etl_level}' ETL")
            self.spark.sql(self.delete_records_sql())

            last_etl_date = get_last_load_date(self.etl_level, lookback_minutes=15)
            if last_etl_date is None:
                # Table has not been loaded yet.  To avoid checking for None in all the locations where
                # last_etl_date is used, set it to a long time ago.
                last_etl_date = datetime.utcfromtimestamp(0)
            self.last_etl_date = str(last_etl_date)

            self.logger.info(f"Running UPSERT SQL for '{self.etl_level}' ETL")
            if self.etl_level == "transaction_id_lookup":
                self.update_transaction_lookup_ids()
            elif self.etl_level == "award_id_lookup":
                self.update_award_lookup_ids()
            elif self.etl_level == "transaction_fabs":
                self.spark.sql(self.merge_into_sql("fabs"))
            elif self.etl_level == "transaction_fpds":
                self.spark.sql(self.merge_into_sql("fpds"))
            elif self.etl_level == "transaction_normalized":
                self.spark.sql(self.transaction_normalized_merge_into_sql("fabs"))
                self.spark.sql(self.transaction_normalized_merge_into_sql("fpds"))

            update_last_load_date(self.etl_level, etl_start)

    @contextmanager
    def prepare_spark(self):
        extra_conf = {
            # Config for additional packages needed
            # "spark.jars.packages": "org.postgresql:postgresql:42.2.23,io.delta:delta-core_2.12:1.2.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-hive_2.12:3.2.1",
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
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

        # Create UDFs for Business Categories
        self.spark.udf.register(
            name="get_business_categories_fabs", f=get_business_categories_fabs, returnType=ArrayType(StringType())
        )
        self.spark.udf.register(
            name="get_business_categories_fpds", f=get_business_categories_fpds, returnType=ArrayType(StringType())
        )

        yield  # Going to wait for the Django command to complete then stop the spark session if needed

        if spark_created_by_command:
            self.spark.stop()

    def delete_records_sql(self):
        if self.etl_level == "transaction_id_lookup":
            id_col = "id"
            subquery = """
                SELECT id AS id_to_remove
                FROM int.transaction_id_lookup AS tidlu LEFT JOIN raw.detached_award_procurement AS dap ON (
                    tidlu.detached_award_procurement_id = dap.detached_award_procurement_id
                )
                WHERE tidlu.detached_award_procurement_id IS NOT NULL AND dap.detached_award_procurement_id IS NULL
                UNION ALL
                SELECT id AS id_to_remove
                FROM int.transaction_id_lookup AS tidlu LEFT JOIN raw.published_fabs AS pfabs ON (
                    tidlu.published_fabs_id = pfabs.published_fabs_id
                )
                WHERE tidlu.published_fabs_id IS NOT NULL AND pfabs.published_fabs_id IS NULL
            """
        elif self.etl_level == "award_id_lookup":
            id_col = "transaction_unique_id"
            subquery = """
                SELECT aidlu.transaction_unique_id AS id_to_remove
                FROM int.award_id_lookup AS aidlu LEFT JOIN raw.detached_award_procurement AS dap ON (
                    aidlu.detached_award_procurement_id = dap.detached_award_procurement_id
                )
                WHERE aidlu.detached_award_procurement_id IS NOT NULL AND dap.detached_award_procurement_id IS NULL
                UNION ALL
                SELECT aidlu.transaction_unique_id AS id_to_remove
                FROM int.award_id_lookup AS aidlu LEFT JOIN raw.published_fabs AS pfabs ON (
                    aidlu.published_fabs_id = pfabs.published_fabs_id
                )
                WHERE aidlu.published_fabs_id IS NOT NULL AND pfabs.published_fabs_id IS NULL
            """
        elif self.etl_level in ("transaction_fabs", "transaction_fpds", "transaction_normalized"):
            id_col = "id" if self.etl_level == "transaction_normalized" else "transaction_id"
            subquery = f"""
                SELECT {id_col} AS id_to_remove
                FROM int.{self.etl_level} LEFT JOIN int.transaction_id_lookup ON (
                    {self.etl_level}.{id_col} = transaction_id_lookup.id
                )
                WHERE {self.etl_level}.{id_col} IS NOT NULL AND transaction_id_lookup.id IS NULL
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

    def source_subquery_sql(self, transaction_type):
        if transaction_type == "fabs":
            bronze_table_name = "raw.published_fabs"
            unique_id = "published_fabs_id"
            col_info = copy.copy(TRANSACTION_FABS_COLUMN_INFO)
        elif transaction_type == "fpds":
            bronze_table_name = "raw.detached_award_procurement"
            unique_id = "detached_award_procurement_id"
            col_info = copy.copy(TRANSACTION_FPDS_COLUMN_INFO)
        else:
            raise ValueError("Invalid value for 'transaction_type'; must select either: 'fabs' or 'fpds'")

        if self.etl_level in ("transaction_fabs", "transaction_fpds"):
            select_cols = []
            additional_joins = ""
            id_col_name = "transaction_id"
            omitted_cols = [id_col_name]
            for col in filter(lambda x: x.silver_name not in omitted_cols, col_info):
                if col.handling == "cast":
                    select_cols.append(
                        f"CAST({bronze_table_name}.{col.bronze_name} AS {col.delta_type}) AS {col.silver_name}"
                    )
                elif col.handling == "literal":
                    # Use col.bronze_name directly as the value
                    select_cols.append(f"{col.bronze_name} AS {col.silver_name}")
                else:
                    select_cols.append(f"{bronze_table_name}.{col.bronze_name} AS {col.silver_name}")

        elif self.etl_level == "transaction_normalized":
            id_col_name = "id"
            select_cols = [
                "award_id_lookup.id AS award_id",
                "awarding_agency.id AS awarding_agency_id",
                f"""CASE WHEN month(to_date({bronze_table_name}.action_date)) > 9 
                             THEN year(to_date({bronze_table_name}.action_date)) + 1
                         ELSE year(to_date({bronze_table_name}.action_date))
                    END AS fiscal_year""",
                "funding_agency.id AS funding_agency_id",
            ]

            if transaction_type == "fabs":
                select_cols.extend(
                    [
                        # business_categories
                        f"get_business_categories_fabs({bronze_table_name}.business_types) AS business_categories",
                        # funding_amount
                        f"""
                        CAST(COALESCE({bronze_table_name}.federal_action_obligation, 0)
                                + COALESCE({bronze_table_name}.non_federal_funding_amount, 0)
                             AS NUMERIC(23, 2)) AS funding_amount
                        """,
                    ]
                )
                map_col_info = copy.copy(FABS_TO_NORMALIZED_COLUMN_INFO)
            else:
                fpds_business_category_columns = copy.copy(fpds_boolean_columns)
                fpds_business_category_columns.extend(["contracting_officers_deter", "domestic_or_foreign_entity"])
                named_struct_text = ", ".join([f"'{col}', {bronze_table_name}.{col}"
                                               for col in fpds_business_category_columns])

                select_cols.extend(
                    [
                        # business_categories
                        f"get_business_categories_fpds(named_struct({named_struct_text})) AS business_categories",
                        # type
                        f"""
                        CASE WHEN {bronze_table_name}.pulled_from <> 'IDV' THEN {bronze_table_name}.contract_award_type
                             WHEN {bronze_table_name}.idv_type = 'B' AND {bronze_table_name}.type_of_idc IS NOT NULL
                               THEN 'IDV_B_' || {bronze_table_name}.type_of_idc
                             WHEN {bronze_table_name}.idv_type = 'B'
                                 AND {bronze_table_name}.type_of_idc_description = 'INDEFINITE DELIVERY / REQUIREMENTS'
                               THEN 'IDV_B_A'
                             WHEN {bronze_table_name}.idv_type = 'B'
                                 AND {bronze_table_name}.type_of_idc_description = 
                                     'INDEFINITE DELIVERY / INDEFINITE QUANTITY'
                               THEN 'IDV_B_B'
                             WHEN {bronze_table_name}.idv_type = 'B'
                                 AND {bronze_table_name}.type_of_idc_description = 
                                     'INDEFINITE DELIVERY / DEFINITE QUANTITY'
                               THEN 'IDV_B_C'
                             ELSE 'IDV_' || {bronze_table_name}.idv_type
                        END AS type
                        """,
                        # type_description
                        f"""
                        CASE WHEN {bronze_table_name}.pulled_from <> 'IDV'
                               THEN {bronze_table_name}.contract_award_type_desc
                             WHEN {bronze_table_name}.idv_type = 'B'
                                 AND {bronze_table_name}.type_of_idc_description IS NOT NULL
                                 AND ucase({bronze_table_name}.type_of_idc_description) <> 'NAN'
                               THEN {bronze_table_name}.type_of_idc_description
                             WHEN {bronze_table_name}.idv_type = 'B'
                               THEN 'INDEFINITE DELIVERY CONTRACT'
                             ELSE {bronze_table_name}.idv_type_description
                        END AS type_description
                        """
                    ]
                )
                map_col_info = copy.copy(DAP_TO_NORMALIZED_COLUMN_INFO)

            for col in map_col_info:
                if col.handling == "cast":
                    select_cols.append(
                        f"CAST({bronze_table_name}.{col.bronze_name} AS {col.delta_type}) AS {col.silver_name}"
                    )
                elif col.handling == "literal":
                    # Use col.bronze_name directly as the value
                    select_cols.append(f"{col.bronze_name} AS {col.silver_name}")
                else:
                    select_cols.append(f"{bronze_table_name}.{col.bronze_name} AS {col.silver_name}")

            additional_joins = f"""
                INNER JOIN int.award_id_lookup AS award_id_lookup ON (
                    {bronze_table_name}.{unique_id} = award_id_lookup.{unique_id}
                )
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
                SELECT
                    transaction_id_lookup.id AS {id_col_name},
                    {", ".join(select_cols)}
                FROM {bronze_table_name}
                INNER JOIN int.transaction_id_lookup ON (
                    {bronze_table_name}.{unique_id} = transaction_id_lookup.{unique_id}
                )
                {additional_joins}
                WHERE {bronze_table_name}.updated_at >= '{self.last_etl_date}'
        """

        return sql

    def merge_into_sql(self, transaction_type):
        if transaction_type == "fabs":
            col_info = copy.copy(TRANSACTION_FABS_COLUMN_INFO)
        elif transaction_type == "fpds":
            col_info = copy.copy(TRANSACTION_FPDS_COLUMN_INFO)
        else:
            raise ValueError(f"Invalid value for 'transaction_type': {transaction_type}. "
                             "Must select either: 'fabs' or 'fpds'")

        set_cols = [f"silver_table.{col.silver_name} = source_subquery.{col.silver_name}" for col in col_info]
        silver_table_cols = ", ".join([col.silver_name for col in col_info])

        sql = f"""
            MERGE INTO int.{self.etl_level} AS silver_table
            USING (
                {self.source_subquery_sql(transaction_type)}
            ) AS source_subquery
            ON silver_table.transaction_id = source_subquery.transaction_id
            WHEN MATCHED
                THEN UPDATE SET
                    {", ".join(set_cols)}
            WHEN NOT MATCHED
                THEN INSERT
                    ({silver_table_cols})
                    VALUES ({silver_table_cols})
        """

        return sql

    def transaction_normalized_merge_into_sql(self, transaction_type):
        if transaction_type != "fabs" and transaction_type != "fpds":
            raise ValueError(f"Invalid value for 'transaction_type': {transaction_type}. "
                             "Must select either: 'fabs' or 'fpds'")

        load_datetime = datetime.now(timezone.utc)

        # On set, create_date will not be changed and update_date will be set below.  All other column
        # values will come from the subquery.
        set_cols = [
            f"int.transaction_normalized.{col_name} = source_subquery.{col_name}"
            for col_name in TRANSACTION_NORMALIZED_COLUMNS if col_name not in ("create_date", "update_date")
        ]
        set_cols.append(f"int.transaction_normalized.update_date = '{load_datetime.isoformat(" ")}'")

        # Move create_date and update_date to the end of the list of column names for ease of handling
        # during record insert
        insert_col_name_list = [col_name for col_name in TRANSACTION_NORMALIZED_COLUMNS
                                         if col_name not in ("create_date", "update_date")]
        insert_col_name_list.extend(["create_date", "update_date"])
        insert_col_names = ", ".join([col_name for col_name in insert_col_name_list])

        # On insert, all values except for create_date and update_date will come from the subquery
        insert_value_list = insert_col_name_list[:-2]
        insert_value_list.extend([f"'{load_datetime.isoformat(" ")}'"] * 2)
        insert_values = ", ".join([value for value in insert_value_list])

        sql = f"""
            MERGE INTO int.transaction_normalized
            USING (
                {self.source_subquery_sql(transaction_type)}
            ) AS source_subquery
            ON transaction_normalized.id = source_subquery.id
            WHEN MATCHED
                THEN UPDATE SET
                {", ".join(set_cols)}
            WHEN NOT MATCHED
                THEN INSERT
                    ({insert_col_names})
                    VALUES ({insert_values})
        """

        return sql

    def update_transaction_lookup_ids(self):
        self.logger.info("Getting the next transaction_id from transaction_id_seq")
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('transaction_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            previous_max_id = cursor.fetchone()[0]

        self.logger.info("Creating new 'transaction_id_lookup' records for new transactions")
        self.spark.sql(
            f"""
            WITH dap_filtered AS (
                SELECT detached_award_procurement_id, detached_award_proc_unique
                FROM raw.detached_award_procurement
                WHERE updated_at >= '{self.last_etl_date}'
            ),
            pfabs_filtered AS (
                SELECT published_fabs_id, afa_generated_unique
                FROM raw.published_fabs
                WHERE updated_at >= '{self.last_etl_date}'
            )
            INSERT INTO int.transaction_id_lookup
            SELECT
                {previous_max_id} + ROW_NUMBER() OVER (
                    ORDER BY all_new_transactions.transaction_unique_id
                ) AS id,
                all_new_transactions.detached_award_procurement_id,
                all_new_transactions.published_fabs_id,
                all_new_transactions.transaction_unique_id
            FROM (
                (
                    SELECT
                        dap.detached_award_procurement_id,
                        NULL AS published_fabs_id,
                        dap.detached_award_proc_unique AS transaction_unique_id
                    FROM
                         dap_filtered AS dap LEFT JOIN int.transaction_id_lookup AS tidlu ON (
                            dap.detached_award_procurement_id = tidlu.detached_award_procurement_id
                         )
                    WHERE tidlu.detached_award_procurement_id IS NULL
                )
                UNION ALL
                (
                    SELECT
                        NULL AS detached_award_procurement_id,
                        pfabs.published_fabs_id,
                        pfabs.afa_generated_unique AS transaction_unique_id
                    FROM
                        pfabs_filtered AS pfabs LEFT JOIN int.transaction_id_lookup AS tidlu ON (
                            pfabs.published_fabs_id = tidlu.published_fabs_id
                         )
                    WHERE tidlu.published_fabs_id IS NULL
                )
            ) AS all_new_transactions
        """
        )

        self.logger.info("Updating transaction_id_seq to the new maximum id value seen so far")
        poss_max_id = self.spark.sql("SELECT MAX(id) AS max_id FROM int.transaction_id_lookup").collect()[0]["max_id"]
        with connection.cursor() as cursor:
            # Set is_called flag to false so that the next call to nextval() will return the specified value, and
            #     avoid the possibility of gaps in the transaction_id sequence
            #     https://www.postgresql.org/docs/13/functions-sequence.html
            # If load_transactions_to_delta is called with --etl-level of transaction_id_lookup, and records are
            #     deleted which happen to correspond to transactions at the end of the transaction_id_lookup table,
            #     but no records are inserted, then poss_max_id will be less than previous_max_id above. Just assigning
            #     the current value of transaction_id_seq to poss_max_id would cause problems in a subsequent call
            #     with inserts, as it would assign the new transactions the same ids as the previously deleted ones.
            #     To avoid this possibility, set the current value of transaction_id_seq to the maximum of poss_max_id
            #     and previous_max_id.
            cursor.execute(f"SELECT setval('transaction_id_seq', {max(poss_max_id, previous_max_id)}, false)")

    def update_award_lookup_ids(self):
        self.logger.info("Getting the next award_id from award_id_seq")
        with connection.cursor() as cursor:
            cursor.execute("SELECT nextval('award_id_seq')")
            # Since all calls to setval() set the is_called flag to false, nextval() returns the actual maximum id
            previous_max_id = cursor.fetchone()[0]

        self.logger.info("Creating new 'award_id_lookup' records for new awards")
        self.spark.sql(
            f"""
            WITH dap_filtered AS (
                SELECT detached_award_procurement_id, detached_award_proc_unique, unique_award_key
                FROM raw.detached_award_procurement
                WHERE updated_at >= '{self.last_etl_date}'
            ),
            pfabs_filtered AS (
                SELECT published_fabs_id, afa_generated_unique, unique_award_key
                FROM raw.published_fabs
                WHERE updated_at >= '{self.last_etl_date}'
            )
            INSERT INTO int.award_id_lookup
            SELECT
                {previous_max_id} + DENSE_RANK(all_new_awards.unique_award_key) OVER (
                    ORDER BY all_new_awards.unique_award_key
                ) AS id,
                all_new_awards.detached_award_procurement_id,
                all_new_awards.published_fabs_id,
                all_new_awards.transaction_unique_id,
                all_new_awards.unique_award_key AS generated_unique_award_id
            FROM (
                (
                    SELECT
                        dap.detached_award_procurement_id,
                        NULL AS published_fabs_id,
                        dap.detached_award_proc_unique AS transaction_unique_id,
                        dap.unique_award_key
                    FROM
                         dap_filtered AS dap LEFT JOIN int.award_id_lookup AS aidlu ON (
                            dap.detached_award_proc_unique = aidlu.transaction_unique_id
                         )
                    WHERE aidlu.transaction_unique_id IS NULL
                )
                UNION ALL
                (
                    SELECT
                        NULL AS detached_award_procurement_id,
                        pfabs.published_fabs_id,
                        pfabs.afa_generated_unique AS transaction_unique_id,
                        pfabs.unique_award_key
                    FROM
                        pfabs_filtered AS pfabs LEFT JOIN int.award_id_lookup AS aidlu ON (
                            pfabs.afa_generated_unique = aidlu.transaction_unique_id
                         )
                    WHERE aidlu.transaction_unique_id IS NULL
                )
            ) AS all_new_awards
        """
        )

        self.logger.info("Updating award_id_seq to the new maximum id value seen so far")
        poss_max_id = self.spark.sql("SELECT MAX(id) AS max_id FROM int.award_id_lookup").collect()[0]["max_id"]
        with connection.cursor() as cursor:
            # Set is_called flag to false so that the next call to nextval() will return the specified value, and
            #     avoid the possibility of gaps in the transaction_id sequence
            #     https://www.postgresql.org/docs/13/functions-sequence.html
            # If load_transactions_to_delta is called with --etl-level of award_id_lookup, and records are deleted
            #     which happen to correspond to transactions at the end of the award_id_lookup table, but no records
            #     are inserted, then poss_max_id will be less than previous_max_id above. Just assigning the current
            #     value of award_id_seq to poss_max_id would cause problems in a subsequent call with inserts, as it
            #     would assign the new awards the same ids as the previously deleted ones.  To avoid this possibility,
            #     set the current value of award_id_seq to the maximum of poss_max_id and previous_max_id.
            cursor.execute(f"SELECT setval('award_id_seq', {max(poss_max_id, previous_max_id)}, false)")

    def initial_run(self):
        """
        Procedure to create & set up transaction_id_lookup and award_id_lookup tables and create other tables in
        int database that will be populated by subsequent calls.
        """
        delta_lake_s3_path = CONFIG.DELTA_LAKE_S3_PATH
        destination_database = "int"

        # transaction_id_lookup

        # Capture start time of the transaction_id_lookup creation to update the "last_load_date" after completion
        transaction_id_lookup_start_time = datetime.now(timezone.utc)

        destination_table = "transaction_id_lookup"

        self.logger.info(f"Creating database {destination_database}, if not already existing.")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {destination_database};")

        self.logger.info("Creating transaction_id_lookup table")
        self.spark.sql(
            f"""
                CREATE OR REPLACE TABLE {destination_database}.{destination_table} (
                    id LONG NOT NULL,
                    detached_award_procurement_id INTEGER,
                    published_fabs_id INTEGER,
                    transaction_unique_id STRING NOT NULL
                )
                USING DELTA
                LOCATION 's3a://{self.spark_s3_bucket}/{delta_lake_s3_path}/{destination_database}/{destination_table}'
            """
        )

        # Insert existing transactions into the lookup table
        self.logger.info("Populating transaction_id_lookup table")
        self.spark.sql(
            f"""
            INSERT OVERWRITE {destination_database}.{destination_table}
                SELECT tn.id, dap.detached_award_procurement_id, pfabs.published_fabs_id, tn.transaction_unique_id
                FROM raw.transaction_normalized AS tn
                LEFT JOIN raw.detached_award_procurement AS dap ON (
                    tn.transaction_unique_id = dap.detached_award_proc_unique
                )
                LEFT JOIN raw.published_fabs AS pfabs ON (
                    tn.transaction_unique_id = pfabs.afa_generated_unique
                )
            """
        )

        self.logger.info("Updating transaction_id_seq to the new max_id value")
        max_id = self.spark.sql(f"SELECT MAX(id) AS max_id FROM {destination_database}.{destination_table}").collect()[
            0
        ]["max_id"]
        with connection.cursor() as cursor:
            # Set is_called flag to false so that the next call to nextval() will return the specified value
            # https://www.postgresql.org/docs/13/functions-sequence.html
            cursor.execute(f"SELECT setval('transaction_id_seq', {max_id}, false)")

        update_last_load_date("transaction_id_lookup", transaction_id_lookup_start_time)

        # award_id_lookup

        # Capture start time of the award_id_lookup creation to update the "last_load_date" after completion
        award_id_lookup_start_time = datetime.now(timezone.utc)

        destination_table = "award_id_lookup"

        # Before creating table or running INSERT, make sure unique_award_key has no NULLs
        # (nothing needed to check before transaction_id_lookup table creation)
        self.logger.info("Checking for NULLs in unique_award_key")
        num_nulls = self.spark.sql(
            "SELECT COUNT(*) AS count FROM raw.transaction_normalized WHERE unique_award_key IS NULL"
        ).collect()[0]["count"]

        if num_nulls > 0:
            raise ValueError(
                f"Found {num_nulls} NULL{'s' if num_nulls > 1 else ''} in 'unique_award_key' in table "
                "raw.transaction_normalized!"
            )

        self.logger.info("Creating award_id_lookup table")
        self.spark.sql(
            f"""
                CREATE OR REPLACE TABLE {destination_database}.{destination_table} (
                    id LONG NOT NULL,
                    -- detached_award_procurement_id and published_fabs_id are needed in this table to allow
                    -- the award_id_lookup ETL level to choose the correct rows for deleting so that it can
                    -- be run in parallel with the transaction_id_lookup ETL level
                    detached_award_procurement_id INTEGER,
                    published_fabs_id INTEGER,
                    -- transaction_unique_id *shouldn't* be NULL in the query used to populate this table.
                    -- However, at least in qat, there are awards that don't actually match any tranactions,
                    -- and we want all awards to be listed in this table, so, for now, at least, leaving off
                    -- the NOT NULL constraint from transaction_unique_id
                    transaction_unique_id STRING,
                    generated_unique_award_id STRING NOT NULL
                )
                USING DELTA
                LOCATION 's3a://{self.spark_s3_bucket}/{delta_lake_s3_path}/{destination_database}/{destination_table}'
            """
        )

        # Insert existing transactions into the lookup table
        self.logger.info("Populating award_id_lookup table")
        self.spark.sql(
            f"""
            INSERT OVERWRITE {destination_database}.{destination_table}
                SELECT aw.id, dap.detached_award_procurement_id, pfabs.published_fabs_id,
                    tn.transaction_unique_id, aw.generated_unique_award_id
                FROM raw.awards AS aw
                LEFT JOIN raw.transaction_normalized AS tn ON (
                    aw.generated_unique_award_id = tn.unique_award_key
                )
                LEFT JOIN raw.detached_award_procurement AS dap ON (
                    tn.transaction_unique_id = dap.detached_award_proc_unique
                )
                LEFT JOIN raw.published_fabs AS pfabs ON (
                    tn.transaction_unique_id = pfabs.afa_generated_unique
                )
            """
        )

        self.logger.info("Updating award_id_seq to the new max_id value")
        max_id = self.spark.sql(f"SELECT MAX(id) AS max_id FROM {destination_database}.{destination_table}").collect()[
            0
        ]["max_id"]
        with connection.cursor() as cursor:
            # Set is_called flag to false so that the next call to nextval() will return the specified value
            # https://www.postgresql.org/docs/13/functions-sequence.html
            cursor.execute(f"SELECT setval('award_id_seq', {max_id}, false)")

        update_last_load_date("award_id_lookup", award_id_lookup_start_time)

        # Create tables in 'int' database
        self.logger.info("Creating tables in new database location")
        destination_tables = ("transaction_fabs", "transaction_fpds", "transaction_normalized")
        for destination_table in destination_tables:
            call_command(
                "create_delta_table",
                "--destination-table",
                destination_table,
                "--spark-s3-bucket",
                self.spark_s3_bucket,
                "--alt-db",
                destination_database,
            )

            if self.initial_copy:
                self.spark.sql(
                    f"""
                    INSERT OVERWRITE {destination_database}.{destination_table}
                        SELECT * FROM raw.{destination_table}
                    """
                )
