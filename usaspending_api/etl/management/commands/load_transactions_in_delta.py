import copy

from django.core.management import BaseCommand
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType

from usaspending_api.broker.helpers.get_business_categories import get_business_categories
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    get_active_spark_session,
    configure_spark_session,
    get_jvm_logger,
)
from usaspending_api.transactions.delta_models.transaction_fabs import TRANSACTION_FABS_COLUMN_INFO
from usaspending_api.transactions.delta_models.transaction_fpds import TRANSACTION_FPDS_COLUMN_INFO


class Command(BaseCommand):

    help = """
    """

    spark: SparkSession
    sql_views: dict

    def add_arguments(self, parser):
        """
        Arguments:
            - etl_level (fabs, fpds, normalized, awards) - The ETL process to run
            -
        """
        pass

    def handle(self, *args, **options):
        """
        Queries that are needed for each etl_level up to a specific point. The "etl_level" chosen will determine how
        many of the queries are run.
        Queries:
            - VIEWs on source data
                - vw_fabs_from_source__update
                - vw_fabs_from_source__insert
                - vw_fpds_from_source__update
                - vw_fpds_from_source__insert
            - VIEWs combining all __update and __insert together to manage new IDs
                - vw_all_transaction__update
                - vw_all_transaction__insert
            - VIEWs to capture all UPSERTs
                - vw_transaction_normalized__upsert
                - vw_award__upsert

        ETL LEVEL NOTES:
            - BEFORE ALL QUERIES
                    - DELETE operation will need to occur; should this be per etl_level as well?
            - Might be able to get away without using a DATE to determine what records to update. If this isn't too slow
              this could be preferred since it would be one less item to keep track of.

        NOTES ABOUT QUERIES:
            - vw_transaction_normalized__upsert
                - Used to populate all `int.transaction_<NAME>` tables in order to have consistent use of Transaction IDs
            - vw_award__upsert
                - Used to populate only `int.awards`
            - vw_<TYPE>_from_source__<OPERATION>
                - Will be needed by all ETL levels in order to ensure that the IDs are consistently created

        NOTES ABOUT IDs:
            - Track the usage of IDs uniquely between all four etl_levels
            - IDs should be compared to one another in order to ensure that each ETL is on the same page
                - Create a new table in Delta that tracks IDs for fabs, fpds, normalized, and awards
                - For each of these a current_val and update_at is tracked
                - When running awards ETL it should verify that the
                - | type | update_at | previous_val | current_val |
                - | ---- | --------- | ------------ | ----------- |
                - | fabs | 01/01/22  | 1,000,000    | 1,500,000   |
                - | fpds | 01/01/22  | 1,000,000    | 1,500,000   |
                - | tn   | 01/01/22  | 1,000,000    | 1,500,000   |
                - | aw   | 01/01/22  | 1,000,000    | 1,500,000   |
                - | fabs | 01/02/22  | 1,500,000    | 2,000,000   | ** IF FABS WAS RUN **
            - If FABS is run by itself the state in the table above would be achieved (ignore the first FABS row).

        CASES TO HANDLE:
            - Pipeline fails Friday night on the FPDS level ETL. Next pipeline runs Saturday night, loading in new
              source data and attempting to create new Transactions and Awards from the source data. In this example,
              the Transaction IDs could become mismatched depending on DELETE and UPSERTs made.
              SOLUTION: Using RESTORE to bring back to a previous version of the tables if they do not line up.
                        Could this be done programatically so that if on a fresh pipeline run all are not in sync they
                        do not copy back?
              ADDITIONALLY: Transaction Search and Award Search shouldn't be allowed to run if the ETLs fail allowing
                            manual cleanup to occur via restore or other process.
              THOUGHT: Add in functionality to rest all tables to most recent shared version (not sure best way to tell)
                       what version to use so might be best to let that be handled via manual cleanup.
        """
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
        # logger = get_jvm_logger(self.spark, __name__)

        # Create UDF for Business Categories
        self.spark.udf.register(
            name="build_business_categories",
            f=lambda val, transaction_type: get_business_categories(val, transaction_type),
            returnType=ArrayType(StringType()),
        )

        create_ref_temp_views(self.spark)

        sql_views = {
            "all_transactions": {
                "vw_fabs_from_source__update": self.from_source_sql("fabs", None, "update"),
                "vw_fabs_from_source__insert": self.from_source_sql("fabs", None, "insert"),
                "vw_fpds_from_source__update": self.from_source_sql("fpds", None, "update"),
                "vw_fpds_from_source__insert": self.from_source_sql("fpds", None, "insert"),
                # vw_all_transaction__update
                # vw_all_transaction__insert
            },
            "transaction_fabs": [
                # vw_transaction_normalized_upsert
                # MERGE INTO int.transaction_fabs
            ],
            "transaction_fpds": [
                # vw_transaction_normalized_upsert
                # MERGE INTO int.transaction_fpds
            ],
            "transaction_normalized": [
                # vw_transaction_normalized_upsert
                # MERGE INTO int.transaction_normalized
            ],
            "awards": [
                # vw_award_upsert
                #      (should reference int.awards as well to handle logic in usaspending_api/etl/award_helpers.py)
                # MERGE INTO int.awards
            ],
        }

        """
        create table used to track last etl run for each table
            last_load_date

        if etl_level == "fpds"
            run all_transactions
            run transaction_fpds
        elif etl_level == "normalized"
            run all_transactions
            run transaction_normalized
        """
        if spark_created_by_command:
            self.spark.stop()

    @staticmethod
    def from_source_sql(transaction_type, last_etl_date, query_type):
        if transaction_type == "fabs":
            bronze_table_name = ""
            silver_table_name = ""
            unique_id = ""
            col_info = copy.copy(TRANSACTION_FABS_COLUMN_INFO)
        elif transaction_type == "fpds":
            bronze_table_name = "raw.detached_award_procurement"
            silver_table_name = "int.transaction_fpds"
            unique_id = "detached_award_procurement_id"
            col_info = TRANSACTION_FPDS_COLUMN_INFO
        else:
            raise ValueError(f"Invalid value for 'transaction_type'; must select either: 'fabs' or 'fpds'")

        select_cols = []
        omitted_cols = ["transaction_id"]
        for col in list(filter(lambda x: x.silver_name not in omitted_cols, col_info)):
            if col.is_cast:
                select_cols.append(
                    f"CAST({bronze_table_name}.{col.bronze_name} AS {col.delta_type}) AS {col.silver_name}"
                )
            else:
                select_cols.append(f"{bronze_table_name}.{col.bronze_name} AS {col.silver_name}")

        # Additional conditional columns needed to support int.transaction_normalized
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
                    # is_fpds
                    "TRUE AS is_fpds"
                ]
            )
        select_cols.extend(
            [
                # business_categories; UDF defined on Spark session in handler
                f"build_business_categories({bronze_table_name}.*, {transaction_type}) AS business_categories"
            ]
        )

        sql = f"""
            CREATE OR REPLACE TEMPORARY VIEW vw_{transaction_type}_from_source__{query_type} AS (
                SELECT
                    {", ".join(select_cols)},
                    -- Common fields between source data types that need a JOIN to the bronze_table
                    awarding_agency.id AS awarding_agency_id,
                    funding_agency.id AS funding_agency_id
                FROM {bronze_table_name}
                LEFT OUTER JOIN global_temp.subtier_agency funding_agency ON (
                    funding_agency.subtier_code = {bronze_table_name}.awarding_sub_tier_agency_c
                )
                LEFT OUTER JOIN global_temp.subtier_agency awarding_agency ON (
                    awarding_agency.subtier_code = {bronze_table_name}.funding_sub_tier_agency_co
                )
                WHERE
                    bronze_table.updated_at >= {last_etl_date}
                    AND {"NOT" if query_type == "INSERT" else ""} EXISTS (
                        SELECT *
                        FROM {silver_table_name}
                        WHERE
                            {silver_table_name}.{unique_id} = {bronze_table_name}.{unique_id}
                    )
            )
        """
        return sql

    @staticmethod
    def all_transactions_insert_sql(query_type, next_id_val):
        if query_type == "insert":
            transaction_id_select = ""
        sql = f"""
            CREATE OR REPLACE TEMPORARY VIEW vw_all_transactions__{query_type} AS (
                SELECT
                    {transaction_id_select},
                    left_table.*,
                    right_table.*
            )
        """
        return sql

    @staticmethod
    def all_transactions_update_sql(query_type):
        sql = f"""
            CREATE OR REPLACE TEMPORARY VIEW vw_all_transactions__update AS (
                SELECT
                    transaction_normalized.transaction_id,
                    transaction_fabs_update.*,
                    transaction_fpds_update.*
                FROM
                    transaction_normalized
            )
        """
        return sql
