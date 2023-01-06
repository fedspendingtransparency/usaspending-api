import copy
import logging
import re

from contextlib import contextmanager
from datetime import datetime, timezone

from django.core.management import BaseCommand, call_command
from django.db import connection
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.utils import AnalysisException

from usaspending_api.awards.delta_models.awards import AWARDS_COLUMNS
from usaspending_api.broker.helpers.build_business_categories_boolean_dict import fpds_boolean_columns
from usaspending_api.broker.helpers.get_business_categories import (
    get_business_categories_fabs,
    get_business_categories_fpds,
)
from usaspending_api.broker.helpers.last_load_date import (
    get_last_load_date,
    update_last_load_date,
    get_earliest_load_date,
)
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    get_active_spark_session,
    configure_spark_session,
    get_jvm_logger,
)
from usaspending_api.config import CONFIG
from usaspending_api.transactions.delta_models.transaction_fabs import (
    TRANSACTION_FABS_COLUMN_INFO,
    TRANSACTION_FABS_COLUMNS,
    FABS_TO_NORMALIZED_COLUMN_INFO,
)
from usaspending_api.transactions.delta_models.transaction_fpds import (
    TRANSACTION_FPDS_COLUMN_INFO,
    TRANSACTION_FPDS_COLUMNS,
    DAP_TO_NORMALIZED_COLUMN_INFO,
)
from usaspending_api.transactions.delta_models.transaction_normalized import TRANSACTION_NORMALIZED_COLUMNS


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
    no_initial_copy: bool
    logger: logging.Logger
    spark: SparkSession
    award_id_lookup_delete_subquery: str = """
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

    def add_arguments(self, parser):
        parser.add_argument(
            "--etl-level",
            type=str,
            required=True,
            help="The silver delta table that should be updated from the bronze delta data.",
            choices=[
                "award_id_lookup",
                "awards",
                "initial_run",
                "transaction_fabs",
                "transaction_fpds",
                "transaction_id_lookup",
                "transaction_normalized",
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
            "--no-initial-copy",
            action="store_true",
            required=False,
            help="Whether to skip copying tables from the 'raw' database to the 'int' database during initial_run.",
        )

    def handle(self, *args, **options):
        with self.prepare_spark():
            self.etl_level = options["etl_level"]
            self.spark_s3_bucket = options["spark_s3_bucket"]
            self.no_initial_copy = options["no_initial_copy"]

            # Capture minimum last load date of the source tables to update the "last_load_date" after completion
            next_last_load = get_earliest_load_date("source_procurement_transaction", "source_assistance_transaction")
            if not next_last_load:
                next_last_load = datetime.utcfromtimestamp(0)

            if self.etl_level == "initial_run":
                self.logger.info("Running initial setup for transaction_id_lookup and award_id_lookup tables")
                self.initial_run(next_last_load)
                return

            if self.etl_level == "award_id_lookup":
                self.logger.info(f"Running pre-delete SQL for '{self.etl_level}' ETL")
                self.award_id_lookup_pre_delete()

            self.logger.info(f"Running delete SQL for '{self.etl_level}' ETL")
            self.spark.sql(self.delete_records_sql())

            if self.etl_level == "award_id_lookup":
                self.logger.info(f"Running post-delete SQL for '{self.etl_level}' ETL")
                self.award_id_lookup_post_delete()

            last_etl_date = get_last_load_date(self.etl_level)
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
            elif self.etl_level in ("transaction_fabs", "transaction_fpds"):
                self.spark.sql(self.transaction_fabs_fpds_merge_into_sql())
            elif self.etl_level == "transaction_normalized":
                create_ref_temp_views(self.spark)
                self.spark.sql(self.transaction_normalized_merge_into_sql("fabs"))
                self.spark.sql(self.transaction_normalized_merge_into_sql("fpds"))
            elif self.etl_level == "awards":
                self.update_awards()

            update_last_load_date(self.etl_level, next_last_load)

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

    def award_id_lookup_pre_delete(self):
        """
        Need to create a table to store the award ids corresponding to the transaction_unique_ids that are
        about to be deleted.
        """
        # This needs to be done here because Spark SQL does not have a RETURNING clause.
        # Also, this needs to be a table, not a view because the records will be deleted from the table,
        #   so a view would not be able to retain the values after the delete.
        # This table does not need to be an external table, though, because it will be dropped in the
        #   post-delete code.  Will put table in temp DB, therefore.
        destination_database = "temp"
        self.logger.info(f"Creating database {destination_database}, if not already existing.")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {destination_database};")
        self.spark.sql(
            f"""
                CREATE OR REPLACE TABLE {destination_database}.possibly_modified_awards
                USING DELTA
                AS (
                    WITH txns_to_delete AS (
                        {self.award_id_lookup_delete_subquery}
                    )
                    SELECT DISTINCT(award_id) AS award_id
                    FROM int.award_id_lookup AS aidlu INNER JOIN txns_to_delete AS to_del ON (
                        aidlu.transaction_unique_id = to_del.id_to_remove
                    )
                )
            """
        )

    def delete_records_sql(self):
        if self.etl_level == "transaction_id_lookup":
            id_col = "transaction_id"
            subquery = """
                SELECT transaction_id AS id_to_remove
                FROM int.transaction_id_lookup AS tidlu LEFT JOIN raw.detached_award_procurement AS dap ON (
                    tidlu.detached_award_procurement_id = dap.detached_award_procurement_id
                )
                WHERE tidlu.detached_award_procurement_id IS NOT NULL AND dap.detached_award_procurement_id IS NULL
                UNION ALL
                SELECT transaction_id AS id_to_remove
                FROM int.transaction_id_lookup AS tidlu LEFT JOIN raw.published_fabs AS pfabs ON (
                    tidlu.published_fabs_id = pfabs.published_fabs_id
                )
                WHERE tidlu.published_fabs_id IS NOT NULL AND pfabs.published_fabs_id IS NULL
            """
        elif self.etl_level == "award_id_lookup":
            id_col = "transaction_unique_id"
            subquery = self.award_id_lookup_delete_subquery
        elif self.etl_level in ("transaction_fabs", "transaction_fpds", "transaction_normalized"):
            id_col = "id" if self.etl_level == "transaction_normalized" else "transaction_id"
            subquery = f"""
                SELECT {self.etl_level}.{id_col} AS id_to_remove
                FROM int.{self.etl_level} LEFT JOIN int.transaction_id_lookup ON (
                    {self.etl_level}.{id_col} = transaction_id_lookup.transaction_id
                )
                WHERE {self.etl_level}.{id_col} IS NOT NULL AND transaction_id_lookup.transaction_id IS NULL
            """
        elif self.etl_level == "awards":
            id_col = "id"
            subquery = f"""
                SELECT awards.id AS id_to_remove
                FROM int.awards LEFT JOIN int.award_id_lookup ON awards.id = award_id_lookup.award_id
                WHERE awards.id IS NOT NULL AND award_id_lookup.award_id IS NULL
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

    def award_id_lookup_post_delete(self):
        """
        Now that deletion from the award_id_lookup table is done, we need to figure out which awards in
        possibly_modified_awards remain.
        """

        # Of those possibly_modified_awards, find those that remain after deleting transactions.  Those are
        #   the award_ids which have had some, but not all, transactions deleted from them.
        # This function will always append to int.award_ids_delete_modified because award_id_lookup ETL
        #   level could be run more than once before awards ETL level is run.
        self.spark.sql(
            f"""
                INSERT INTO int.award_ids_delete_modified
                    SELECT DISTINCT(aidlu.award_id)
                    FROM temp.possibly_modified_awards AS poss_mod_awards INNER JOIN int.award_id_lookup AS aidlu ON (
                        poss_mod_awards.award_id = aidlu.award_id
                    )
            """
        )

        # Now, we're done with the possibly_modified_awards table.
        self.spark.sql("DROP TABLE temp.possibly_modified_awards")

    def update_awards(self):
        load_datetime = datetime.now(timezone.utc)

        insert_special_columns = ["subaward_count", "total_subaward_amount", "create_date", "update_date"]
        set_subquery_ignored_columns = insert_special_columns + ["id"]

        # Use a UNION in award_ids_to_update, not UNION ALL because there could be duplicates among the award ids
        # between the query parts or in int.award_ids_delete_modified.
        subquery = f"""
            WITH
            award_ids_to_update AS (
                SELECT DISTINCT(award_id)
                FROM int.award_id_lookup
                WHERE transaction_unique_id IN (SELECT transaction_unique_id
                                                FROM int.transaction_normalized
                                                WHERE update_date >= '{self.last_etl_date}')
                UNION
                SELECT award_id FROM int.award_ids_delete_modified
            ),
            transaction_earliest AS (
                SELECT * FROM (
                    SELECT
                        tn.award_id AS id,
                        tn.id AS earliest_transaction_id,
                        tn.action_date AS date_signed,
                        tn.description,
                        tn.period_of_performance_start_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY tn.award_id
                            ORDER BY tn.award_id, tn.action_date ASC, tn.modification_number ASC,
                                     tn.transaction_unique_id ASC
                        ) AS rank
                    FROM int.transaction_normalized AS tn
                    WHERE tn.award_id IN (SELECT * FROM award_ids_to_update)
                )
                WHERE rank = 1
            ),
            transaction_latest as (
                SELECT * FROM (
                    SELECT
                        -- General update columns (id at top, rest alphabetically by alias/name)
                        tn.award_id AS id,
                        tn.awarding_agency_id,
                        CASE
                            WHEN tn.type IN ('A', 'B', 'C', 'D')      THEN 'contract'
                            WHEN tn.type IN ('02', '03', '04', '05')  THEN 'grant'
                            WHEN tn.type IN ('06', '10')              THEN 'direct payment'
                            WHEN tn.type IN ('07', '08')              THEN 'loans'
                            WHEN tn.type = '09'                       THEN 'insurance'
                            WHEN tn.type = '11'                       THEN 'other'
                            WHEN tn.type LIKE 'IDV%%'                 THEN 'idv'
                            ELSE NULL
                        END AS category,
                        tn.action_date AS certified_date,
                        CASE
                            WHEN month(tn.action_date) > 9 THEN year(tn.action_date) + 1
                            ELSE year(tn.action_date)
                        END AS fiscal_year,
                        tn.funding_agency_id,
                        tn.unique_award_key AS generated_unique_award_id,
                        tn.is_fpds,
                        tn.last_modified_date,
                        tn.id AS latest_transaction_id,
                        tn.period_of_performance_current_end_date,
                        tn.transaction_unique_id,
                        tn.type,
                        tn.type_description,
                        -- FPDS Columns
                        fpds.agency_id AS fpds_agency_id,
                        fpds.referenced_idv_agency_iden AS fpds_parent_agency_id,
                        fpds.parent_award_id AS parent_award_piid,
                        fpds.piid,
                        -- FABS Columns
                        fabs.fain,
                        fabs.uri,
                        -- Excecutive Compensation
                        -- This might not be correct. In award_helpers.py some extra CTE logic is being done to
                        -- select executive compensation
                        COALESCE(fpds.officer_1_amount, fabs.officer_1_amount) AS officer_1_amount,
                        COALESCE(fpds.officer_1_name, fabs.officer_1_name)     AS officer_1_name,
                        COALESCE(fpds.officer_2_amount, fabs.officer_2_amount) AS officer_2_amount,
                        COALESCE(fpds.officer_2_name, fabs.officer_2_name)     AS officer_2_name,
                        COALESCE(fpds.officer_3_amount, fabs.officer_3_amount) AS officer_3_amount,
                        COALESCE(fpds.officer_3_name, fabs.officer_3_name)     AS officer_3_name,
                        COALESCE(fpds.officer_4_amount, fabs.officer_4_amount) AS officer_4_amount,
                        COALESCE(fpds.officer_4_name, fabs.officer_4_name)     AS officer_4_name,
                        COALESCE(fpds.officer_5_amount, fabs.officer_5_amount) AS officer_5_amount,
                        COALESCE(fpds.officer_5_name, fabs.officer_5_name)     AS officer_5_name,
                        -- Other
                        'DBR' AS data_source,
                        -- Windowing Function
                        ROW_NUMBER() OVER (
                            PARTITION BY tn.award_id
                            ORDER BY tn.award_id, tn.action_date DESC, tn.modification_number DESC,
                                     tn.transaction_unique_id DESC
                        ) as rank
                    FROM int.transaction_normalized AS tn
                    LEFT JOIN int.transaction_fpds AS fpds ON fpds.transaction_id = tn.id
                    LEFT JOIN int.transaction_fabs AS fabs ON fabs.transaction_id = tn.id
                    WHERE tn.award_id IN (SELECT * FROM award_ids_to_update)
                )
                WHERE rank = 1
            ),
            transaction_totals as (
                SELECT
                    -- Transaction Normalized Fields
                    tn.award_id AS id,
                    SUM(tn.federal_action_obligation)   AS total_obligation,
                    SUM(tn.original_loan_subsidy_cost)  AS total_subsidy_cost,
                    SUM(tn.funding_amount)              AS total_funding_amount,
                    SUM(tn.face_value_loan_guarantee)   AS total_loan_value,
                    SUM(tn.non_federal_funding_amount)  AS non_federal_funding_amount,
                    SUM(tn.indirect_federal_sharing)    AS total_indirect_federal_sharing,
                    -- Transaction FPDS Fields
                    SUM(CAST(fpds.base_and_all_options_value AS NUMERIC)) AS base_and_all_options_value,
                    SUM(CAST(fpds.base_exercised_options_val AS NUMERIC)) AS base_exercised_options_val
                FROM int.transaction_normalized AS tn
                LEFT JOIN int.transaction_fpds AS fpds ON tn.id = fpds.transaction_id
                WHERE tn.award_id IN (SELECT * FROM award_ids_to_update)
                GROUP BY tn.award_id
            )
            SELECT latest.id, {", ".join([col_name
                                          for col_name in AWARDS_COLUMNS
                                          if col_name not in set_subquery_ignored_columns])}
            FROM transaction_latest AS latest
            INNER JOIN transaction_earliest AS earliest ON latest.id = earliest.id
            INNER JOIN transaction_totals AS totals on latest.id = totals.id
        """

        # On set, create_date will not be changed and update_date will be set below.  The subaward columns will not
        # be changed, and id is used to match.  All other column values will come from the subquery.
        set_cols = [
            f"int.awards.{col_name} = source_subquery.{col_name}"
            for col_name in AWARDS_COLUMNS
            if col_name not in set_subquery_ignored_columns
        ]
        set_cols.append(f"""int.awards.update_date = '{load_datetime.isoformat(" ")}'""")

        # Move insert_special_columns to the end of the list of column names for ease of handling
        # during record insert
        insert_col_name_list = [col_name for col_name in AWARDS_COLUMNS if col_name not in insert_special_columns]
        insert_col_name_list.extend(insert_special_columns)
        insert_col_names = ", ".join([col_name for col_name in insert_col_name_list])

        # On insert, all values except for those in insert_special_columns will come from the subquery
        insert_value_list = insert_col_name_list[:-4]
        insert_value_list.extend(["NULL"] * 2)
        insert_value_list.extend([f"""'{load_datetime.isoformat(" ")}'"""] * 2)
        insert_values = ", ".join([value for value in insert_value_list])

        sql = f"""
            MERGE INTO int.awards
            USING (
                {subquery}
            ) AS source_subquery
            ON awards.id = source_subquery.id
            WHEN MATCHED
                THEN UPDATE SET
                {", ".join(set_cols)}
            WHEN NOT MATCHED
                THEN INSERT
                    ({insert_col_names})
                    VALUES ({insert_values})
        """

        self.spark.sql(sql)

        # Now that the award table update is done, we can empty the award_ids_delete_modified table.
        # Note that an external table can't be TRUNCATED; use blanket DELETE instead.
        self.spark.sql("DELETE FROM int.award_ids_delete_modified")

    def source_subquery_sql(self, transaction_type=None):
        def handle_column(col, bronze_table_name):
            if col.handling == "cast":
                return f"CAST({bronze_table_name}.{col.source} AS {col.delta_type}) AS {col.dest_name}"
            elif col.handling == "literal":
                # Use col.source directly as the value
                return f"{col.source} AS {col.dest_name}"
            elif col.handling in ("parse_string_date", "truncate_string_date"):
                # Require a separator for mmddYYYY, but not for YYYYmmdd, or there is no way to tell them apart
                # from just regexp.
                regexp_mmddYYYY = (
                    r"(\\d{2})(?<sep>[-/])(\\d{2})(\\k<sep>)(\\d{4})(.\\d{2}:\\d{2}:\\d{2}([+-]\\d{2}:\\d{2})?)?"
                )
                regexp_YYYYmmdd = (
                    r"(\\d{4})(?<sep>[-/]?)(\\d{2})(\\k<sep>)(\\d{2})(.\\d{2}:\\d{2}:\\d{2}([+-]\\d{2}:\\d{2})?)?"
                )
                if col.handling == "parse_string_date":
                    return f"""
                        CASE WHEN regexp({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}')
                                  THEN CAST((regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 5)
                                             || '-' ||
                                             regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 1)
                                             || '-' ||
                                             regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 3))
                                            AS DATE)
                             ELSE CAST((regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 1)
                                        || '-' ||
                                        regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 3)
                                        || '-' ||
                                        regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 5))
                                       AS DATE)
                        END AS {col.dest_name}
                    """
                else:
                    # These are string fields that actually hold DATES/TIMESTAMPS, but need the non-DATE part discarded,
                    # even though they remain as strings
                    return f"""
                        CASE WHEN regexp({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}')
                                  THEN (regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 5)
                                        || '-' ||
                                        regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 1)
                                        || '-' ||
                                        regexp_extract({bronze_table_name}.{col.source}, '{regexp_mmddYYYY}', 3))
                             ELSE (regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 1)
                                   || '-' ||
                                   regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 3)
                                   || '-' ||
                                   regexp_extract({bronze_table_name}.{col.source}, '{regexp_YYYYmmdd}', 5))
                        END AS {col.dest_name}
                    """
            elif col.delta_type.upper() == "STRING":
                # Capitalize all string values
                return f"ucase({bronze_table_name}.{col.source}) AS {col.dest_name}"
            elif col.delta_type.upper() == "BOOLEAN" and not col.handling == "leave_null":
                # Unless specified, convert any nulls to false for boolean columns
                return f"COALESCE({bronze_table_name}.{col.source}, FALSE) AS {col.dest_name}"
            else:
                return f"{bronze_table_name}.{col.source} AS {col.dest_name}"

        def select_columns_transaction_fabs_fpds(bronze_table_name):
            if self.etl_level == "transaction_fabs":
                col_info = copy.copy(TRANSACTION_FABS_COLUMN_INFO)
            elif self.etl_level == "transaction_fpds":
                col_info = copy.copy(TRANSACTION_FPDS_COLUMN_INFO)
            else:
                raise RuntimeError(
                    f"Function called with invalid 'etl_level': {self.etl_level}. "
                    "Only for use with 'transaction_fabs' or 'transaction_fpds' etl_level."
                )

            select_cols = []
            for col in filter(lambda x: x.dest_name not in ["transaction_id"], col_info):
                select_cols.append(handle_column(col, bronze_table_name))

            return select_cols

        def select_columns_transaction_normalized(bronze_table_name):
            select_cols = [
                "award_id_lookup.award_id",
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
                # Add a couple of non-boolean columns that are needed in the business category logic
                fpds_business_category_columns.extend(["contracting_officers_deter", "domestic_or_foreign_entity"])
                named_struct_text = ", ".join(
                    [f"'{col}', {bronze_table_name}.{col}" for col in fpds_business_category_columns]
                )

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
                        """,
                    ]
                )
                map_col_info = copy.copy(DAP_TO_NORMALIZED_COLUMN_INFO)

            for col in map_col_info:
                select_cols.append(handle_column(col, bronze_table_name))

            return select_cols

        if self.etl_level == "transaction_fabs":
            bronze_table_name = "raw.published_fabs"
            unique_id = "published_fabs_id"
            id_col_name = "transaction_id"
            select_columns = select_columns_transaction_fabs_fpds(bronze_table_name)
            additional_joins = ""
        elif self.etl_level == "transaction_fpds":
            bronze_table_name = "raw.detached_award_procurement"
            unique_id = "detached_award_procurement_id"
            id_col_name = "transaction_id"
            select_columns = select_columns_transaction_fabs_fpds(bronze_table_name)
            additional_joins = ""
        elif self.etl_level == "transaction_normalized":
            if transaction_type == "fabs":
                bronze_table_name = "raw.published_fabs"
                unique_id = "published_fabs_id"
            elif transaction_type == "fpds":
                bronze_table_name = "raw.detached_award_procurement"
                unique_id = "detached_award_procurement_id"
            else:
                raise ValueError(
                    f"Invalid value for 'transaction_type': {transaction_type}; " "must select either: 'fabs' or 'fpds'"
                )

            id_col_name = "id"
            select_columns = select_columns_transaction_normalized(bronze_table_name)
            additional_joins = f"""
                INNER JOIN int.award_id_lookup AS award_id_lookup ON (
                    {bronze_table_name}.{unique_id} = award_id_lookup.{unique_id}
                )
                LEFT OUTER JOIN global_temp.subtier_agency AS funding_subtier_agency ON (
                    funding_subtier_agency.subtier_code = {bronze_table_name}.funding_sub_tier_agency_co
                )
                LEFT OUTER JOIN global_temp.agency AS funding_agency ON (
                    funding_agency.subtier_agency_id = funding_subtier_agency.subtier_agency_id
                )
                LEFT OUTER JOIN global_temp.subtier_agency AS awarding_subtier_agency ON (
                    awarding_subtier_agency.subtier_code = {bronze_table_name}.awarding_sub_tier_agency_c
                )
                LEFT OUTER JOIN global_temp.agency AS awarding_agency ON (
                    awarding_agency.subtier_agency_id = awarding_subtier_agency.subtier_agency_id
                )
            """
        else:
            raise RuntimeError(
                f"Function called with invalid 'etl_level': {self.etl_level}. "
                "Only for use with 'transaction_fabs', 'transaction_fpds', or 'transaction_normalized' "
                "etl_level."
            )

        # {} expressions in f-strings can't contain backslashes, so need to be a little creative to get \n
        # into {} expression (see
        # https://stackoverflow.com/questions/67680296/syntaxerror-f-string-expression-part-cannot-include-a-backslash
        # and https://martinheinz.dev/blog/70, section 'Nested F-Strings')
        sql = f"""
            SELECT
                transaction_id_lookup.transaction_id AS {id_col_name},
                {","f'{chr(10)}    '.join(select_columns)}
            FROM {bronze_table_name}
            INNER JOIN int.transaction_id_lookup ON (
                {bronze_table_name}.{unique_id} = transaction_id_lookup.{unique_id}
            )
            {additional_joins}
            WHERE {bronze_table_name}.updated_at >= '{self.last_etl_date}'
        """

        return sql

    def transaction_fabs_fpds_merge_into_sql(self):
        if self.etl_level == "transaction_fabs":
            col_info = copy.copy(TRANSACTION_FABS_COLUMN_INFO)
        elif self.etl_level == "transaction_fpds":
            col_info = copy.copy(TRANSACTION_FPDS_COLUMN_INFO)
        else:
            raise RuntimeError(
                f"Function called with invalid 'etl_level': {self.etl_level}. "
                "Only for use with 'transaction_fabs' or 'transaction_fpds' etl_level."
            )

        set_cols = [f"silver_table.{col.dest_name} = source_subquery.{col.dest_name}" for col in col_info]
        silver_table_cols = ", ".join([col.dest_name for col in col_info])

        sql = f"""
            MERGE INTO int.{self.etl_level} AS silver_table
            USING (
                {self.source_subquery_sql()}
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
            raise ValueError(
                f"Invalid value for 'transaction_type': {transaction_type}. " "Must select either: 'fabs' or 'fpds'"
            )

        load_datetime = datetime.now(timezone.utc)
        special_columns = ["create_date", "update_date"]

        # On set, create_date will not be changed and update_date will be set below.  All other column
        # values will come from the subquery.
        set_cols = [
            f"int.transaction_normalized.{col_name} = source_subquery.{col_name}"
            for col_name in TRANSACTION_NORMALIZED_COLUMNS
            if col_name not in (special_columns + ["id"])
        ]
        set_cols.append(f"""int.transaction_normalized.update_date = '{load_datetime.isoformat(" ")}'""")

        # Move create_date and update_date to the end of the list of column names for ease of handling
        # during record insert
        insert_col_name_list = [
            col_name for col_name in TRANSACTION_NORMALIZED_COLUMNS if col_name not in special_columns
        ]
        insert_col_name_list.extend(special_columns)
        insert_col_names = ", ".join([col_name for col_name in insert_col_name_list])

        # On insert, all values except for create_date and update_date will come from the subquery
        insert_value_list = insert_col_name_list[:-2]
        insert_value_list.extend([f"""'{load_datetime.isoformat(" ")}'"""] * 2)
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
                ) AS transaction_id,
                all_new_transactions.detached_award_procurement_id,
                all_new_transactions.published_fabs_id,
                all_new_transactions.transaction_unique_id
            FROM (
                (
                    SELECT
                        dap.detached_award_procurement_id,
                        NULL AS published_fabs_id,
                        -- The transaction loader code will convert this to upper case, so use that version here.
                        ucase(dap.detached_award_proc_unique) AS transaction_unique_id
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
                        -- The transaction loader code will convert this to upper case, so use that version here.
                        ucase(pfabs.afa_generated_unique) AS transaction_unique_id
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
        poss_max_id = self.spark.sql("SELECT MAX(transaction_id) AS max_id FROM int.transaction_id_lookup").collect()[
            0
        ]["max_id"]
        if poss_max_id is None:
            # Since initial_run will always start the id sequence from at least 1, and we take the max of
            # poss_max_id and previous_max_id below, this can be set to 0 here.
            poss_max_id = 0
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
                ) AS award_id,
                all_new_awards.detached_award_procurement_id,
                all_new_awards.published_fabs_id,
                all_new_awards.transaction_unique_id,
                all_new_awards.unique_award_key AS generated_unique_award_id
            FROM (
                (
                    SELECT
                        dap.detached_award_procurement_id,
                        NULL AS published_fabs_id,
                        -- The transaction loader code will convert these to upper case, so use those versions here.
                        ucase(dap.detached_award_proc_unique) AS transaction_unique_id,
                        ucase(dap.unique_award_key) AS unique_award_key
                    FROM
                         dap_filtered AS dap LEFT JOIN int.award_id_lookup AS aidlu ON (
                            ucase(dap.detached_award_proc_unique) = aidlu.transaction_unique_id
                         )
                    WHERE aidlu.transaction_unique_id IS NULL
                )
                UNION ALL
                (
                    SELECT
                        NULL AS detached_award_procurement_id,
                        pfabs.published_fabs_id,
                        -- The transaction loader code will convert these to upper case, so use those versions here.
                        ucase(pfabs.afa_generated_unique) AS transaction_unique_id,
                        ucase(pfabs.unique_award_key) AS unique_award_key
                    FROM
                        pfabs_filtered AS pfabs LEFT JOIN int.award_id_lookup AS aidlu ON (
                            ucase(pfabs.afa_generated_unique) = aidlu.transaction_unique_id
                         )
                    WHERE aidlu.transaction_unique_id IS NULL
                )
            ) AS all_new_awards
        """
        )

        self.logger.info("Updating award_id_seq to the new maximum id value seen so far")
        poss_max_id = self.spark.sql("SELECT MAX(award_id) AS max_id FROM int.award_id_lookup").collect()[0]["max_id"]
        if poss_max_id is None:
            # Since initial_run will always start the id sequence from at least 1, and we take the max of
            # poss_max_id and previous_max_id below, this can be set to 0 here.
            poss_max_id = 0
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

    def initial_run(self, next_last_load):
        """
        Procedure to create & set up transaction_id_lookup and award_id_lookup tables and create other tables in
        int database that will be populated by subsequent calls.
        """
        delta_lake_s3_path = CONFIG.DELTA_LAKE_S3_PATH
        destination_database = "int"

        # transaction_id_lookup
        destination_table = "transaction_id_lookup"
        set_last_load_date = True

        self.logger.info(f"Creating database {destination_database}, if not already existing.")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {destination_database};")

        self.logger.info(f"Creating {destination_table} table")
        self.spark.sql(
            f"""
                CREATE OR REPLACE TABLE {destination_database}.{destination_table} (
                    transaction_id LONG NOT NULL,
                    detached_award_procurement_id INTEGER,
                    published_fabs_id INTEGER,
                    transaction_unique_id STRING NOT NULL
                )
                USING DELTA
                LOCATION 's3a://{self.spark_s3_bucket}/{delta_lake_s3_path}/{destination_database}/{destination_table}'
            """
        )

        # Test to see if raw.transaction_normalized exists
        try:
            self.spark.sql("SELECT 1 FROM raw.transaction_normalized")
        except AnalysisException as e:
            if re.match(r"Table or view not found: raw\.transaction_normalized", e.desc):
                # In this case, we just don't populate transaction_id_lookup
                self.logger.warn(
                    "Skipping population of transaction_id_lookup table; no raw.transaction_normalized table."
                )
            else:
                # Don't try to handle anything else
                raise e
        else:
            # Insert existing transactions into the lookup table
            self.logger.info("Populating transaction_id_lookup table from raw.transaction_normalized")
            # Note that the transaction loader code will convert string fields to upper case, so we have to match
            # on the upper-cased versions of the strings.
            self.spark.sql(
                f"""
                INSERT OVERWRITE {destination_database}.{destination_table}
                    SELECT
                        tn.id AS transaction_id,
                        dap.detached_award_procurement_id,
                        pfabs.published_fabs_id,
                        tn.transaction_unique_id
                    FROM raw.transaction_normalized AS tn
                    LEFT JOIN raw.detached_award_procurement AS dap ON (
                        tn.transaction_unique_id = ucase(dap.detached_award_proc_unique)
                    )
                    LEFT JOIN raw.published_fabs AS pfabs ON (
                        tn.transaction_unique_id = ucase(pfabs.afa_generated_unique)
                    )
                """
            )

        self.logger.info("Updating transaction_id_seq to the new max_id value")
        max_id = self.spark.sql(
            f"SELECT MAX(transaction_id) AS max_id FROM {destination_database}.{destination_table}"
        ).collect()[0]["max_id"]
        if max_id is None:
            # Can't set a Postgres sequence to 0, so set to 1 in this case.  If this happens, the transaction IDs
            # will start at 2.
            max_id = 1
            # Also, don't set the last load date in this case
            set_last_load_date = False
        with connection.cursor() as cursor:
            # Set is_called flag to false so that the next call to nextval() will return the specified value
            # https://www.postgresql.org/docs/13/functions-sequence.html
            cursor.execute(f"SELECT setval('transaction_id_seq', {max_id}, false)")

        if set_last_load_date:
            update_last_load_date(destination_table, next_last_load)
            # es_deletes should remain in lockstep with transaction load dates, so if they are reset, it should be reset
            update_last_load_date("es_deletes", next_last_load)

        # award_id_lookup
        destination_table = "award_id_lookup"
        set_last_load_date = True

        # Test to see if raw.transaction_normalized exists
        try:
            self.spark.sql("SELECT 1 FROM raw.transaction_normalized")
        except AnalysisException as e:
            if re.match(r"Table or view not found: raw\.transaction_normalized", e.desc):
                # In this case, we just don't check for NULLs in unique_award_key.
                pass
            else:
                # Don't try to handle anything else
                raise e
        else:
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

        self.logger.info(f"Creating {destination_table} table")
        self.spark.sql(
            f"""
                CREATE OR REPLACE TABLE {destination_database}.{destination_table} (
                    award_id LONG NOT NULL,
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

        # Test to see if raw.awards exists
        try:
            self.spark.sql("SELECT 1 FROM raw.awards")
        except AnalysisException as e:
            if re.match(r"Table or view not found: raw\.awards", e.desc):
                # In this case, we just don't populate award_id_lookup
                self.logger.warn("Skipping population of award_id_lookup table; no raw.awards table.")
            else:
                # Don't try to handle anything else
                raise e
        else:
            # Insert existing transactions into the lookup table
            self.logger.info("Populating award_id_lookup table from raw.awards")
            # Once again we have to match on the upper-cased versions of the strings from published_fabs
            # and detached_award_procurement.
            self.spark.sql(
                f"""
                INSERT OVERWRITE {destination_database}.{destination_table}
                    SELECT
                        existing_awards.id AS award_id,
                        existing_awards.detached_award_procurement_id,
                        existing_awards.published_fabs_id,
                        existing_awards.transaction_unique_id,
                        existing_awards.generated_unique_award_id
                    FROM (
                        (
                            SELECT
                                aw.id,
                                dap.detached_award_procurement_id,
                                NULL AS published_fabs_id,
                                -- The transaction loader code will convert this to upper case, so use that
                                -- version here.
                                ucase(dap.detached_award_proc_unique) AS transaction_unique_id,
                                aw.generated_unique_award_id
                            FROM
                                raw.awards AS aw INNER JOIN raw.detached_award_procurement AS dap ON (
                                    aw.generated_unique_award_id = ucase(dap.unique_award_key)
                                )
                        )
                        UNION ALL
                        (
                            SELECT
                                aw.id,
                                NULL AS detached_award_procurement_id,
                                pfabs.published_fabs_id,
                                -- The transaction loader code will convert this to upper case, so use that
                                -- version here.
                                ucase(pfabs.afa_generated_unique) AS transaction_unique_id,
                                aw.generated_unique_award_id
                            FROM
                                raw.awards AS aw INNER JOIN raw.published_fabs AS pfabs ON (
                                    aw.generated_unique_award_id = ucase(pfabs.unique_award_key)
                                )
                        )
                    ) AS existing_awards
                """
            )

        self.logger.info("Updating award_id_seq to the new max_id value")
        max_id = self.spark.sql(
            f"SELECT MAX(award_id) AS max_id FROM {destination_database}.{destination_table}"
        ).collect()[0]["max_id"]
        if max_id is None:
            # Can't set a Postgres sequence to 0, so set to 1 in this case.  If this happens, the award IDs
            # will start at 2.
            max_id = 1
            # Also, don't set the last load date in this case
            set_last_load_date = False
        with connection.cursor() as cursor:
            # Set is_called flag to false so that the next call to nextval() will return the specified value
            # https://www.postgresql.org/docs/13/functions-sequence.html
            cursor.execute(f"SELECT setval('award_id_seq', {max_id}, false)")

        if set_last_load_date:
            update_last_load_date(destination_table, next_last_load)
            # es_deletes should remain in lockstep with transaction load dates, so if they are reset, it should be reset
            update_last_load_date("es_deletes", next_last_load)

        # Need a table to keep track of awards in which some, but not all, transactions are deleted.
        destination_table = "award_ids_delete_modified"

        self.logger.info(f"Creating {destination_table} table")
        self.spark.sql(
            f"""
                CREATE OR REPLACE TABLE {destination_database}.{destination_table} (
                    award_id LONG NOT NULL
                )
                USING DELTA
                LOCATION 's3a://{self.spark_s3_bucket}/{delta_lake_s3_path}/{destination_database}/{destination_table}'
            """
        )

        # Nothing to add to this table here.

        # Create other tables in 'int' database
        self.logger.info("Creating tables in new database location")
        table_to_col_names_dict = {}
        table_to_col_names_dict["transaction_fabs"] = TRANSACTION_FABS_COLUMNS
        table_to_col_names_dict["transaction_fpds"] = TRANSACTION_FPDS_COLUMNS
        table_to_col_names_dict["transaction_normalized"] = list(TRANSACTION_NORMALIZED_COLUMNS)
        table_to_col_names_dict["awards"] = list(AWARDS_COLUMNS)
        for destination_table, col_names in table_to_col_names_dict.items():
            call_command(
                "create_delta_table",
                "--destination-table",
                destination_table,
                "--spark-s3-bucket",
                self.spark_s3_bucket,
                "--alt-db",
                destination_database,
            )

            if not self.no_initial_copy:
                # Test to see if the raw table exists
                try:
                    self.spark.sql(f"SELECT 1 FROM raw.{destination_table}")
                except AnalysisException as e:
                    if re.match(rf"Table or view not found: raw\.{destination_table}", e.desc):
                        # In this case, we just don't copy anything over
                        self.logger.warn(
                            f"Skipping copy of {destination_table} table from 'raw' to 'int' database; "
                            f"no raw.{destination_table} table."
                        )
                    else:
                        # Don't try to handle anything else
                        raise e
                else:
                    # Handle the possibility that the order of columns is different between the raw and int tables.
                    self.spark.sql(
                        f"""
                        INSERT OVERWRITE {destination_database}.{destination_table} ({", ".join(col_names)})
                            SELECT {", ".join(col_names)} FROM raw.{destination_table}
                        """
                    )

                    count = self.spark.sql(
                        f"SELECT COUNT(*) AS count FROM {destination_database}.{destination_table}"
                    ).collect()[0]["count"]
                    if count > 0:
                        update_last_load_date(destination_table, next_last_load)
                        # es_deletes should remain in lockstep with transaction load dates, so if they are reset,
                        # it should be reset
                        update_last_load_date("es_deletes", next_last_load)
