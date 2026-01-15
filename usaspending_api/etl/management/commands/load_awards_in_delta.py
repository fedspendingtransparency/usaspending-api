import logging
from contextlib import contextmanager
from datetime import datetime, timezone

from django.core.management import BaseCommand, call_command
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType

from usaspending_api.awards.delta_models.awards import AWARDS_COLUMNS
from usaspending_api.broker.helpers.get_business_categories import (
    get_business_categories_fabs,
    get_business_categories_fpds,
)
from usaspending_api.broker.helpers.last_load_date import (
    get_earliest_load_date,
    update_last_load_date,
)
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)
from usaspending_api.config import CONFIG


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = """
        This command reads transaction data from source / bronze tables in delta and creates the delta silver tables
        for awards.
    """

    spark_s3_bucket: str
    spark: SparkSession

    def add_arguments(self, parser):
        parser.add_argument(
            "--spark-s3-bucket",
            type=str,
            required=False,
            default=CONFIG.SPARK_S3_BUCKET,
            help="The destination bucket in S3 for creating the tables.",
        )

    def handle(self, *args, **options):
        with self.prepare_spark():
            self.spark_s3_bucket = options["spark_s3_bucket"]

            # Capture earliest last load date of the source tables to update the "last_load_date" after completion
            next_last_load = get_earliest_load_date(
                ("source_procurement_transaction", "source_assistance_transaction"), datetime.utcfromtimestamp(0)
            )

            # Do this check now to avoid uncaught errors later when running queries
            # Use 'int' because that is what will be targeted for deletes/updates/etc.
            table_exists = self.spark._jsparkSession.catalog().tableExists(f"int.awards")
            if not table_exists:
                raise Exception(f"Table: int.awards does not exist.")

            logger.info(f"Running delete SQL for awards ETL")
            self.spark.sql(self.delete_records_sql())

            logger.info(f"Running UPSERT SQL for awards ETL")
            self.update_awards()
            update_last_load_date("awards", next_last_load)

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
        id_col = "generated_unique_award_id"
        # TODO could do an outer join here to find awards that do not join to transaction fpds or transaction fabs
        subquery = """
            SELECT awards.generated_unique_award_id AS id_to_remove
            FROM int.awards
            LEFT JOIN int.transaction_normalized on awards.transaction_unique_id = transaction_normalized.transaction_unique_id
            WHERE awards.generated_unique_award_id IS NOT NULL AND transaction_normalized.transaction_unique_id IS NULL
        """

        sql = f"""
            MERGE INTO int.awards
            USING (
                {subquery}
            ) AS deleted_records
            ON awards.{id_col} = deleted_records.id_to_remove
            WHEN MATCHED
            THEN DELETE
        """
        return sql

    def update_awards(self):
        load_datetime = datetime.now(timezone.utc)

        set_insert_special_columns = ["total_subaward_amount", "create_date", "update_date"]
        subquery_ignored_columns = set_insert_special_columns + ["subaward_count"]

        # Use a UNION in award_ids_to_update, not UNION ALL because there could be duplicates among the award ids
        # between the query parts or in int.award_ids_delete_modified.
        subquery = f"""            
            WITH
            transaction_earliest AS (
                SELECT * FROM (
                    SELECT
                        tn.unique_award_key,
                        tn.transaction_unique_id AS earliest_transaction_id,
                        tn.action_date AS date_signed,
                        tn.description,
                        tn.period_of_performance_start_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY tn.unique_award_key
                            /* NOTE: In Postgres, the default sorting order sorts NULLs as larger than all other values.
                               However, in Spark, the default sorting order sorts NULLs as smaller than all other
                               values.  In the Postgres transaction loader the default sorting behavior was used, so to
                               be consistent with the behavior of the previous loader, we need to reverse the default
                               Spark NULL sorting behavior for any field that can be NULL. */
                            ORDER BY tn.unique_award_key, tn.action_date ASC NULLS LAST, tn.modification_number ASC NULLS LAST,
                                     tn.transaction_unique_id ASC
                        ) AS rank
                    FROM int.transaction_normalized AS tn
                )
                WHERE rank = 1
            ),
            transaction_latest AS (
                SELECT * FROM (
                    SELECT
                        -- General update columns (id at top, rest alphabetically by alias/name)
                        tn.unique_award_key,
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
                        tn.transaction_unique_id AS latest_transaction_id,
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
                        -- Other
                        'DBR' AS data_source,
                        -- Windowing Function
                        ROW_NUMBER() OVER (
                            PARTITION BY tn.unique_award_key
                            -- See note in transaction_earliest about NULL ordering.
                            ORDER BY tn.unique_award_key, tn.action_date DESC NULLS FIRST,
                                     tn.modification_number DESC NULLS FIRST, tn.transaction_unique_id DESC
                        ) as rank
                    FROM int.transaction_normalized AS tn
                    LEFT JOIN int.transaction_fpds AS fpds ON fpds.detached_award_proc_unique = tn.transaction_unique_id
                    LEFT JOIN int.transaction_fabs AS fabs ON fabs.afa_generated_unique = tn.transaction_unique_id
                )
                WHERE rank = 1
            ),
            -- For executive compensation information, we want the latest transaction for each award
            -- for which there is at least an officer_1_name.
            transaction_ec AS (
                SELECT * FROM (
                    SELECT
                        tn.unique_award_key,
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
                        ROW_NUMBER() OVER (
                            PARTITION BY tn.unique_award_key
                            -- See note in transaction_earliest about NULL ordering.
                            ORDER BY tn.unique_award_key, tn.action_date DESC NULLS FIRST,
                                     tn.modification_number DESC NULLS FIRST, tn.transaction_unique_id DESC
                        ) as rank
                    FROM int.transaction_normalized AS tn
                    LEFT JOIN int.transaction_fpds AS fpds ON fpds.detached_award_proc_unique = tn.transaction_unique_id
                    LEFT JOIN int.transaction_fabs AS fabs ON fabs.afa_generated_unique = tn.transaction_unique_id
                    WHERE fpds.officer_1_name IS NOT NULL OR fabs.officer_1_name IS NOT NULL
                )
                WHERE rank = 1
            ),
            transaction_totals AS (
                SELECT
                    -- Transaction Normalized Fields
                    tn.unique_award_key,
                    SUM(tn.federal_action_obligation)   AS total_obligation,
                    SUM(tn.original_loan_subsidy_cost)  AS total_subsidy_cost,
                    SUM(tn.funding_amount)              AS total_funding_amount,
                    SUM(tn.face_value_loan_guarantee)   AS total_loan_value,
                    SUM(tn.non_federal_funding_amount)  AS non_federal_funding_amount,
                    SUM(tn.indirect_federal_sharing)    AS total_indirect_federal_sharing,
                    -- Transaction FPDS Fields
                    SUM(CAST(fpds.base_and_all_options_value AS NUMERIC(23, 2))) AS base_and_all_options_value,
                    SUM(CAST(fpds.base_exercised_options_val AS NUMERIC(23, 2))) AS base_exercised_options_val,
                    COUNT(tn.transaction_unique_id) AS transaction_count
                FROM int.transaction_normalized AS tn
                LEFT JOIN int.transaction_fpds AS fpds ON tn.transaction_unique_id = fpds.detached_award_proc_unique
                GROUP BY tn.unique_award_key
            )
            SELECT
                latest.unique_award_key,
                0 AS subaward_count,  -- for consistency with Postgres table
                {", ".join([col_name for col_name in AWARDS_COLUMNS if col_name not in subquery_ignored_columns])}
            FROM transaction_latest AS latest
            INNER JOIN transaction_earliest AS earliest ON latest.unique_award_key = earliest.unique_award_key
            INNER JOIN transaction_totals AS totals on latest.unique_award_key = totals.unique_award_key
            -- Not every award will have a record in transaction_ec, so need to do a LEFT JOIN on it.
            LEFT JOIN transaction_ec AS ec ON latest.unique_award_key = ec.unique_award_key
        """

        # On set, create_date will not be changed and update_date will be set below.  The subaward columns will not
        # be changed, and id is used to match.  All other column values will come from the subquery.
        set_cols = [
            f"int.awards.{col_name} = source_subquery.{col_name}"
            for col_name in AWARDS_COLUMNS
            if col_name not in set_insert_special_columns
        ]
        set_cols.append(f"""int.awards.update_date = '{load_datetime.isoformat(" ")}'""")

        # Move insert_special_columns to the end of the list of column names for ease of handling
        # during record insert
        insert_col_name_list = [col_name for col_name in AWARDS_COLUMNS if col_name not in set_insert_special_columns]
        insert_col_name_list.extend(set_insert_special_columns)
        insert_col_names = ", ".join([col_name for col_name in insert_col_name_list])

        # On insert, all values except for those in insert_special_columns will come from the subquery
        insert_value_list = insert_col_name_list[: -len(set_insert_special_columns)]
        insert_value_list.extend(["NULL"])
        insert_value_list.extend([f"""'{load_datetime.isoformat(" ")}'"""] * 2)
        insert_values = ", ".join([value for value in insert_value_list])

        sql = f"""
            MERGE INTO int.awards
            USING (
                {subquery}
            ) AS source_subquery
            ON awards.generated_unique_award_id = source_subquery.unique_award_key
            WHEN MATCHED
                THEN UPDATE SET
                {", ".join(set_cols)}
            WHEN NOT MATCHED
                THEN INSERT
                    ({insert_col_names})
                    VALUES ({insert_values})
        """
        self.spark.sql(sql)
