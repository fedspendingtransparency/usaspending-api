import logging

from django.core.management import call_command
from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.awards.delta_models import c_to_d_linkage_view_sql_strings, c_to_d_linkage_drop_view_sql_strings
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
    get_jdbc_connection_properties,
    get_usas_jdbc_url,
)
from usaspending_api.config import CONFIG

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    help = (
        "This command links File C (financial_accounts_by_awards) records to File D (awards) records.",
        "It creates a new copy of the `financial_accounts_by_awards` Delta table in the `int` schema",
        "based on the copy in the `raw` schema and applies the updates there. These records are linked to",
        "the `awards` table in the `int` schema. By default, this copy is peformed as a SHALLOW CLONE,",
        "but if the `--no-clone` command is used, a full copy of the table will be made instead. In either",
        "case, the int table is replaced during each run.",
        "",
        "Before updating the int table, it builds a temporary view containing all necessary updates to",
        "linkages. At the end of the command, this view is written to Postgres replacing the contents of the",
        "contents of the `c_to_d_linkages` table, which is later used to perform the same updates on the",
        "Postgres version of the FABA table.",
    )

    spark: SparkSession

    def add_arguments(self, parser):

        parser.add_argument(
            "--no-clone",
            action="store_true",
            required=False,
            help="Whether to create a full new `int` table instead of a clone.",
        )

        parser.add_argument(
            "--spark-s3-bucket",
            type=str,
            required=False,
            default=CONFIG.SPARK_S3_BUCKET,
            help="The destination bucket in S3 to rewrite the FABA table when the --no-clone option is used.",
        )

    def get_unlinked_count(self, schema):
        unlinked_sql = f"""
        SELECT
            count(*)
        FROM
            {schema}.financial_accounts_by_awards
        WHERE
            award_id IS NULL;
        """

        results = self.spark.sql(unlinked_sql)
        return results.collect()[0][0]

    def handle(self, *args, **options):

        extra_conf = {
            # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # See comment below about old date and time values cannot parsed without these
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
            "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
        }

        no_clone = options["no_clone"]
        spark_s3_bucket = options["spark_s3_bucket"]

        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)  # type: SparkSession

        # Before we lose history after a clone or drop table of our int.financial_accounts_by_awards table
        #   we need to identify FABA records (file C submissions) that were deleted.
        #   This step is important because we need to identify which awards need to be unlinked
        #   from file C submissions that are now deleted.
        identify_faba_deletions_query = """
            CREATE OR REPLACE TEMPORARY VIEW identify_faba_deletions_query AS (
                SELECT DISTINCT faba.award_id
                    FROM int.financial_accounts_by_awards faba

                    LEFT JOIN raw.financial_accounts_by_awards rfaba
                    ON faba.financial_accounts_by_awards_id = rfaba.financial_accounts_by_awards_id

                    WHERE rfaba.financial_accounts_by_awards_id IS NULL
            );
        """
        # If this command is running for the first time in an environment or in our test
        #   suites, int.financial_accounts_by_awards isn't guarenteed to exist.
        int_faba_exists = self.spark._jsparkSession.catalog().tableExists("int.financial_accounts_by_awards")
        if int_faba_exists:
            self.spark.sql(identify_faba_deletions_query)
            # Update Awards whose file C submissions are now deleted
            delete_sql = """
                MERGE INTO int.awards AS aw
                USING identify_faba_deletions_query AS deletes
                ON aw.id = deletes.award_id
                WHEN MATCHED
                    THEN UPDATE SET aw.update_date = NOW();
            """
            self.spark.sql(delete_sql)

        # Clean up deletion view(s)
        self.spark.sql("DROP VIEW identify_faba_deletions_query;")

        # Setup int table. Creates a shallow clone of the `raw` FABA table in the `int` schema.
        # If the --no-clone option is provided a full table is created instead.
        if no_clone:
            self.spark.sql("DROP TABLE IF EXISTS int.financial_accounts_by_awards")
            call_command(
                "create_delta_table",
                "--destination-table",
                "financial_accounts_by_awards",
                "--spark-s3-bucket",
                spark_s3_bucket,
                "--alt-db",
                "int",
            )
            self.spark.sql(
                "INSERT OVERWRITE int.financial_accounts_by_awards SELECT * FROM raw.financial_accounts_by_awards;"
            )
        else:
            self.spark.sql(
                f"""
                CREATE OR REPLACE TABLE int.financial_accounts_by_awards
                SHALLOW CLONE raw.financial_accounts_by_awards
                LOCATION 's3a://{spark_s3_bucket}/{CONFIG.DELTA_LAKE_S3_PATH}/int/financial_accounts_by_awards';
            """
            )

        # Log initial linkage count
        unlinked_count = self.get_unlinked_count("int")
        logger.info(f"Count of unlinked records before updates: {unlinked_count:,}")

        # Run Linkage Queries
        linkage_queries = c_to_d_linkage_view_sql_strings
        for index, query in enumerate(linkage_queries):
            logger.info(f"Running query number: {index + 1}\nPreview of query: {query[:100]}")
            self.spark.sql(query)

        # Update to be linked to Award record's `update_date`
        update_sql = """
            MERGE INTO int.awards AS aw
            USING updated_awards AS updates
            ON aw.id = updates.award_id
            WHEN MATCHED
                THEN UPDATE SET aw.update_date = NOW();
        """

        # Log the number of Awards linked to and updated
        results = self.spark.sql(update_sql).collect()
        # Merge results do not contain counts outside of Databricks, so check the contents of results before logging
        if len(results) > 0:
            update_count = results[0][0]
            logger.info(f"{update_count:,} int.awards records were linked to and updated.")

        # Final MERGE of `union_all_priority` into `int.financial_accounts_by_awards`;
        # This merge will set the `award_id` field to the `id` of the linked `int.awards` record;
        # If any previously linked Awards have been deleted, the `award_id` will be set to NULL
        merge_sql = """
            MERGE INTO int.financial_accounts_by_awards AS faba
            USING union_all_priority AS updates
            ON faba.financial_accounts_by_awards_id = updates.financial_accounts_by_awards_id
            WHEN MATCHED
                THEN UPDATE SET faba.award_id = updates.award_id;
        """

        # Log the number of FABA records updated in the merge and final linkage count
        results = self.spark.sql(merge_sql).collect()
        # Merge results do not contain counts outside of Databricks, so check the contents of results before logging
        if len(results) > 0:
            update_count = results[0][0]
            logger.info(
                f"{update_count:,} int.financial_accounts_by_awards records were unlinked or linked to int.awards"
            )

        # Log the number of FABA records still unlinked
        unlinked_count = self.get_unlinked_count("int")
        logger.info(f"Count of unlinked records after updates: {unlinked_count:,}")

        # Write view back to Postgres for linkages
        c_to_d_linkage_updates_df = self.spark.sql("SELECT * FROM union_all_priority;")
        c_to_d_linkage_updates_df.write.jdbc(
            url=get_usas_jdbc_url(),
            table="public.c_to_d_linkage_updates",
            mode="overwrite",
            properties=get_jdbc_connection_properties(),
        )

        # Run Linkage Queries
        drop_view_queries = c_to_d_linkage_drop_view_sql_strings
        for index, query in enumerate(drop_view_queries):
            logger.info(f"Running query number: {index + 1}\nPreview of query: {query[:100]}")
            self.spark.sql(query)

        if spark_created_by_command:
            self.spark.stop()
