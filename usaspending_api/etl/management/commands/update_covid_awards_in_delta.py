import logging

from django.core.management.base import BaseCommand
from pyspark.sql import SparkSession

from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.spark_helpers import (
    configure_spark_session,
    get_active_spark_session,
)
from usaspending_api.etl.management.helpers.recent_periods import retrieve_recent_periods

logger = logging.getLogger(__name__)

DISTINCT_AWARD_SQL = """
    SELECT
        DISTINCT award_id
    FROM
        int.financial_accounts_by_awards faba
    INNER JOIN global_temp.submission_attributes sa ON
        faba.submission_id = sa.submission_id
    INNER JOIN global_temp.dabs_submission_window_schedule dabs ON
        dabs.id = sa.submission_window_id
    INNER JOIN global_temp.disaster_emergency_fund_code defc ON
        defc.code = faba.disaster_emergency_fund_code
        AND (defc.group_name = 'covid_19' or defc.group_name = 'infrastructure')
    WHERE
        (
            submission_fiscal_year = {last_months_year}
            AND submission_fiscal_month = {last_months_month}
            AND is_quarter = FALSE
        )
        OR (
            submission_fiscal_year = {last_quarters_year}
            AND submission_fiscal_month = {last_quarters_month}
            AND is_quarter = TRUE
        )
        OR (
            submission_fiscal_year = {this_months_year}
            AND submission_fiscal_month = {this_months_month}
            AND is_quarter = FALSE
        )
        OR (
            submission_fiscal_year = {this_quarters_year}
            AND submission_fiscal_month = {this_quarters_month}
            AND is_quarter = TRUE
        )
"""

CONDITION_SQL = """
    update_date < '{submission_reveal_date}'
"""

COUNT_OPERATION_SQL = """
    WITH recent_covid_awards AS (
        {distinct_award_sql}
    )
    SELECT COUNT(*)
    FROM int.awards
    WHERE
        {condition_sql}
        AND EXISTS (
            SELECT 1
            FROM recent_covid_awards
            WHERE awards.id = recent_covid_awards.award_id
        )
"""

UPDATE_OPERATION_SQL = """
    MERGE INTO int.awards USING ({distinct_award_sql}) AS recent_covid_awards
    ON
        {condition_sql}
        AND awards.id = recent_covid_awards.award_id
    WHEN MATCHED
        THEN UPDATE SET update_date = NOW()
"""


class Command(BaseCommand):
    help = (
        "This command updates the `update_date` field on the `int.awards` table in Delta Lake for",
        "awards that had Covid or Infrastructure data in the previous two submission periods when a",
        "new submission period is revealed. This is to signal to Elasticsearch that the award records",
        "should be reindexed.",
    )

    spark: SparkSession

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Only log the count of awards that would have been updated. No awards will be updated",
        )

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

        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)  # type: SparkSession

        create_ref_temp_views(self.spark)

        # Read arguments
        dry_run = options["dry_run"]

        # Retreive information about recent submission periods. (Last two quarters and last two months)
        periods = retrieve_recent_periods()

        # Use the dry_run option to determine whether to actually update awards or only determine the count of
        # awards that would be updated
        distinct_award_sql = DISTINCT_AWARD_SQL.format(
            last_months_year=periods["last_month"]["year"],
            last_months_month=periods["last_month"]["month"],
            last_quarters_year=periods["last_quarter"]["year"],
            last_quarters_month=periods["last_quarter"]["month"],
            this_months_year=periods["this_month"]["year"],
            this_months_month=periods["this_month"]["month"],
            this_quarters_year=periods["this_quarter"]["year"],
            this_quarters_month=periods["this_quarter"]["month"],
        )
        condition_sql = CONDITION_SQL.format(
            submission_reveal_date=periods["this_month"]["submission_reveal_date"],
        )
        operation_sql = UPDATE_OPERATION_SQL
        if dry_run:
            logger.info("Dry run flag provided. No records will be updated.")
            operation_sql = COUNT_OPERATION_SQL

        results = self.spark.sql(
            operation_sql.format(
                distinct_award_sql=distinct_award_sql,
                condition_sql=condition_sql,
            )
        )

        count = results.collect()[0][0]

        if dry_run:
            logger.info(
                f"There are {count:,} award records which should be reloaded into Elasticsearch for data consistency."
            )
        else:
            logger.info(
                f"{count:,} award records were updated and will be reloaded into Elasticsearch for data consistency."
            )

        if spark_created_by_command:
            self.spark.stop()
