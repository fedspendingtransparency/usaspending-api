"""
One-time command to backfill historical FABS action types to new GSDM 1.2 codes.

This command maps old action type codes (A, B, C, D, E, 0, NULL) to new action type codes
(A1, A2, B1, C1-C4, D1, E1, EX, FX, G1) based on the GSDM 1.2 changes.

The mapping is as follows:
- A, 0, NULL → A1 (New Award)
- B → B1 (Continuation)
- C → EX (Non-Financial) if federal_action_obligation = 0 OR original_loan_subsidy_cost = 0
- C → FX (Financial) otherwise
- D → FX (Other Action, Financial)
- E → G1 (Mixed Aggregate)

This updates:
1. transaction_search (Postgres) - Search/API table (FABS records where is_fpds = FALSE)
2. source_assistance_transaction (Postgres) - Raw copy of Broker's published_fabs
3. raw.published_fabs (Delta) - Bronze layer source data
4. int.transaction_fabs (Delta) - Silver layer FABS transactions
5. int.transaction_normalized (Delta) - Silver layer unified transactions
6. rpt.transaction_search (Delta, if exists) - Gold layer reporting table
7. transaction OpenSearch Index (via reindexing after backfill)

Note:
- The vw_transaction_fabs view is derived from transaction_search, so updating
  transaction_search automatically updates the view.
- rpt.transaction_search is optional and may not exist in all environments.
"""

import logging

from django.core.management.base import BaseCommand, CommandParser
from django.db import connection
from pyspark.sql import SparkSession

from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Backfill historical FABS action types to new GSDM 1.2 codes"

    spark: SparkSession

    # Reusable CASE statements for action type mapping
    ACTION_TYPE_MAPPING = """
        CASE
            WHEN action_type IN ('0', '') OR action_type IS NULL THEN 'A1'
            WHEN action_type = 'A' THEN 'A1'
            WHEN action_type = 'B' THEN 'B1'
            WHEN action_type = 'C' AND (
                COALESCE(federal_action_obligation, 0) = 0 OR COALESCE(original_loan_subsidy_cost, 0) = 0
            ) THEN 'EX'
            WHEN action_type = 'C' THEN 'FX'
            WHEN action_type = 'D' THEN 'FX'
            WHEN action_type = 'E' THEN 'G1'
            ELSE action_type
        END
    """

    ACTION_TYPE_DESCRIPTION_MAPPING = """
        CASE
            WHEN action_type IN ('0', '') OR action_type IS NULL THEN 'New Award'
            WHEN action_type = 'A' THEN 'New Award'
            WHEN action_type = 'B' THEN 'Continuation'
            WHEN action_type = 'C' AND (
            COALESCE(federal_action_obligation, 0) = 0 OR COALESCE(original_loan_subsidy_cost, 0) = 0
            ) THEN 'Other Action, Non-Financial'
            WHEN action_type = 'C' THEN 'Other Action, Financial'
            WHEN action_type = 'D' THEN 'Other Action, Financial'
            WHEN action_type = 'E' THEN 'Mixed Aggregate'
            WHEN action_type = 'A1' THEN 'New Award'
            WHEN action_type = 'A2' THEN 'Renewal Award'
            WHEN action_type = 'B1' THEN 'Continuation'
            WHEN action_type = 'C1' THEN 'Termination Initiated: Material Failure to Comply'
            WHEN action_type = 'C2' THEN 'Termination Initiated: Mutual Consent'
            WHEN action_type = 'C3' THEN 'Termination Initiated: Recipient-Initiated'
            WHEN action_type = 'C4' THEN (
                'Termination Initiated: No Longer Effectuates Program Goals or Agency Priorities'
            )
            WHEN action_type = 'D1' THEN 'Closeout'
            WHEN action_type = 'E1' THEN 'Recipient Change'
            WHEN action_type = 'EX' THEN 'Other Action, Non-Financial'
            WHEN action_type = 'FX' THEN 'Other Action, Financial'
            WHEN action_type = 'G1' THEN 'Mixed Aggregate'
            ELSE action_type_description
        END
    """

    # WHERE clause for identifying records to update
    OLD_ACTION_TYPE_WHERE = """
        action_type IN ('A', 'B', 'C', 'D', 'E', '0', '')
        OR action_type IS NULL
    """

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be updated without making changes",
        )
        parser.add_argument(
            "--postgres-only",
            action="store_true",
            help="Only update Postgres tables (transaction_search, source_assistance_transaction)",
        )
        parser.add_argument(
            "--delta-only",
            action="store_true",
            help="""
                Only update Delta tables (raw.published_fabs, int.transaction_fabs, 
                int.transaction_normalized, rpt.transaction_search)
            """,
        )
        parser.add_argument(
            "--spark-master",
            type=str,
            default=None,
            help="Spark master URL (e.g., spark://spark-master:7077). If not provided, uses local mode.",
        )

    def handle(self, *args, **options) -> None:
        dry_run = options["dry_run"]
        postgres_only = options["postgres_only"]
        delta_only = options["delta_only"]
        spark_master = options.get("spark_master")

        if postgres_only and delta_only:
            raise ValueError("Cannot specify both --postgres-only and --delta-only")

        logger.info("Starting FABS action type backfill...")

        if dry_run:
            logger.info("[DRY RUN MODE - No changes will be made]")

        # Update Postgres tables.
        if not delta_only:
            logger.info("Updating Postgres tables...")
            self.update_postgres_tables(dry_run)

        # Update Delta tables.
        if not postgres_only:
            logger.info("Updating Delta tables...")
            if spark_master:
                logger.info(f"Connecting to Spark cluster at: {spark_master}")
            self.update_delta_tables(dry_run, spark_master=spark_master)

        logger.info("FABS action type backfill complete!")

    def _generate_update_sql(self, table_name: str, additional_where: str = "") -> str:
        """Generate UPDATE SQL for action type mapping.

        Args:
            table_name: Name of the table to update
            additional_where: Additional WHERE clause conditions (optional)

        Returns:
            Complete UPDATE SQL statement
        """
        where_clause = self.OLD_ACTION_TYPE_WHERE
        if additional_where:
            where_clause = f"{additional_where} AND ({where_clause})"

        return f"""
            UPDATE {table_name}
            SET 
                action_type = {self.ACTION_TYPE_MAPPING},
                action_type_description = {self.ACTION_TYPE_DESCRIPTION_MAPPING}
            WHERE {where_clause}
        """

    def _generate_count_sql(self, table_name: str, additional_where: str = "") -> str:
        """Generate COUNT SQL for records that will be updated.

        Args:
            table_name: Name of the table to count
            additional_where: Additional WHERE clause conditions (optional)

        Returns:
            Complete COUNT SQL statement
        """
        where_clause = self.OLD_ACTION_TYPE_WHERE
        if additional_where:
            where_clause = f"{additional_where} AND ({where_clause})"

        return f"""
            SELECT COUNT(*) 
            FROM {table_name} 
            WHERE {where_clause}
        """

    def update_postgres_tables(self, dry_run: bool) -> None:
        """Update action_type and action_type_description in Postgres tables.

        Note: In modern setups, FABS data primarily lives in Delta Lake. This method
        updates the Postgres transaction_search table for FABS records (is_fpds = FALSE).
        The vw_transaction_fabs view is derived from transaction_search, so changes will
        automatically be visible through the view.

        Also updates source_assistance_transaction, which is a 100% duplicate copy of
        published_fabs records pulled from Broker, and is the Postgres source table that
        raw.published_fabs (Delta) is loaded from.
        """

        # Update transaction_search directly for FABS records
        update_transaction_search_sql = self._generate_update_sql(
            "transaction_search", additional_where="is_fpds = FALSE"
        )

        with connection.cursor() as cursor:
            # Update transaction_search for FABS records (is_fpds = FALSE).
            logger.info("Updating transaction_search (Postgres FABS records)...")
            if dry_run:
                count_sql = self._generate_count_sql("transaction_search", additional_where="is_fpds = FALSE")
                cursor.execute(count_sql)
                count = cursor.fetchone()[0]
                logger.info(f"[DRY RUN] Would update {count} records in transaction_search")
            else:
                cursor.execute(update_transaction_search_sql)
                logger.info(f"Updated {cursor.rowcount} records in transaction_search")

            # Update source_assistance_transaction (the raw Broker-sourced copy of published_fabs).
            logger.info("Updating source_assistance_transaction (Postgres)...")
            if dry_run:
                count_sql = self._generate_count_sql("source_assistance_transaction")
                cursor.execute(count_sql)
                count = cursor.fetchone()[0]
                logger.info(f"[DRY RUN] Would update {count} records in source_assistance_transaction")
            else:
                cursor.execute(self._generate_update_sql("source_assistance_transaction"))
                logger.info(f"Updated {cursor.rowcount} records in source_assistance_transaction")

    def update_delta_tables(self, dry_run: bool, spark_master: str = None) -> None:
        """Update action_type and action_type_description in Delta tables.

        Args:
            dry_run: If True, only count records without making changes
            spark_master: Spark master URL (e.g., spark://spark-master:7077).
                         If None, runs in local mode (requires Java).
        """

        # Initialize Spark session.
        self.spark = get_active_spark_session()
        spark_created_by_command = False
        if not self.spark:
            spark_created_by_command = True
            extra_conf = {
                # Config for Delta Lake tables and SQL. Need these to keep Delta table metadata in the metastore
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                # See comment below about old date and time values cannot be parsed without these
                "spark.sql.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
                "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
                "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
            }
            # Pass master parameter to configure_spark_session
            self.spark = configure_spark_session(**extra_conf, master=spark_master, spark_context=self.spark)

        try:
            # Update raw.published_fabs.
            logger.info("Updating raw.published_fabs...")
            self._update_published_fabs(dry_run)

            # Update int.transaction_fabs.
            logger.info("Updating int.transaction_fabs...")
            self._update_transaction_fabs_delta(dry_run)

            # Update int.transaction_normalized.
            logger.info("Updating int.transaction_normalized...")
            self._update_transaction_normalized_delta(dry_run)

            # Update transaction_search in Delta.
            logger.info("Updating transaction_search (Delta)...")
            self._update_transaction_search_delta(dry_run)

        finally:
            if spark_created_by_command:
                self.spark.stop()

        logger.info(
            "Note: OpenSearch index should be reindexed using elasticsearch_indexer command after this backfill"
        )

    def _update_published_fabs(self, dry_run: bool) -> None:
        """Update raw.published_fabs Delta table."""
        table_name = "raw.published_fabs"

        if dry_run:
            count = self.spark.sql(self._generate_count_sql(table_name)).collect()[0][0]
            logger.info(f"[DRY RUN] Would update {count} records in {table_name}")
        else:
            self.spark.sql(self._generate_update_sql(table_name))
            logger.info(f"Updated {table_name}")

    def _update_transaction_fabs_delta(self, dry_run: bool) -> None:
        """Update int.transaction_fabs Delta table."""
        table_name = "int.transaction_fabs"

        if dry_run:
            count = self.spark.sql(self._generate_count_sql(table_name)).collect()[0][0]
            logger.info(f"[DRY RUN] Would update {count} records in {table_name}")
        else:
            self.spark.sql(self._generate_update_sql(table_name))
            logger.info(f"Updated {table_name}")

    def _update_transaction_normalized_delta(self, dry_run: bool) -> None:
        """Update int.transaction_normalized Delta table for FABS records."""
        table_name = "int.transaction_normalized"
        additional_where = "is_fpds = FALSE"

        if dry_run:
            count = self.spark.sql(self._generate_count_sql(table_name, additional_where)).collect()[0][0]
            logger.info(f"[DRY RUN] Would update {count} records in {table_name}")
        else:
            self.spark.sql(self._generate_update_sql(table_name, additional_where))
            logger.info(f"Updated {table_name}")

    def _update_transaction_search_delta(self, dry_run: bool) -> None:
        """Update transaction_search Delta table for FABS records.

        Note: Unlike the core transaction tables (raw.published_fabs, int.transaction_fabs,
        int.transaction_normalized), which are created by the 'load_transactions_in_delta'
        command, rpt.transaction_search is a reporting table created by the separate
        'load_query_to_delta' command. It may not exist in development or test environments
        that only run the transaction pipeline. This check ensures graceful handling of such
        environments - the core data will still be backfilled and reporting tables can be
        rebuilt later from the updated source data.
        """
        # Check if table exists in Delta (it's in the rpt schema, not int).
        try:
            self.spark.sql("DESCRIBE TABLE rpt.transaction_search")
            table_exists = True
        except Exception:
            logger.warning("rpt.transaction_search table does not exist in Delta, skipping...")
            table_exists = False

        if not table_exists:
            return

        table_name = "rpt.transaction_search"
        additional_where = "is_fpds = FALSE"

        if dry_run:
            count = self.spark.sql(self._generate_count_sql(table_name, additional_where)).collect()[0][0]
            logger.info(f"[DRY RUN] Would update {count} records in {table_name}")
        else:
            self.spark.sql(self._generate_update_sql(table_name, additional_where))
            logger.info(f"Updated {table_name}")
