import argparse
import concurrent.futures
import logging
from collections import namedtuple
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Literal, get_args

from delta import DeltaTable
from distutils.util import strtobool
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from psycopg.sql import SQL
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.etl.postgres import ETLQueryFile, ETLTable, mixins
from usaspending_api.common.helpers.sql_helpers import execute_sql, get_connection
from usaspending_api.common.helpers.text_helpers import (
    standardize_nullable_whitespace as prep,
)
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer
from usaspending_api.common.spark.utils import prepare_spark
from usaspending_api.etl.operations.federal_account.update_agency import (
    DOD_SUBSUMED_AIDS,
    update_federal_account_agency,
)
from usaspending_api.etl.operations.treasury_appropriation_account.update_agencies import (
    update_treasury_appropriation_account_agencies,
)

logger = logging.getLogger("script")

TEMP_TABLE_NAME = "temp_load_agencies_raw_agency"

Agency = namedtuple(
    "Agency",
    [
        "row_number",
        "cgac_agency_code",
        "agency_name",
        "agency_abbreviation",
        "frec",
        "frec_entity_description",
        "frec_abbreviation",
        "subtier_code",
        "subtier_name",
        "subtier_abbreviation",
        "toptier_flag",
        "is_frec",
        "frec_cgac_association",
        "user_selectable",
        "mission",
        "about_agency_data",
        "website",
        "congressional_justification",
        "icon_filename",
    ],
)

MAX_CHANGES = 200


@dataclass
class SourceTableForUpdates:
    name: str
    unique_id: str


PostgresRawFabsAndFpdsTableNames = Literal["raw.source_assistance_transaction", "raw.source_procurement_transaction"]
DeltaFabsAndFpdsTableNames = Literal[
    "raw.published_fabs", "raw.detached_award_procurement", "int.transaction_fabs", "int.transaction_fpds"
]
DeltaAwardAndTransactionNormalizedTableNames = Literal["int.awards", "int.transaction_normalized"]


class Command(mixins.ETLMixin, BaseCommand):
    help = (
        "Loads CGACs, FRECs, Subtier Agencies, Toptier Agencies, and Agencies.  Load is all or nothing.  "
        "If anything fails, nothing gets saved."
    )

    agency_file = None
    force = False
    file_d_dry_run = False

    etl_logger_function = logger.info
    etl_dml_sql_directory = Path(__file__).resolve().parent / "load_agencies_sql"
    etl_timer = Timer

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "agency_file",
            metavar="AGENCY_FILE",
            help="Path (for local files) or URI (for http(s) or S3 files) of the raw agency CSV file to be loaded.",
        )

        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                f"Reloads agencies even if the max change threshold of {MAX_CHANGES:,} is exceeded.  This is a "
                f"safety precaution to prevent accidentally updating every award, transaction, and subaward in "
                f"the system as part of the nightly pipeline.  Will also force foreign key table links to be "
                f"examined even if it appears there were no agency changes."
            ),
        )

        parser.add_argument(
            "--file-d-dry-run",
            action="store_true",
            help=(
                "Performs all agency updates and then captures only a count of the Award and Transaction updates. "
                "A subsequent run using '--force' will process File D updates regardless of agency update count."
            ),
        )

    def handle(self, *args, **options) -> None:
        self.agency_file = options["agency_file"]
        self.force = options["force"]
        self.file_d_dry_run = options["file_d_dry_run"]

        logger.info(f"AGENCY FILE: {self.agency_file}")
        logger.info(f"FORCE SWITCH: {self.force}")
        logger.info(f"MAX CHANGE LIMIT: {'unlimited' if self.force else f'{MAX_CHANGES:,}'}")

        with Timer("Load agencies"):
            rows_affected = 0
            try:
                with transaction.atomic():
                    rows_affected = self._perform_load()
                    t = Timer("Commit agency transaction")
                    t.log_starting_message()
                t.log_success_message()
            except Exception:
                logger.error("ALL AGENCY CHANGES ROLLED BACK DUE TO EXCEPTION")
                raise

            if rows_affected > 0 or self.force:
                if rows_affected > MAX_CHANGES and not self.force:
                    raise RuntimeError(
                        f"Exceeded maximum number of allowed changes ({MAX_CHANGES:,}).  Use --force switch if this "
                        f"was intentional."
                    )
                try:
                    with (
                        # Need to make sure we generate the tables for handling updates outside a transaction
                        self.prepare_tables_with_updates(transaction_type="fabs") as fabs_source_update_table,
                        self.prepare_tables_with_updates(transaction_type="fpds") as fpds_source_update_table,
                        # Capture updates to the source tables in Postgres inside a transaction
                        transaction.atomic(),
                    ):
                        self.update_awards_and_transactions(fabs_source_update_table, fpds_source_update_table)
                except Exception:
                    logger.error("CHANGES TO SOURCE DATA IN POSTGRES ROLLED BACK DUE TO EXCEPTION")
                    raise
            else:
                logger.info("Skipping Award and Transaction updates since there were no agency changes.")

            try:
                self._vacuum_tables()
            except Exception:
                logger.exception("CHANGES WERE SUCCESSFULLY COMMITTED EVEN THOUGH VACUUMS FAILED")
                raise

    def _read_raw_agencies_csv(self) -> int:
        agencies = read_csv_file_as_list_of_dictionaries(self.agency_file)
        if len(agencies) < 1:
            raise RuntimeError(f"Agency file '{self.agency_file}' appears to be empty")

        self.agencies = [
            Agency(
                row_number=row_number,
                cgac_agency_code=prep(agency["CGAC AGENCY CODE"]),
                agency_name=prep(agency["AGENCY NAME"]),
                agency_abbreviation=prep(agency["AGENCY ABBREVIATION"]),
                frec=prep(agency["FREC"]),
                frec_entity_description=prep(agency["FREC Entity Description"]),
                frec_abbreviation=prep(agency["FREC ABBREVIATION"]),
                subtier_code=prep(agency["SUBTIER CODE"]),
                subtier_name=prep(agency["SUBTIER NAME"]),
                subtier_abbreviation=prep(agency["SUBTIER ABBREVIATION"]),
                toptier_flag=bool(strtobool(prep(agency["TOPTIER_FLAG"]))),
                is_frec=bool(strtobool(prep(agency["IS_FREC"]))),
                frec_cgac_association=bool(strtobool(prep(agency["FREC CGAC ASSOCIATION"]))),
                user_selectable=bool(strtobool(prep(agency["USER SELECTABLE ON USASPENDING.GOV"]))),
                mission=prep(agency["MISSION"]),
                about_agency_data=prep(agency["ABOUT AGENCY DATA"]),
                website=prep(agency["WEBSITE"]),
                congressional_justification=prep(agency["CONGRESSIONAL JUSTIFICATION"]),
                icon_filename=prep(agency["ICON FILENAME"]),
            )
            for row_number, agency in enumerate(agencies, start=1)
        ]

        return len(self.agencies)

    def _perform_validations(self) -> None:
        sql = (Path(self.etl_dml_sql_directory) / "validations.sql").read_text().format(temp_table=TEMP_TABLE_NAME)
        messages = [result[0] for result in execute_sql(sql, read_only=False)]

        if messages:
            m = "\n".join(messages)
            raise RuntimeError(f"The following {len(messages):,} problem(s) have been found with the agency file:\n{m}")

    def _import_raw_agencies(self) -> int:
        sql = (Path(self.etl_dml_sql_directory) / "insert_into.sql").read_text().format(temp_table=TEMP_TABLE_NAME)
        with get_connection(read_only=False).cursor() as cursor:
            cursor.executemany(sql, self.agencies)
            return cursor.rowcount

    def _perform_load(self) -> int:
        overrides = {
            "insert_overrides": {"create_date": SQL("now()"), "update_date": SQL("now()")},
            "update_overrides": {"update_date": SQL("now()")},
        }

        agency_table = ETLTable("agency", key_overrides=["toptier_agency_id", "subtier_agency_id"], **overrides)
        cgac_table = ETLTable("cgac", key_overrides=["cgac_code"])
        frec_table = ETLTable("frec", key_overrides=["frec_code"])
        subtier_agency_table = ETLTable("subtier_agency", key_overrides=["subtier_code"], **overrides)
        toptier_agency_table = ETLTable("toptier_agency", key_overrides=["toptier_code"], **overrides)

        agency_query = ETLQueryFile(self.etl_dml_sql_directory / "agency_query.sql", temp_table=TEMP_TABLE_NAME)
        cgac_query = ETLQueryFile(self.etl_dml_sql_directory / "cgac_query.sql", temp_table=TEMP_TABLE_NAME)
        frec_query = ETLQueryFile(self.etl_dml_sql_directory / "frec_query.sql", temp_table=TEMP_TABLE_NAME)
        subtier_agency_query = ETLQueryFile(
            self.etl_dml_sql_directory / "subtier_agency_query.sql", temp_table=TEMP_TABLE_NAME
        )
        toptier_agency_query = ETLQueryFile(
            self.etl_dml_sql_directory / "toptier_agency_query.sql",
            temp_table=TEMP_TABLE_NAME,
            dod_subsumed=DOD_SUBSUMED_AIDS,
        )

        path = self._get_sql_directory_file_path("raw_agency_create_temp_table")
        sql = path.read_text().format(temp_table=TEMP_TABLE_NAME)
        self._execute_dml_sql(sql, "Create raw agency temp table")
        self._execute_function_and_log(self._read_raw_agencies_csv, "Read raw agencies csv")
        self._execute_function_and_log(self._import_raw_agencies, "Import raw agencies")
        self._execute_function(self._perform_validations, "Perform validations")

        rows_affected = 0

        rows_affected += self._delete_update_insert_rows("CGACs", cgac_query, cgac_table)
        rows_affected += self._delete_update_insert_rows("FRECs", frec_query, frec_table)

        rows_affected += self._delete_update_insert_rows("toptier agencies", toptier_agency_query, toptier_agency_table)
        rows_affected += self._delete_update_insert_rows("subtier agencies", subtier_agency_query, subtier_agency_table)
        rows_affected += self._delete_update_insert_rows("agencies", agency_query, agency_table)

        if rows_affected > MAX_CHANGES and not self.force:
            raise RuntimeError(
                f"Exceeded maximum number of allowed changes ({MAX_CHANGES:,}).  Use --force switch if this "
                f"was intentional."
            )

        elif rows_affected > 0 or self.force:
            self._execute_function_and_log(
                update_treasury_appropriation_account_agencies, "Update treasury appropriation accounts"
            )
            self._execute_function_and_log(update_federal_account_agency, "Update federal accounts")
        else:
            logger.info(
                "Skipping treasury_appropriation_account and federal_account updates "
                "since there were no agency changes."
            )

        return rows_affected

    def _vacuum_tables(self) -> None:
        self._execute_dml_sql("vacuum (full, analyze) agency", "Vacuum agency table")
        self._execute_dml_sql("vacuum (full, analyze) cgac", "Vacuum cgac table")
        self._execute_dml_sql("vacuum (full, analyze) frec", "Vacuum frec table")
        self._execute_dml_sql("vacuum (full, analyze) subtier_agency", "Vacuum subtier_agency table")
        self._execute_dml_sql("vacuum (full, analyze) toptier_agency", "Vacuum toptier_agency table")

    def update_awards_and_transactions(
        self, fabs_update_table: SourceTableForUpdates, fpds_update_table: SourceTableForUpdates
    ) -> None:
        logger.info("Starting to update award and transaction tables")

        with prepare_spark(
            create_temp_views=True, extra_table_names=[fabs_update_table.name, fpds_update_table.name]
        ) as spark:
            # Make sure the Postgres tables are updated in case we need to re-generate the Transactions. This handles
            # the case of agency codes being "999".
            with (
                concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor_for_postgres,
                concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor_for_delta,
            ):
                postgres_raw_fabs_table: PostgresRawFabsAndFpdsTableNames = "raw.source_assistance_transaction"
                postgres_raw_fpds_table: PostgresRawFabsAndFpdsTableNames = "raw.source_procurement_transaction"
                postgres_update_futures = {
                    executor_for_postgres.submit(
                        self.update_postgres_table, target_table_name, source_update_table
                    ): target_table_name
                    for target_table_name, source_update_table in [
                        (postgres_raw_fabs_table, fabs_update_table),
                        (postgres_raw_fpds_table, fpds_update_table),
                    ]
                }

                delta_raw_fabs_table: DeltaFabsAndFpdsTableNames = "raw.published_fabs"
                delta_raw_fpds_table: DeltaFabsAndFpdsTableNames = "raw.detached_award_procurement"
                delta_int_fabs_table: DeltaFabsAndFpdsTableNames = "int.transaction_fabs"
                delta_int_fpds_table: DeltaFabsAndFpdsTableNames = "int.transaction_fpds"
                delta_update_futures = {
                    executor_for_delta.submit(
                        self.update_delta_fabs_and_fpds_tables, spark, target_table_name, source_update_table
                    ): target_table_name
                    for target_table_name, source_update_table in [
                        (delta_raw_fabs_table, fabs_update_table),
                        (delta_raw_fpds_table, fpds_update_table),
                        (delta_int_fabs_table, fabs_update_table),
                        (delta_int_fpds_table, fpds_update_table),
                    ]
                }

                # Wait for first set of Delta futures to complete
                for future in concurrent.futures.as_completed(delta_update_futures):
                    try:
                        # We don't use the result, but want to make sure this didn't run into an exception
                        future.result()
                    except Exception:
                        logger.exception(f"Error occurred when processing table: {delta_update_futures[future]}")
                        raise

                delta_update_futures = {
                    executor_for_delta.submit(
                        self.update_delta_transaction_normalized_and_awards_table,
                        spark,
                        target_table_name,
                    ): target_table_name
                    for target_table_name in get_args(DeltaAwardAndTransactionNormalizedTableNames)
                }

                # Wait for Postgres and Delta futures to complete
                for future in concurrent.futures.as_completed([*postgres_update_futures, *delta_update_futures]):
                    try:
                        # We don't use the result, but want to make sure this didn't run into an exception
                        future.result()
                    except Exception:
                        table_name = postgres_update_futures.get(future) or delta_update_futures[future]
                        logger.exception(f"Error occurred when processing table: {table_name}")
                        raise

        logger.info("Finished updating award and transaction tables")

    @contextmanager
    def prepare_tables_with_updates(
        self, transaction_type: Literal["fabs", "fpds"]
    ) -> Generator[SourceTableForUpdates, None, None]:
        broker_table_name = "published_fabs" if transaction_type == "fabs" else "detached_award_procurement"
        broker_table_id = f"{broker_table_name}_id"
        update_table_name = f"{transaction_type}_agencies_to_update"
        create_table_sql = f"""
            -- Drop the table in case it happens to still exist
            DROP TABLE IF EXISTS {update_table_name};
            -- Generate the table for capturing records that may need updating
            CREATE TABLE {update_table_name} AS
            WITH transactions_to_update AS (
                SELECT {broker_table_id}
                FROM rpt.transaction_search
                WHERE
                    is_fpds = {"TRUE" if transaction_type == "fpds" else "FALSE"}
                    AND (awarding_agency_code = '999' OR funding_agency_code = '999')
            )
            SELECT *
            FROM dblink(
                '{settings.BROKER_DBLINK_NAME}',
                '
                    SELECT
                        {broker_table_id},
                        UPPER(awarding_agency_code) AS awarding_agency_code,
                        UPPER(awarding_agency_name) AS awarding_agency_name,
                        UPPER(funding_agency_code) AS funding_agency_code,
                        UPPER(funding_agency_name) AS funding_agency_name
                    FROM {broker_table_name}
                '
            ) AS broker_data(
                {broker_table_id} integer,
                awarding_agency_code text,
                awarding_agency_name text,
                funding_agency_code text,
                funding_agency_name text
            )
            WHERE
                EXISTS(
                    SELECT 1
                    FROM transactions_to_update
                    WHERE broker_data.{broker_table_id} = transactions_to_update.{broker_table_id}
                );
            -- Create index on to assist updates using this data; uses UUID to ensure uniqueness
            CREATE INDEX {update_table_name}_index ON {update_table_name}({broker_table_id});
        """
        try:
            self._execute_dml_sql(create_table_sql, f"Create table for updating {transaction_type} transactions")
            yield SourceTableForUpdates(name=update_table_name, unique_id=broker_table_id)
        finally:
            # Make sure the table created for updates is removed regardless of success or failure
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {update_table_name};")

    def update_postgres_table(
        self,
        target_table_name: PostgresRawFabsAndFpdsTableNames,
        source_table: SourceTableForUpdates,
    ) -> None:
        if self.file_d_dry_run:
            sql = f"""
                SELECT COUNT(*)
                FROM {target_table_name} AS t
                WHERE EXISTS (
                    SELECT 1
                    FROM {source_table.name} AS s
                    WHERE t.{source_table.unique_id} = s.{source_table.unique_id}
                )
            """
            logger.info(f"Getting count of record(s) that would be updated in {target_table_name}")
            with connection.cursor() as cursor:
                cursor.execute(sql)
                record_count = cursor.fetchone()
            logger.info(f"{record_count[0]:,} record(s) would be updated in {target_table_name}")
        else:
            sql = f"""
                UPDATE {target_table_name} AS t
                SET
                    awarding_agency_code = s.awarding_agency_code,
                    awarding_agency_name = s.awarding_agency_name,
                    funding_agency_code = s.funding_agency_code,
                    funding_agency_name = s.funding_agency_name
                FROM {source_table.name} AS s
                WHERE t.{source_table.unique_id} = s.{source_table.unique_id}
            """
            self._execute_dml_sql(sql, f"Update {target_table_name} in Postgres")

    def update_delta_fabs_and_fpds_tables(
        self,
        spark: SparkSession,
        target_table_name: Literal[
            "raw.published_fabs", "raw.detached_award_procurement", "int.transaction_fabs", "int.transaction_fpds"
        ],
        source_table: SourceTableForUpdates,
    ) -> None:
        target = DeltaTable.forName(spark, target_table_name)
        source_df = spark.table(f"global_temp.{source_table.name}")

        if self.file_d_dry_run:
            logger.info(f"Getting count of record(s) that would be updated in {target_table_name}")
            target_df = target.toDF()
            join_df = target_df.join(source_df, on=source_table.unique_id, how="left_semi")
            logger.info(f"{join_df.count():,} record(s) would be updated in {target_table_name}")
        else:
            logger.info(f"Merging values into {target_table_name}")
            (
                target.alias("t")
                .merge(
                    source_df.alias("s"),
                    f"t.{source_table.unique_id} = s.{source_table.unique_id}",
                )
                .whenMatchedUpdate(
                    set={
                        "awarding_agency_code": "s.awarding_agency_code",
                        "awarding_agency_name": "s.awarding_agency_name",
                        "funding_agency_code": "s.funding_agency_code",
                        "funding_agency_name": "s.funding_agency_name",
                    }
                )
                .execute()
            )
            logger.info(f"Finished merging values into {target_table_name}")

    def update_delta_transaction_normalized_and_awards_table(
        self, spark: SparkSession, table_name: Literal["int.awards", "int.transaction_normalized"]
    ) -> None:
        target = DeltaTable.forName(spark, table_name).alias("t")

        agency_df = spark.table("global_temp.agency").select("id", "subtier_agency_id")
        subtier_agency_df = (
            spark.table("global_temp.subtier_agency")
            .select("subtier_agency_id", "subtier_code")
            .join(agency_df, on="subtier_agency_id", how="left")
        )

        awarding_agency_df = subtier_agency_df.alias("awarding")
        awarding_agency_df = awarding_agency_df.select(
            [sf.col(c).alias("awarding_" + c) for c in awarding_agency_df.columns]
        )

        funding_agency_df = subtier_agency_df.alias("funding")
        funding_agency_df = funding_agency_df.select(
            [sf.col(c).alias("funding_" + c) for c in funding_agency_df.columns]
        )

        cols = ["transaction_id", "awarding_sub_tier_agency_c", "funding_sub_tier_agency_co"]
        transaction_fabs_df = spark.table("int.transaction_fabs").select(*cols)
        transaction_fpds_df = spark.table("int.transaction_fpds").select(*cols)
        transaction_union_df = transaction_fabs_df.unionAll(transaction_fpds_df)

        source_df = transaction_union_df.join(
            awarding_agency_df,
            on=(transaction_union_df["awarding_sub_tier_agency_c"] == awarding_agency_df["awarding_subtier_code"]),
            how="left",
        ).join(
            funding_agency_df,
            on=(transaction_union_df["funding_sub_tier_agency_co"] == funding_agency_df["funding_subtier_code"]),
            how="left",
        )

        if table_name == "int.awards":
            # This is needed to make sure that OpenSearch documents are updated. They use the update_date on the Award
            # or the etl_update_date on the Transaction; the latter being a coalesce of the Award and Transaction
            # update dates.
            extra_update = {"update_date": f"'{datetime.now(timezone.utc).isoformat(' ')}'"}
            target_join_id = "latest_transaction_id"
        else:
            extra_update = {}
            target_join_id = "id"

        if self.file_d_dry_run:
            logging.info(f"Getting count of record(s) that would be updated in {table_name}")
            target_df = target.toDF()
            join_df = target_df.join(
                source_df,
                on=(
                    (target_df[target_join_id] == source_df["transaction_id"])
                    & (
                        (~target_df["awarding_agency_id"].eqNullSafe(source_df["awarding_id"]))
                        | (~target_df["funding_agency_id"].eqNullSafe(source_df["funding_id"]))
                    )
                ),
                how="left_semi",
            )
            logger.info(f"{join_df.count():,} record(s) would be updated in {table_name}")
        else:
            logger.info(f"Merging values into {table_name}")
            (
                target.merge(source_df.alias("s"), f"t.{target_join_id} = s.transaction_id")
                .whenMatchedUpdate(
                    condition=(
                        (~sf.col("t.awarding_agency_id").eqNullSafe(sf.col("s.awarding_id")))
                        | (~sf.col("t.funding_agency_id").eqNullSafe(sf.col("s.funding_id")))
                    ),
                    set={**extra_update, "awarding_agency_id": "s.awarding_id", "funding_agency_id": "s.funding_id"},
                )
                .execute()
            )
            logger.info(f"Finished merging values into {table_name}")
