import argparse
import logging

import duckdb
from django.core.management.base import BaseCommand

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer

logger = logging.getLogger("script")

VALIDATE_SQL = """
    WITH numbered AS (
        SELECT
            row_number() OVER () AS row_number,
            trim("DEFC"::VARCHAR) AS code,
            trim("Public Law"::VARCHAR) AS public_law
        FROM csv_source
    ),
    problems AS (
        SELECT
            CASE WHEN NOT regexp_full_match(code, '[a-zA-Z0-9]+')
                THEN format(
                    'Invalid DEF Code ''{}'' in row {}. DEF codes must be only one or two alphanumeric characters.',
                    code,
                    row_number
                )
            END AS code_error,
            CASE WHEN public_law = '' OR public_law IS NULL
                THEN format('DEF Code public law is required in row {}.', row_number)
            END AS law_error
        FROM numbered
    ),
    messages AS (
        SELECT unnest(list_filter([code_error, law_error], x -> x IS NOT NULL)) AS message
        FROM problems
    )
    SELECT count(*) AS problem_count, string_agg(message, chr(10)) AS messages
    FROM messages
"""

MERGE_SQL = """
    MERGE INTO pg.public.disaster_emergency_fund_code AS target
    USING csv_source AS source
    ON target.code = source."DEFC"
    WHEN MATCHED THEN UPDATE SET
        public_law = source."Public Law",
        title = NULLIF(
            trim(source."Public Law Short Title"),
            ''
        ),
        group_name = NULLIF(
            trim(source."Group Name"),
            ''
        ),
        urls = NULLIF(
            trim(source."URLs"),
            ''
        ),
        earliest_public_law_enactment_date = NULLIF(
            trim(source."Earliest Public Law Enactment Date"::VARCHAR),
            ''
        )::DATE,
        update_date = now()
    WHEN NOT MATCHED THEN INSERT (
        code, public_law, title, group_name, urls, earliest_public_law_enactment_date, create_date, update_date
    ) VALUES (
        source."DEFC",
        source."Public Law",
        NULLIF(trim(source."Public Law Short Title"), ''),
        NULLIF(trim(source."Group Name"), ''),
        NULLIF(trim(source."URLs"), ''),
        NULLIF(trim(source."Earliest Public Law Enactment Date"::VARCHAR), '')::DATE,
        now(),
        now()
    )
"""


class Command(BaseCommand):
    """Load DEF Code CSV file via DuckDB.  If anything fails, nothing gets saved."""

    help = __doc__

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--def-code-file",
            metavar="FILE",
            help="Path or URI of the raw DEF Code CSV file to be loaded.",
            default="https://files.usaspending.gov/reference_data/def_codes.csv",
        )

    def handle(self, *args, **options) -> None:
        def_code_file = options["def_code_file"]
        logger.info(f"Attempting to load from file: {def_code_file}")
        con = duckdb.connect()
        for statement in ("INSTALL httpfs", "LOAD httpfs", "INSTALL postgres", "LOAD postgres"):
            con.execute(statement)
        con.execute(f"ATTACH '{get_database_dsn_string()}' AS pg (TYPE postgres)")
        con.read_csv(def_code_file, all_varchar=True).create_view("csv_source")

        self._validate(con)

        with Timer("Load DEF Code"):
            try:
                con.execute(MERGE_SQL)
            except Exception:
                logger.error("ALL CHANGES ROLLED BACK DUE TO EXCEPTION")
                raise

    @staticmethod
    def _validate(con: duckdb.DuckDBPyConnection) -> None:
        (total_rows,) = con.sql("SELECT count(*) FROM csv_source").fetchone()
        if total_rows == 0:
            raise RuntimeError("DEF Code file appears to be empty")

        problem_count, messages = con.sql(VALIDATE_SQL).fetchone()
        if problem_count:
            logger.error(messages)
            raise RuntimeError(
                f"{problem_count:,} problem(s) have been found with the raw DEF Code file.  See log for details."
            )
