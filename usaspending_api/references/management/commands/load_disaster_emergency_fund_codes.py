import logging
import re
from collections import namedtuple
from datetime import datetime
from typing import Union

from django.core.management.base import BaseCommand
from django.db import transaction
from psycopg2.extras import execute_values
from psycopg2.sql import SQL

from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.etl.postgres import ETLTable, ETLTemporaryTable, mixins
from usaspending_api.common.etl.postgres.operations import insert_missing_rows, update_changed_rows
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer

DEF_CODE_PATTERN = re.compile("[a-zA-Z0-9]+")

CREATE_TEMP_TABLE = """
    drop table if exists temp_load_disaster_emergency_fund_codes;

    create temporary table temp_load_disaster_emergency_fund_codes (
        row_number int,
        code text,
        public_law text,
        title text,
        group_name text,
        urls text,
        earliest_public_law_enactment_date date
    );
"""

logger = logging.getLogger("script")

DisasterEmergencyFundCode = namedtuple(
    "DisasterEmergencyFundCode",
    ["row_number", "code", "public_law", "title", "group_name", "urls", "earliest_public_law_enactment_date"],
)


class Command(mixins.ETLMixin, BaseCommand):
    """Used to load DEF Codes either from URI or a local file."""

    help = "Load DEF Code CSV file.  If anything fails, nothing gets saved.  DOES NOT DELETE RECORDS."
    def_code_file = None
    etl_logger_function = logger.info

    def add_arguments(self, parser):
        parser.add_argument(
            "--def-code-file",
            metavar="FILE",
            help="Path or URI of the raw DEF Code CSV file to be loaded.",
            default="https://files.usaspending.gov/reference_data/def_codes.csv",
        )

    def handle(self, *args, **options):
        self.def_code_file = options["def_code_file"]
        logger.info(f"Attempting to load from file: {self.def_code_file}")

        with Timer("Load DEF Code"):
            try:
                with transaction.atomic():
                    self._perform_load()
                    t = Timer("Commit transaction")
                    t.log_starting_message()
                t.log_success_message()
            except Exception:
                logger.error("ALL CHANGES ROLLED BACK DUE TO EXCEPTION")
                raise

            try:
                self._vacuum_tables()
            except Exception:
                logger.error("CHANGES WERE SUCCESSFULLY COMMITTED EVEN THOUGH VACUUMS FAILED")
                raise

    @staticmethod
    def _prep(text):
        """
        A semi-common problem with CSV files that have been edited by third party tools is the
        introduction of leading and/or trailing spaces. Strip them.
        """
        if text and type(text) is str:
            return text.strip()
        return text

    @staticmethod
    def _prep_date(date_string: str) -> Union[datetime, None]:
        """Convert a string to a datetime object or None if no string was provided.
        This assumes the date is in the following format: 2023-12-25 (yyyy-mm-dd)

        Args:
            date_string (str): `Earliest Public Law Enactment Date` value from def_codes.csv file

        Returns:
            Union[datetime, None]: Return the `Earliest Public Law Enactment Date` converted to a datetime
                or None
        """
        if date_string and type(date_string) is str:
            return datetime.strptime(date_string, "%Y-%m-%d")
        else:
            return None

    def _read_raw_def_code_csv(self):
        raw_def_codes = read_csv_file_as_list_of_dictionaries(self.def_code_file)
        if len(raw_def_codes) < 1:
            raise RuntimeError(f"File '{self.def_code_file}' appears to be empty")
        self.def_codes = [
            DisasterEmergencyFundCode(
                row_number=row_number,
                code=self._prep(def_code["DEFC"]),
                public_law=self._prep(def_code["Public Law"]),
                title=self._prep(def_code["Public Law Short Title"]) or None,
                group_name=self._prep(def_code["Group Name"]) or None,
                urls=self._prep(def_code["URLs"]) or None,
                earliest_public_law_enactment_date=self._prep_date(def_code["Earliest Public Law Enactment Date"]),
            )
            for row_number, def_code in enumerate(raw_def_codes, start=1)
        ]

        return len(self.def_codes)

    @staticmethod
    def _validate_raw_def_code(raw_def_codes):
        messages = []

        if not DEF_CODE_PATTERN.fullmatch(raw_def_codes.code):
            messages.append(
                f"Invalid DEF Code '{raw_def_codes.code}' in row "
                f"{raw_def_codes.row_number:,}. DEF codes must be only one or two alphanumeric characters."
            )

        if not raw_def_codes.public_law:
            messages.append(f"DEF Code public law is required in row {raw_def_codes.row_number:,}.")

        return messages

    def _validate_raw_def_codes(self):
        messages = []

        for raw_def_code in self.def_codes:
            messages += self._validate_raw_def_code(raw_def_code)

        if messages:
            for message in messages:
                logger.error(message)
            raise RuntimeError(
                f"{len(messages):,} problem(s) have been found with the raw DEF Code file.  See log for details."
            )

    def _import_def_codes(self):
        with get_connection(read_only=False).cursor() as cursor:
            execute_values(
                cursor.cursor,
                """
                    insert into temp_load_disaster_emergency_fund_codes (
                        row_number,
                        code,
                        public_law,
                        title,
                        group_name,
                        urls,
                        earliest_public_law_enactment_date
                    ) values %s
                """,
                self.def_codes,
                page_size=len(self.def_codes),
            )
            return cursor.rowcount

    def _perform_load(self):
        overrides = {
            "insert_overrides": {"create_date": SQL("now()"), "update_date": SQL("now()")},
            "update_overrides": {"update_date": SQL("now()")},
        }

        def_code_table = ETLTable("disaster_emergency_fund_code", **overrides)
        temp_def_code_table = ETLTemporaryTable("temp_load_disaster_emergency_fund_codes")

        self._execute_dml_sql(CREATE_TEMP_TABLE, "Create disaster_emergency_fund_code temp table")
        self._execute_function_and_log(self._read_raw_def_code_csv, "Read raw DEF Code csv")
        self._execute_function(self._validate_raw_def_codes, "Validate raw DEF Codes")
        self._execute_function_and_log(self._import_def_codes, "Import DEF Codes")

        self._execute_function_and_log(
            update_changed_rows, "Update changed DEF Codes", temp_def_code_table, def_code_table
        )
        self._execute_function_and_log(
            insert_missing_rows, "Insert missing DEF Codes", temp_def_code_table, def_code_table
        )

    def _vacuum_tables(self):
        self._execute_dml_sql(
            "vacuum (full, analyze) disaster_emergency_fund_code", "Vacuum disaster_emergency_fund_code table"
        )
