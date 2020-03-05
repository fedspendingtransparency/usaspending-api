import logging

from collections import namedtuple
from distutils.util import strtobool
from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from psycopg2.extras import execute_values
from psycopg2.sql import SQL
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.etl import ETLQueryFile, ETLTable, mixins
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.common.helpers.text_helpers import standardize_nullable_whitespace as prep
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer

logger = logging.getLogger("console")

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
        "user_selectable",
        "mission",
        "website",
        "congressional_justification",
        "icon_filename",
    ],
)

MAX_CHANGES = 75


class Command(mixins.ETLMixin, BaseCommand):
    help = (
        "Loads CGACs, FRECs, Subtier Agencies, Toptier Agencies, and Agencies.  Load is all or nothing.  "
        "If anything fails, nothing gets saved."
    )

    agency_file = None
    force = False

    etl_logger_function = logger.info
    etl_dml_sql_directory = Path(__file__).resolve().parent / "load_agencies_sql"

    def add_arguments(self, parser):

        parser.add_argument(
            "agency_file",
            metavar="AGENCY_FILE",
            help="Path (for local files) or URI (for http(s) or S3 files) of the raw agency CSV file to be loaded.",
        )

        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                "Reloads agencies even if the max change threshold of {:,} is exceeded.  This is a safety "
                "precaution to prevent accidentally updating every award, transaction, and subaward in the system as "
                "part of the nightly pipeline.  Will also force foreign key table links to be examined even if it "
                "appears there were no agency changes.".format(MAX_CHANGES)
            ),
        )

    def handle(self, *args, **options):

        self.agency_file = options["agency_file"]
        self.force = options["force"]

        logger.info("AGENCY FILE: {}".format(self.agency_file))
        logger.info("FORCE SWITCH: {}".format(self.force))
        logger.info("MAX CHANGE LIMIT: {}".format("unlimited" if self.force else "{:,}".format(MAX_CHANGES)))

        with Timer("Load agencies"):
            try:
                with transaction.atomic():
                    self._perform_load()
                    t = Timer("Commit agency transaction")
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

    def _read_raw_agencies_csv(self):
        agencies = read_csv_file_as_list_of_dictionaries(self.agency_file)
        if len(agencies) < 1:
            raise RuntimeError("Agency file '{}' appears to be empty".format(self.agency_file))

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
                user_selectable=bool(strtobool(prep(agency["USER SELECTABLE ON USASPENDING.GOV"]))),
                mission=prep(agency["MISSION"]),
                website=prep(agency["WEBSITE"]),
                congressional_justification=prep(agency["CONGRESSIONAL JUSTIFICATION"]),
                icon_filename=prep(agency["ICON FILENAME"]),
            )
            for row_number, agency in enumerate(agencies, start=1)
        ]

        return len(self.agencies)

    @staticmethod
    def _validate_raw_agency(agency):
        messages = []

        if agency.cgac_agency_code is not None and agency.agency_name is None:
            message = "Row number {:,} has a CGAC AGENCY CODE but no AGENCY NAME"
            messages.append(message.format(agency.row_number))
        if agency.frec is not None and agency.frec_entity_description is None:
            messages.append("Row number {:,} has a FREC but no FREC Entity Description".format(agency.row_number))
        if agency.subtier_code is not None and agency.subtier_name is None:
            messages.append("Row number {:,} has a SUBTIER CODE but no SUBTIER NAME".format(agency.row_number))
        if agency.is_frec is True and agency.frec is None:
            messages.append("Row number {:,} is marked as IS_FREC but has no FREC".format(agency.row_number))
        if agency.is_frec is not True and agency.cgac_agency_code is None:
            messages.append(
                "Row number {:,} is not marked as IS_FREC but has no CGAC AGENCY CODE".format(agency.row_number)
            )
        if agency.cgac_agency_code and len(agency.cgac_agency_code) != 3:
            messages.append(
                "Row number {:,} has CGAC AGENCY CODE that is not 3 characters long ({})".format(
                    agency.row_number, agency.cgac_agency_code
                )
            )
        if agency.frec and len(agency.frec) != 4:
            messages.append(
                "Row number {:,} has FREC that is not 4 characters long ({})".format(agency.row_number, agency.frec)
            )
        if agency.subtier_code and len(agency.subtier_code) != 4:
            messages.append(
                "Row number {:,} has SUBTIER CODE that is not 4 characters long ({})".format(
                    agency.row_number, agency.subtier_code
                )
            )

        return messages

    def _validate_raw_agencies(self):

        messages = []
        for agency in self.agencies:
            messages += self._validate_raw_agency(agency)

        if messages:
            for message in messages:
                logger.error(message)
            raise RuntimeError(
                "{:,} problem(s) have been found with the agency file.  See log for details.".format(len(messages))
            )

    def _import_raw_agencies(self):
        with get_connection(read_only=False).cursor() as cursor:
            execute_values(
                cursor.cursor,
                """
                    insert into temp_load_agencies_raw_agency (
                        row_number,
                        cgac_agency_code,
                        agency_name,
                        agency_abbreviation,
                        frec,
                        frec_entity_description,
                        frec_abbreviation,
                        subtier_code,
                        subtier_name,
                        subtier_abbreviation,
                        toptier_flag,
                        is_frec,
                        user_selectable,
                        mission,
                        website,
                        congressional_justification,
                        icon_filename
                    ) values %s
                """,
                self.agencies,
                page_size=len(self.agencies),
            )
            return cursor.rowcount

    def _perform_load(self):

        overrides = {
            "insert_overrides": {"create_date": SQL("now()"), "update_date": SQL("now()")},
            "update_overrides": {"update_date": SQL("now()")},
        }

        agency_table = ETLTable("agency", key_overrides=["toptier_agency_id", "subtier_agency_id"], **overrides)
        cgac_table = ETLTable("cgac", key_overrides=["cgac_code"])
        frec_table = ETLTable("frec", key_overrides=["frec_code"])
        subtier_agency_table = ETLTable("subtier_agency", key_overrides=["subtier_code"], **overrides)
        toptier_agency_table = ETLTable("toptier_agency", key_overrides=["toptier_code"], **overrides)

        agency_query = ETLQueryFile(self.etl_dml_sql_directory / "agency_query.sql")
        cgac_query = ETLQueryFile(self.etl_dml_sql_directory / "cgac_query.sql")
        frec_query = ETLQueryFile(self.etl_dml_sql_directory / "frec_query.sql")
        subtier_agency_query = ETLQueryFile(self.etl_dml_sql_directory / "subtier_agency_query.sql")
        toptier_agency_query = ETLQueryFile(self.etl_dml_sql_directory / "toptier_agency_query.sql")

        self._execute_etl_dml_sql_directory_file("raw_agency_create_temp_table", "Create raw agency temp table")
        self._execute_function_and_log(self._read_raw_agencies_csv, "Read raw agencies csv")
        self._execute_function(self._validate_raw_agencies, "Validate raw agencies")
        self._execute_function_and_log(self._import_raw_agencies, "Import raw agencies")

        self._delete_update_insert_rows("CGACs", cgac_query, cgac_table)
        self._delete_update_insert_rows("FRECs", frec_query, frec_table)

        rows_affected = 0
        rows_affected += self._delete_update_insert_rows("toptier agencies", toptier_agency_query, toptier_agency_table)
        rows_affected += self._delete_update_insert_rows("subtier agencies", subtier_agency_query, subtier_agency_table)
        rows_affected += self._delete_update_insert_rows("agencies", agency_query, agency_table)

        if rows_affected > MAX_CHANGES and not self.force:
            raise RuntimeError(
                "Exceeded maximum number of allowed changes ({:,}).  Use --force switch if this was "
                "intentional.".format(MAX_CHANGES)
            )

        elif rows_affected > 0 or self.force:
            self._execute_etl_dml_sql_directory_file(
                "treasury_appropriation_account_update", "Update treasury appropriation accounts"
            )
            self._execute_etl_dml_sql_directory_file("transaction_normalized_update", "Update transactions")
            self._execute_etl_dml_sql_directory_file("award_update", "Update awards")
            self._execute_etl_dml_sql_directory_file("subaward_update", "Update subawards")

        else:
            logger.info(
                "Skipping treasury_appropriation_account, transaction_normalized, "
                "awards, and subaward updates since there were no agency changes."
            )

    def _vacuum_tables(self):
        self._execute_dml_sql("vacuum (full, analyze) agency", "Vacuum agency table")
        self._execute_dml_sql("vacuum (full, analyze) cgac", "Vacuum cgac table")
        self._execute_dml_sql("vacuum (full, analyze) frec", "Vacuum frec table")
        self._execute_dml_sql("vacuum (full, analyze) subtier_agency", "Vacuum subtier_agency table")
        self._execute_dml_sql("vacuum (full, analyze) toptier_agency", "Vacuum toptier_agency table")
