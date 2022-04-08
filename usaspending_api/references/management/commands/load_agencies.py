import logging

from collections import namedtuple
from distutils.util import strtobool
from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from psycopg2.extras import execute_values
from psycopg2.sql import SQL
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.etl.postgres import ETLQueryFile, ETLTable
from usaspending_api.common.etl.postgres import mixins
from usaspending_api.common.helpers.sql_helpers import get_connection, execute_sql
from usaspending_api.common.helpers.text_helpers import standardize_nullable_whitespace as prep
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer
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


class Command(mixins.ETLMixin, BaseCommand):
    help = (
        "Loads CGACs, FRECs, Subtier Agencies, Toptier Agencies, and Agencies.  Load is all or nothing.  "
        "If anything fails, nothing gets saved."
    )

    agency_file = None
    force = False

    etl_logger_function = logger.info
    etl_dml_sql_directory = Path(__file__).resolve().parent / "load_agencies_sql"
    etl_timer = Timer

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
                f"Reloads agencies even if the max change threshold of {MAX_CHANGES:,} is exceeded.  This is a "
                f"safety precaution to prevent accidentally updating every award, transaction, and subaward in "
                f"the system as part of the nightly pipeline.  Will also force foreign key table links to be "
                f"examined even if it appears there were no agency changes."
            ),
        )

    def handle(self, *args, **options):

        self.agency_file = options["agency_file"]
        self.force = options["force"]

        logger.info(f"AGENCY FILE: {self.agency_file}")
        logger.info(f"FORCE SWITCH: {self.force}")
        logger.info(f"MAX CHANGE LIMIT: {'unlimited' if self.force else f'{MAX_CHANGES:,}'}")

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

    def _perform_validations(self):

        sql = (Path(self.etl_dml_sql_directory) / "validations.sql").read_text().format(temp_table=TEMP_TABLE_NAME)
        messages = [result[0] for result in execute_sql(sql, read_only=False)]

        if messages:
            m = "\n".join(messages)
            raise RuntimeError(f"The following {len(messages):,} problem(s) have been found with the agency file:\n{m}")

    def _import_raw_agencies(self):
        sql = (Path(self.etl_dml_sql_directory) / "insert_into.sql").read_text().format(temp_table=TEMP_TABLE_NAME)
        with get_connection(read_only=False).cursor() as cursor:
            execute_values(cursor.cursor, sql, self.agencies, page_size=len(self.agencies))
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
