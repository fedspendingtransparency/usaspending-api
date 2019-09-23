import logging

from collections import namedtuple
from distutils.util import strtobool
from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from psycopg2.extras import execute_values
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.common.helpers.text_helpers import standardize_nullable_whitespace as prep
from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.references.constants import DOD_ARMED_FORCES_CGAC


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
        "mission",
        "website",
        "congressional_justification",
        "icon_filename",
        "include_toptier_without_subtier",
    ],
)

# Set keep_count to True to update the overall affected row count.  Set skip_if_no_changes to True
# to not run this step if the overall affected row count is zero.  Useful for not running expensive
# steps if nothing changed.
ProcessingStep = namedtuple("ProcessingStep", ["description", "file", "function", "keep_count", "skip_if_no_changes"])

MAX_CHANGES = 50

PROCESSING_STEPS = [
    ProcessingStep("Create raw agency temp table", "raw_agency_create_temp_table", None, False, False),
    ProcessingStep("Read raw agencies csv", None, "_read_raw_agencies_csv", False, False),
    ProcessingStep("Validate raw agencies", None, "_validate_raw_agencies", False, False),
    ProcessingStep("Import raw agencies", None, "_import_raw_agencies", False, False),
    ProcessingStep("Recreate CGACs", "cgac_recreate", None, False, False),
    ProcessingStep("Recreate FRECs", "frec_recreate", None, False, False),
    ProcessingStep("Create toptier agency temp table", "toptier_agency_create_temp_table", None, False, False),
    ProcessingStep("Delete obsolete toptier agencies", "toptier_agency_delete", None, True, False),
    ProcessingStep("Update existing toptier agencies", "toptier_agency_update", None, True, False),
    ProcessingStep("Create new toptier agencies", "toptier_agency_create", None, True, False),
    ProcessingStep("Create subtier agency temp table", "subtier_agency_create_temp_table", None, False, False),
    ProcessingStep("Delete obsolete subtier agencies", "subtier_agency_delete", None, True, False),
    ProcessingStep("Update existing subtier agencies", "subtier_agency_update", None, True, False),
    ProcessingStep("Create new subtier agencies", "subtier_agency_create", None, True, False),
    ProcessingStep("Create agency temp table", "agency_create_temp_table", None, False, False),
    ProcessingStep("Delete obsolete agencies", "agency_delete", None, True, False),
    ProcessingStep("Update existing agencies", "agency_update", None, True, False),
    ProcessingStep("Create new agencies", "agency_create", None, True, False),
    ProcessingStep(
        "Update treasury appropriation accounts", "treasury_appropriation_account_update", None, False, True
    ),
    ProcessingStep("Update transactions", "transaction_normalized_update", None, False, True),
    ProcessingStep("Update awards", "award_update", None, False, True),
    ProcessingStep("Update subawards", "subaward_update", None, False, True),
    ProcessingStep("Clean up", "clean_up", None, False, False),
]

SQL_PATH = Path(__file__).resolve().parent / "load_agencies_sql"


class Command(BaseCommand):

    help = (
        "Loads CGACs, FRECs, Subtier Agencies, Toptier Agencies, and Agencies.  Load is all or nothing.  "
        "If anything fails, nothing gets saved."
    )

    agency_file = None
    connection = get_connection(read_only=False)
    force = False
    rows_affected_count = 0

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
                "Reloads agencies even if the change max change threshold of {:,} is exceeded.  This is a safety "
                "feature to prevent accidentally updating every award, transaction, and subaward in the system as "
                "part of the nightly pipeline.".format(MAX_CHANGES)
            ),
        )

    def handle(self, *args, **options):
        self.agency_file = options["agency_file"]
        logger.info("AGENCY FILE: {}".format(self.agency_file))

        self.force = options["force"]
        logger.info("FORCE: {}".format(self.force))

        self.rows_affected_count = 0

        try:
            with Timer("Import agencies"):
                with transaction.atomic():
                    self._run_steps()
        except Exception:
            logger.error("ALL CHANGES WERE ROLLED BACK DUE TO EXCEPTION")
            raise

        try:
            with Timer("Vacuum tables"):
                self._vacuum_tables()
        except Exception:
            logger.error("CHANGES WERE SUCCESSFULLY COMMITTED EVEN THOUGH VACUUMS FAILED")
            raise

    def _execute_sql(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            if cursor.rowcount > -1:
                logger.info("{:,} rows affected".format(cursor.rowcount))
        return cursor.rowcount

    def _execute_sql_file(self, filename):
        file = (SQL_PATH / filename).with_suffix(".sql")
        sql = file.read_text()
        return self._execute_sql(sql)

    def _read_raw_agencies_csv(self):
        agencies = read_csv_file_as_list_of_dictionaries(self.agency_file)
        if len(agencies) < 1:
            raise RuntimeError("Agency file '{}' appears to be empty".format(self.agency_file))

        self.agencies = [
            Agency(
                row_number=row_number + 1,
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
                mission=prep(agency["MISSION"]),
                website=prep(agency["WEBSITE"]),
                congressional_justification=prep(agency["CONGRESSIONAL JUSTIFICATION"]),
                icon_filename=prep(agency["ICON FILENAME"]),
                include_toptier_without_subtier=prep(agency["CGAC AGENCY CODE"]) in DOD_ARMED_FORCES_CGAC,
            )
            for row_number, agency in enumerate(agencies)
        ]

    def _validate_raw_agencies(self):
        messages = []

        for agency in self.agencies:
            if agency.cgac_agency_code is not None and agency.agency_name is None:
                messages.append("Row number {:,} has a CGAC AGENCY CODE but no AGENCY NAME".format(agency.row_number))
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

        if messages:
            for message in messages:
                logger.error(message)
            raise RuntimeError(
                "{:,} problem(s) have been found with the agency file.  See log for details.".format(len(messages))
            )

    def _import_raw_agencies(self):
        with self.connection.cursor() as cursor:
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
                        mission,
                        website,
                        congressional_justification,
                        icon_filename,
                        include_toptier_without_subtier
                    ) values %s
                """,
                self.agencies,
                page_size=len(self.agencies),
            )
            if cursor.rowcount > -1:
                logger.info("{:,} rows affected".format(cursor.rowcount))

    def _run_steps(self):
        for step in PROCESSING_STEPS:

            with Timer(step.description):

                if step.skip_if_no_changes and self.rows_affected_count == 0:
                    logger.info("Skipping due to no agency changes")
                    continue

                if step.file:
                    count = self._execute_sql_file(step.file)
                    # If a table for which we're tracking changes exceeds MAX_CHANGES and the
                    # --force switch was not supplied, raise an exception.
                    if step.keep_count and count > MAX_CHANGES and not self.force:
                        raise RuntimeError(
                            "Exceed maximum number of allowed changes ({:,}).  Use --force switch if this was "
                            "intentional.".format(MAX_CHANGES)
                        )
                    if step.keep_count:
                        self.rows_affected_count += count

                if step.function:
                    getattr(self, step.function)()

    def _vacuum_tables(self):
        self._execute_sql("vacuum (full, analyze) agency")
        self._execute_sql("vacuum (full, analyze) cgac")
        self._execute_sql("vacuum (full, analyze) frec")
        self._execute_sql("vacuum (full, analyze) subtier_agency")
        self._execute_sql("vacuum (full, analyze) toptier_agency")
