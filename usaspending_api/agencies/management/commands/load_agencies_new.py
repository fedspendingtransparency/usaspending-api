"""
! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !

This is pre-work for DEV-2752 and will be folded into mainline code as part of that ticket.

If this warning is still hanging around in the year 2020, it's probably safe to drop this
model as the original developer probably won the lottery or something and now owns an island
in the Pacific and can't be bothered with such nonsense.

! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !
"""
import logging

from distutils.util import strtobool
from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from psycopg2.extras import execute_values
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.common.helpers.text_helpers import standardize_nullable_whitespace as prep
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("console")


class Command(BaseCommand):

    help = (
        "Loads CGACs, FRECs, Subtier Agencies, and Toptier Agencies.  Load is all or nothing -- if "
        "anything fails, nothing gets saved."
    )

    agency_file = None
    connection = None
    sql_path = Path(__file__).resolve().parent / "load_agencies_sql"

    def add_arguments(self, parser):

        parser.add_argument(
            "agency_file",
            metavar="AGENCY_FILE",
            help=(
                "Path (for local files) or URI (for http(s) or S3 files) of the raw agency CSV " "file to be loaded."
            ),
        )

    def handle(self, *args, **options):
        self.agency_file = options["agency_file"]
        logger.info("AGENCY FILE: {}".format(self.agency_file))

        self.connection = get_connection(read_only=False)

        try:
            with Timer("Overall import"):
                with transaction.atomic():
                    self._execute_sql_file("create_temp_table.sql")
                    self._import_agency_file()
                    self._execute_sql_file("populate_dependent_tables.sql")
                self._vacuum_tables()
        except Exception:
            logger.error("ALL CHANGES WERE ROLLED BACK DUE TO EXCEPTION")
            raise

    def _execute_sql(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            rowcount = cursor.rowcount
            if rowcount > -1:
                logger.info("{:,} rows affected".format(rowcount))

    def _execute_sql_file(self, filename):
        file = self.sql_path / filename
        sql = file.read_text()
        self._execute_sql(sql)

    def _import_agency_file(self):
        agencies = read_csv_file_as_list_of_dictionaries(self.agency_file)
        if len(agencies) < 1:
            raise RuntimeError("Agency file '{}' appears to be empty".format(self.agency_file))

        agencies = [
            (
                prep(agency["CGAC AGENCY CODE"]),
                prep(agency["AGENCY NAME"]),
                prep(agency["AGENCY ABBREVIATION"]),
                prep(agency["FREC"]),
                prep(agency["FREC Entity Description"]),
                prep(agency["FREC ABBREVIATION"]),
                prep(agency["SUBTIER CODE"]),
                prep(agency["SUBTIER NAME"]),
                prep(agency["SUBTIER ABBREVIATION"]),
                bool(strtobool(prep(agency["TOPTIER_FLAG"]))),
                bool(strtobool(prep(agency["IS_FREC"]))),
                prep(agency["MISSION"]),
                prep(agency["WEBSITE"]),
                prep(agency["CONGRESSIONAL JUSTIFICATION"]),
                prep(agency["ICON FILENAME"]),
            )
            for agency in agencies
        ]

        with self.connection.cursor() as cursor:
            execute_values(
                cursor.cursor,
                """
                    insert into temp_raw_agency_csv (
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
                        icon_filename
                    ) values %s
                """,
                agencies,
            )

    def _vacuum_tables(self):
        self._execute_sql("vacuum analyze cgac")
        self._execute_sql("vacuum analyze frec")
        self._execute_sql("vacuum analyze subtier_agency_new")
        self._execute_sql("vacuum analyze toptier_agency_new")
