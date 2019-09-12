"""
! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !

This is pre-work for DEV-2752 and will be folded into mainline code as part of that ticket.

If this warning is still hanging around in the year 2020, it's probably safe to drop this
model as the original developer probably won the lottery or something and now owns an island
in the Pacific and can't be bothered with such nonsense.

! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !
"""
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
    ],
)


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
            help="Path (for local files) or URI (for http(s) or S3 files) of the raw agency CSV file to be loaded.",
        )

    def handle(self, *args, **options):
        self.agency_file = options["agency_file"]
        logger.info("AGENCY FILE: {}".format(self.agency_file))

        self.connection = get_connection(read_only=False)

        try:
            with Timer("Import agencies"):
                self._read_agencies()
                self._validate_agencies()
                with transaction.atomic():
                    self._execute_sql_file("create_temp_table.sql")
                    self._import_agencies()
                    self._execute_sql_file("populate_dependent_tables.sql")
                self._vacuum_tables()
        except Exception:
            logger.error("ALL CHANGES WERE ROLLED BACK DUE TO EXCEPTION")
            raise

    def _execute_sql(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            if cursor.rowcount > -1:
                logger.info("{:,} rows affected".format(cursor.rowcount))

    def _execute_sql_file(self, filename):
        file = self.sql_path / filename
        sql = file.read_text()
        self._execute_sql(sql)

    def _read_agencies(self):
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
            )
            for row_number, agency in enumerate(agencies)
        ]

    def _validate_agencies(self):
        for agency in self.agencies:
            if agency.cgac_agency_code is not None and agency.agency_name is None:
                raise RuntimeError("Row number {:,} has a CGAC but no AGENCY NAME".format(agency.row_number))
            if agency.frec is not None and agency.frec_entity_description is None:
                raise RuntimeError(
                    "Row number {:,} has a FREC but no FREC Entity Description".format(agency.row_number)
                )
            if agency.subtier_code is not None and agency.subtier_name is None:
                raise RuntimeError("Row number {:,} has a SUBTIER CODE but no SUBTIER NAME".format(agency.row_number))
            if agency.is_frec is True and agency.frec is None:
                raise RuntimeError("Row number {:,} is marked as IS_FREC but has no FREC".format(agency.row_number))
            if agency.is_frec is not True and agency.cgac_agency_code is None:
                raise RuntimeError(
                    "Row number {:,} is not marked as IS_FREC but has no CGAC AGENCY CODE".format(agency.row_number)
                )

    def _import_agencies(self):

        with self.connection.cursor() as cursor:
            execute_values(
                cursor.cursor,
                """
                    insert into temp_raw_agency_csv (
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
                        icon_filename
                    ) values %s
                """,
                self.agencies,
            )

    def _vacuum_tables(self):
        self._execute_sql("vacuum (full, analyze) cgac")
        self._execute_sql("vacuum (full, analyze) frec")
        self._execute_sql("vacuum (full, analyze) subtier_agency_new")
        self._execute_sql("vacuum (full, analyze) toptier_agency_new")
