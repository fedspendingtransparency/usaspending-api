"""
This is a quick and dirty loader for a "one off" dump of CARS File A data to support the Agency 2.0
feature on USAspending.  It is expected that this dump will be regenerated several times during
feature development to include more fields and more fiscal years.  I have made the assumption that
writing a loader for this will shorten development cycles as we receive new files since the loader
SHOULD only need minor tweaks moving forward to support new files.

The process:
    1) The product owner provides us with four Excel spreadsheet files:
        - DATA Act Budgetary Resources.xlsx
        - DATA Act Deobligations.xlsx
        - DATA Act Obligations.xlsx
        - DATA Act Outlays.xlsx.
    2) Export these spreadsheets to UTF-8 CSV files to:
        - cars_dump__budgetary_resources__yyyy_mm_dd.csv
        - cars_dump__deobligations__yyyy_mm_dd.csv
        - cars_dump__obligations__yyyy_mm_dd.csv
        - cars_dump__outlays__yyyy_mm_dd.csv.
    3) Remove the byte order mark (BOM) that Excel likes to add to CSV files.
        - I used PyCharm for this, but apparently awk is another option (untested):
            awk 'NR==1{sub(/^\xef\xbb\xbf/,"")}{print}' INFILE > OUTFILE
    4) Copy to s3://da-public-files/broker_reference_data/
    5) Optionally remove the old CSV files if you're positive we don't need them any more.
    6) Update the FILE_TO_TABLE_MAPPING to mapping below.
    7) Run this loader.

Note that this loader ALWAYS performs a full reload.
"""

import logging
import os
import psycopg2

from django.conf import settings
from django.core.management.base import BaseCommand
from pathlib import Path
from usaspending_api.accounts.models import HistoricalAppropriationAccountBalances
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string
from usaspending_api.common.helpers.timing_helpers import ScriptTimer
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri


logger = logging.getLogger("script")


DEFAULT_CSV_LOCATION = f"{settings.FILES_SERVER_BASE_URL}/reference_data/"

# file_name: temporary_table_name
FILE_TO_TABLE_MAPPING = {
    "cars_dump__budgetary_resources__2020_03_17.csv": "temp_load_historical_file_a_data_act_budgetary_resources",
    "cars_dump__deobligations__2020_03_17.csv": "temp_load_historical_file_a_data_act_deobligations",
    "cars_dump__obligations__2020_03_17.csv": "temp_load_historical_file_a_data_act_obligations",
    "cars_dump__outlays__2020_03_17.csv": "temp_load_historical_file_a_data_act_outlays",
}

# column_name: data_type
DATA_TYPE_MAPPING = {
    "ALLOC_XFER_AGENCY": "text",
    "AGENCY_IDENTIFIER": "text",
    "AVAIL_TYPE_CODE": "text",
    "BEG_POA": "text",
    "END_POA": "text",
    "MAIN_ACCT": "text",
    "SUB_ACCT": "text",
    "TAS_TITLE": "text",
    "FR_ENTITY": "text",
    "BUDGET_SUBFUNCTION": "text",
    "STMT_DETAIL_LABEL": "text",
    "FY": "smallint",
    "PD": "smallint",
    "BUDGETARY_RESOURCES_AMOUNT": "decimal(23, 2)",
    "DEOBLIGATIONS_AMOUNT": "decimal(23, 2)",
    "OBLIGATIONS_AMOUNT": "decimal(23, 2)",
    "OUTLAYS_AMOUNT": "decimal(23, 2)",
}

READ_BUFFER_SIZE_BYTES = 1024 * 1024


class Timer(ScriptTimer):
    """Make logging a little less verbose."""

    def log_starting_message(self):
        pass


class Command(BaseCommand):
    help = (
        "One off loader for pulling in historical File A CARS data from CSV files provided by the "
        "product owner.  Performs a full replace every run."
    )

    def add_arguments(self, parser):

        parser.add_argument(
            "source_location",
            default=DEFAULT_CSV_LOCATION,
            metavar="SOURCEURI",
            nargs="?",
            help=(
                "Path or URI to the location of the raw CSV files to be loaded.  Raw files MUST BE NAMED "
                f"{', '.join(FILE_TO_TABLE_MAPPING.keys())}!  Default location: {DEFAULT_CSV_LOCATION}"
            ),
        )

    def handle(self, *args, **options):

        source_location = options["source_location"]
        logger.info(f"SOURCE CSV LOCATION: {source_location}")

        with psycopg2.connect(get_database_dsn_string()) as connection:
            with connection.cursor() as cursor:
                self.connection = connection
                self.cursor = cursor
                for file_name, table_name in FILE_TO_TABLE_MAPPING.items():
                    with Timer(f"Copy {file_name}"):
                        uri = os.path.join(source_location, file_name)
                        file_path = RetrieveFileFromUri(uri).copy_to_temporary_file()
                    with Timer(f"Get CSV headers from {file_name}"):
                        headers = self._get_headers(file_path)
                    with Timer(f"Create temporary table {table_name}"):
                        self._create_temporary_table(table_name, headers)
                    with Timer(f"Import {file_name}"):
                        self._import_file(file_path, table_name)
                    os.remove(file_path)

                destination_table_name = HistoricalAppropriationAccountBalances._meta.db_table
                with Timer(f"Empty {destination_table_name}"):
                    cursor.execute(f"delete from {destination_table_name}")
                with Timer(f"Import into {destination_table_name}"):
                    self._import_data()

    def _create_temporary_table(self, table_name, headers):
        columns = ", ".join(f"{h} {DATA_TYPE_MAPPING[h]}" for h in headers)
        sql = f"create temporary table {table_name} ({columns})"
        self.cursor.execute(sql)

    def _get_headers(self, file_path):
        with open(file_path) as f:
            return next(f).strip().split(",")

    def _import_data(self):
        sql = (Path(__file__).resolve().parent / "load_historical_appropriation_account_balances.sql").read_text()
        self.cursor.execute(sql)

    def _import_file(self, file_path, table_name):
        sql = f"copy {table_name} from stdin with (format csv, header)"
        with open(file_path) as f:
            self.cursor.copy_expert(sql, f, READ_BUFFER_SIZE_BYTES)
        return self.cursor.rowcount
