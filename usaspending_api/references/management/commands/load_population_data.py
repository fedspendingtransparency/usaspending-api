import logging

from typing import List
from django.core.management.base import BaseCommand

from django.db import connection

# from psycopg2.extras import execute_values
# from psycopg2.sql import SQL
# from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
# from usaspending_api.common.etl import ETLTable, mixins, ETLTemporaryTable
# from usaspending_api.common.etl.operations import insert_missing_rows, update_changed_rows
# from usaspending_api.common.helpers.sql_helpers import get_connection
# from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
# from usaspending_api.references.models import ObjectClass
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries


TEMP_TABLE_NAME = "temp_population_load"
TEMP_TABLE_SQL = "CREATE TABLE {table} ({columns});"

COPY_COUNTY_DATA = """

"""

COUNTY_COLUMNS_MAPPER = {
    "STATE": "state_code",
    "COUNTY": "county_number",
    "STNAME": "state_name",
    "CTYNAME": "county_name",
    "POPESTIMATE2018": "latest_population",
}

logger = logging.info


class Command(BaseCommand):

    help = "Load CSV files contianing population data. "

    def add_arguments(self, parser):

        parser.add_argument("--file", required=True, help="Path or URI of the raw object class CSV file to be loaded.")
        parser.add_argument(
            "--type",
            choices=["county", "district"],
            required=True,
            help="Load either a county file or a congressional district file from census.gov",
        )

    def handle(self, *args, **options):
        csv_dict_list = read_csv_file_as_list_of_dictionaries(options["file"], text=True)
        if csv_dict_list:
            cols = csv_dict_list[0].keys()
        else:
            raise RuntimeError(f"No data in CSV {options['file']}")

        self.create_table(columns=cols)

    def create_table(self, columns: List[str]) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                TEMP_TABLE_SQL.format(table=TEMP_TABLE_NAME, columns=",".join([f"{c} TEXT" for c in columns]))
            )
