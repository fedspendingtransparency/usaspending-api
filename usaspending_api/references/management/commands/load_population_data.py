import logging

from typing import List
from django.core.management.base import BaseCommand

from django.db import connection

from usaspending_api.references.models import PopCongressionalDistrict, PopCounty
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries


TEMP_TABLE_NAME = "temp_population_load"
TEMP_TABLE_SQL = "CREATE TABLE {table} ({columns});"
COUNTY_COLUMNS_MAPPER = {
    "state": "state_code",
    "county": "county_number",
    "stname": "state_name",
    "ctyname": "county_name",
    "popestimate2019": "latest_population",
}
DISTRICT_COLUMNS_MAPPER = {
    "state_code": "state_code",
    "state_name": "state_name",
    "usps_code": "state_abbreviation",
    "congressional_district": "congressional_district",
    "popestimate2019": "latest_population",
}


logger = logging.getLogger("script")


class Command(BaseCommand):

    help = "Load CSV files containing population data. "

    def add_arguments(self, parser):

        parser.add_argument("--file", required=True, help="Path or URI of the raw object class CSV file to be loaded.")
        parser.add_argument(
            "--type",
            choices=["county", "district"],
            required=True,
            help="Load either a county file or a congressional district file from census.gov",
        )

    def handle(self, *args, **options):
        self.type = options["type"]
        logger.info(f"Loading {self.type} Population data from {options['file']}")
        csv_dict_list = read_csv_file_as_list_of_dictionaries(options["file"])
        if csv_dict_list:
            cols = csv_dict_list[0].keys()
        else:
            raise RuntimeError(f"No data in CSV {options['file']}")

        self.drop_temp_table()
        self.create_table(columns=cols)
        self.load_data(csv_dict_list)
        self.drop_temp_table()

    def drop_temp_table(self):
        logger.info(f"Dropping temp table {TEMP_TABLE_NAME}")
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE_NAME}")

    def create_table(self, columns: List[str]) -> None:
        logger.info(f"Creating temp table {TEMP_TABLE_NAME}")
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE_NAME}")
            cursor.execute(
                TEMP_TABLE_SQL.format(table=TEMP_TABLE_NAME, columns=",".join([f"{c} TEXT" for c in columns]))
            )

    def load_data(self, data: List[dict]) -> None:
        model = PopCongressionalDistrict
        mapper = DISTRICT_COLUMNS_MAPPER
        if self.type == "county":
            model = PopCounty
            mapper = COUNTY_COLUMNS_MAPPER

        logger.info(f"Attempting to load {len(data)} records into {model.__name__}")

        model.objects.all().delete()

        model.objects.bulk_create([model(**{col: row[csv] for csv, col in mapper.items()}) for row in data])
        logger.info("Success? Please Verify")
