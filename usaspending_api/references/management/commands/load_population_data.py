import logging
import re


from django.core.management.base import BaseCommand
from django.db import transaction
from psycopg2.extras import execute_values
from psycopg2.sql import SQL
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.etl import ETLTable, mixins, ETLTemporaryTable
from usaspending_api.common.etl.operations import insert_missing_rows, update_changed_rows
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.references.models import ObjectClass

TEMP_TABLE_SQL = "CREATE TABLE temp_population_load ({});"

COPY_COUNTY_DATA = """

"""

COUNTY_COLUMNS = ["STATE", "COUNTY", "STNAME", "CTYNAME", "POPESTIMATE2018"]



class Command(BaseCommand):

    help = "Load object class CSV file.  If anything fails, nothing gets saved.  DOES NOT DELETE RECORDS."
    object_class_file = None
    etl_logger_function = logger.info

    def add_arguments(self, parser):

        parser.add_argument(
            "object_class_file", metavar="FILE", help="Path or URI of the raw object class CSV file to be loaded."
        )

    def handle(self, *args, **options):
        pass
