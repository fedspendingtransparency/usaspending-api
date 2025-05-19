import logging

from django.conf import settings
from django.core.management import BaseCommand
from django.db import connections
import pandas as pd


logger = logging.getLogger(__name__)


class Command(BaseCommand):

    help = "Command used for loading a broker table into usaspending"

    CHUNK_SIZE = 50_000

    def add_arguments(self, parser):

        broker_group = parser.add_argument_group(
            title="broker",
            description=("Fields that reference components of the Broker DB."),
        )
        broker_group.add_argument(
            "--table-name",
            type=str,
            required=True,
            help="Table name in the Broker DB to load into USAspending",
        )
        broker_group.add_argument(
            "--schema-name",
            type=str,
            required=True,
            help="Schema name in the Broker DB to load into USAspending",
        )

        usaspending_group = parser.add_argument_group(
            title="usaspending",
            description="Optional fields that reference components of the USAspending DB.  Defaults to Broker DB counterpart",
        )
        usaspending_group.add_argument(
            "--usasspending-table-name",
            type=str,
            required=False,
            help="Table name in the USAspending DB to load table into",
        )
        usaspending_group.add_argument(
            "--usaspending-schema-name",
            type=str,
            required=False,
            help="Schema name in the USAspending DB to load data into",
        )

    def handle(self, *args, **options):

        broker_table_name = options["table_name"]
        broker_schema_name = options["schema_name"]
        usas_table_name = options["usaspending_table_name"] or broker_table_name
        usas_schema_name = options["usaspending_schema_name"] or broker_schema_name

        logger.info(
            f'Copying "{broker_schema_name}"."{broker_table_name}" from Broker to '
            f'"{usas_table_name}"."{usas_schema_name}" in USAspending.'
        )
        broker_conn = connections[settings.DATA_BROKER_DB_ALIAS]
        usas_conn = connections[settings.DEFAULT_DB_ALIAS]
        first_load = True
        for chunk in pd.read_sql_table(
            table_name=broker_table_name,
            schema_name=broker_schema_name,
            con=broker_conn,
            chunksize=self.CHUNK_SIZE,
        ):
            chunk.to_sql(
                table_name=usas_table_name,
                schema=usas_schema_name,
                con=usas_conn,
                if_exists="replace" if first_load else "append",
                index=False,
            )
            first_load = False
