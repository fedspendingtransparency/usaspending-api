import csv
import logging

from django.conf import settings
from django.core.management import BaseCommand
from django.db import connections
from io import StringIO

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
            required=False,
            default="public",
            help="Schema name in the Broker DB to load into USAspending",
        )

        usaspending_group = parser.add_argument_group(
            title="usaspending",
            description="Optional fields that reference components of the USAspending DB.  Defaults to Broker DB counterpart",
        )
        usaspending_group.add_argument(
            "--usaspending-table-name",
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
            f'"{usas_schema_name}"."{usas_table_name}" in USAspending.'
        )
        broker_conn = connections[settings.DATA_BROKER_DB_ALIAS]
        usas_conn = connections[settings.DEFAULT_DB_ALIAS]
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM pg_tables
                WHERE schemaname = '{usas_schema_name}'
                AND tablename = '{usas_table_name}'
            );
        """
        with usas_conn.cursor() as usas_cursor:
            usas_cursor.execute(table_exists_query)
            table_exists = all(usas_cursor.fetchone())
            if not table_exists:
                raise ValueError(f"Table '{usas_schema_name}.{usas_table_name}' does not exist.")
            truncate_sql = f"TRUNCATE TABLE {usas_schema_name}.{usas_table_name}"
            usas_cursor.execute(truncate_sql)
            with broker_conn.cursor() as broker_cursor:
                broker_cursor.execute(f"SELECT * FROM {broker_schema_name}.{broker_table_name}")
                while True:
                    rows = broker_cursor.fetchmany(self.CHUNK_SIZE)
                    if not rows:
                        break
                    f = StringIO()
                    writer = csv.writer(f)
                    writer.writerows(rows)
                    f.seek(0)
                    usas_cursor.copy_expert(
                        f"COPY {usas_schema_name}.{usas_table_name} FROM STDIN WITH (DELIMITER ',', FORMAT CSV)",
                        f,
                    )
                    usas_conn.commit()
