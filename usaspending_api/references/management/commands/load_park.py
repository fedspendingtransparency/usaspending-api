import logging
from collections import namedtuple
from contextlib import contextmanager
from typing import Generator

from django.core.management import BaseCommand, call_command
from django.db import connection, transaction

from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.references.models import ProgramActivityPark

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    is_full_reload = False
    temp_table_name = "load_park_to_usas_temp"

    def add_arguments(self, parser):
        parser.add_argument(
            "--full-reload",
            action="store_true",
            required=False,
            default=False,
            help=(
                "If provided the USAspending PARK table will be truncated and reloaded."
                " This does require removing all Foreign Keys that reference this table prior to reload"
                " and then replacing them after."
            ),
        )

    def handle(self, *args, **options) -> None:
        self.is_full_reload = options["full_reload"]

        with (
            Timer(
                "Creating PARK reference table in USAspending", success_logger=logger.info, failure_logger=logger.error
            ),
            connection.cursor() as cursor,
            self.create_temporary_table(cursor),
        ):
            self.populate_temporary_table(cursor)
            self.upsert_park_values(cursor)

    @contextmanager
    def create_temporary_table(self, cursor: connection.cursor) -> Generator[None, None, None]:
        logger.info(f"Creating table: {self.temp_table_name}")
        cursor.execute(
            f"""
            DROP TABLE IF EXISTS {self.temp_table_name};
            CREATE TABLE {self.temp_table_name} (
                park_name TEXT,
                park_code TEXT
            );
        """
        )

        yield

        cursor.execute(f"DROP TABLE IF EXISTS {self.temp_table_name};")

    @contextmanager
    def handle_foreign_keys(self, cursor: connection.cursor) -> Generator[None, None, None]:
        foreign_key_list = []
        if self.is_full_reload:
            ForeignKey = namedtuple(
                "ForeignKey", ["schema_name", "table_name", "constraint_name", "constraint_definition"]
            )
            cursor.execute(
                f"""
                SELECT
                    connamespace::REGNAMESPACE,
                    conrelid::REGCLASS,
                    conname,
                    PG_GET_CONSTRAINTDEF(oid)
                FROM pg_constraint
                WHERE REGCLASS(confrelid)::TEXT = '{ProgramActivityPark._meta.db_table}'
            """
            )
            foreign_key_list.extend([ForeignKey(*row) for row in cursor.fetchall()])
            for foreign_key in foreign_key_list:
                cursor.execute(
                    f"""
                    ALTER TABLE {foreign_key.schema_name}.{foreign_key.table_name}
                    DROP CONSTRAINT {foreign_key.constraint_name};
                """
                )

        yield

        if self.is_full_reload:
            for foreign_key in foreign_key_list:
                cursor.execute(
                    f"""
                    ALTER TABLE {foreign_key.schema_name}.{foreign_key.table_name}
                    ADD CONSTRAINT {foreign_key.constraint_name} {foreign_key.constraint_definition}
                """
                )

    def populate_temporary_table(self, cursor: connection.cursor) -> None:
        with Timer("Loading PARK from Broker to USAspending", success_logger=logger.info, failure_logger=logger.error):
            # Populate temp table with Broker PARK table
            call_command(
                "load_broker_table",
                "--table-name=program_activity_park",
                "--schema-name=public",
                f"--usaspending-table-name={self.temp_table_name}",
                "--usaspending-schema-name=public",
            )
            # Append additional records not found in the Broker table
            cursor.execute(
                f"INSERT INTO {self.temp_table_name} (park_code, park_name) VALUES ('0000', 'UNKNOWN/OTHER');"
            )

    @transaction.atomic
    def upsert_park_values(self, cursor: connection.cursor) -> None:
        with (
            Timer("Upserting PARK values in USAspending", success_logger=logger.info, failure_logger=logger.error),
            self.handle_foreign_keys(cursor),
        ):
            if self.is_full_reload:
                cursor.execute(f"TRUNCATE {ProgramActivityPark._meta.db_table};")
            cursor.execute(
                f"""
                WITH grouped_park AS (
                    SELECT DISTINCT park_code, park_name
                    FROM {self.temp_table_name}
                )
                INSERT INTO {ProgramActivityPark._meta.db_table} (code, name)
                SELECT park_code, UPPER(park_name)
                FROM grouped_park
                ON CONFLICT (code) DO UPDATE
                SET name = EXCLUDED.name
                WHERE {ProgramActivityPark._meta.db_table}.name IS DISTINCT FROM EXCLUDED.name;
            """
            )
            logger.info(f"Number of rows affected: {cursor.rowcount}")
