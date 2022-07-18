import logging
import asyncio
from pathlib import Path

from django.db import connection, transaction
from django.core.management.base import BaseCommand

from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.sql_helpers import execute_sql_simple
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.common.matview_manager import DEFAULT_CHUNKED_MATIVEW_DIR
from usaspending_api.search.models.transaction_search import TransactionSearch
from usaspending_api.etl.management.commands.copy_table_metadata import create_indexes

logger = logging.getLogger("script")

TABLE_SCHEMA_NAME = "rpt"
TABLE_NAME = "transaction_search"


class Command(BaseCommand):

    help = """
    This script combines the chunked Universal Transaction Matviews and
    combines them into a single table.
    """

    constraint_names = []

    def add_arguments(self, parser):
        parser.add_argument("--chunk-count", default=10, help="Number of chunked matviews to read from", type=int)
        parser.add_argument("--index-concurrency", default=20, help="Concurrency limit for index creation", type=int)
        parser.add_argument(
            "--matview-dir",
            type=Path,
            help="Choose a non-default directory to store materialized view SQL files.",
            default=DEFAULT_CHUNKED_MATIVEW_DIR,
        )
        parser.add_argument(
            "--keep-old-data",
            action="store_true",
            default=False,
            help="Indicates whether or not to drop old table at end of command",
        )
        parser.add_argument(
            "--keep-matview-data",
            action="store_true",
            default=False,
            help="Indicates whether or not to empty data from chunked matviews at the end of command",
        )
        parser.add_argument("--retry-count", default=5, help="Number of retry attempts for removing old data")

    def handle(self, *args, **options):
        warnings = []  # Warnings returned to Jenkins to send to Slack

        chunk_count = options["chunk_count"]
        index_concurrency = options["index_concurrency"]
        self.matview_dir = options["matview_dir"]
        self.retry_count = options["retry_count"]

        logger.info(f"Chunk Count: {chunk_count}")

        create_temp_indexes, rename_indexes = self.read_index_definitions()

        with Timer("Recreating table"):
            execute_sql_simple((self.matview_dir / "componentized" / f"{TABLE_NAME}__create.sql").read_text())

        with Timer("Inserting data into table"):
            self.insert_matview_data(chunk_count)

        with Timer("Creating table indexes"):
            create_indexes(create_temp_indexes, index_concurrency)

        with Timer("Swapping Tables/Indexes"):
            self.swap_tables(rename_indexes)

        if not options["keep_old_data"]:
            with Timer("Clearing old table"):
                execute_sql_simple((self.matview_dir / "componentized" / f"{TABLE_NAME}__drops.sql").read_text())

        if not options["keep_matview_data"]:
            with Timer("Emptying Matviews"):
                execute_sql_simple((self.matview_dir / "componentized" / f"{TABLE_NAME}__empty.sql").read_text())

        with Timer("Granting Table Permissions"):
            execute_sql_simple((self.matview_dir / "componentized" / f"{TABLE_NAME}__mods.sql").read_text())

        return "\n".join(warnings)

    def read_index_definitions(self):
        indexes_sql = (self.matview_dir / "componentized" / f"{TABLE_NAME}__indexes.sql").read_text()

        with connection.cursor() as cursor:
            cursor.execute(indexes_sql)
            rows = cursor.fetchall()

        create_temp_indexes = []
        rename_indexes_old = []
        rename_indexes_temp = []
        for row in rows:
            index_name = row[0]
            index_name_temp = index_name + "_temp"
            index_name_old = index_name + "_old"

            # Ensure that the index hasn't already been created by a constraint
            if index_name not in self.constraint_names:
                create_temp_index_sql = row[1].replace(index_name, index_name_temp)
                create_temp_index_sql = create_temp_index_sql.replace(
                    f"{TABLE_SCHEMA_NAME}.{TABLE_NAME}", f"{TABLE_SCHEMA_NAME}.{TABLE_NAME}_temp"
                )
                create_temp_indexes.append(create_temp_index_sql)

                rename_indexes_old.append(f"ALTER INDEX IF EXISTS {index_name} RENAME TO {index_name_old};")
                rename_indexes_temp.append(f"ALTER INDEX IF EXISTS {index_name_temp} RENAME TO {index_name};")

        return create_temp_indexes, rename_indexes_old + rename_indexes_temp

    def insert_matview_data(self, chunk_count):
        loop = asyncio.new_event_loop()
        tasks = []

        columns = [f.column for f in TransactionSearch._meta.fields]
        column_string = ",".join(columns)

        for chunk in range(chunk_count):
            sql = f"INSERT INTO {TABLE_NAME}_temp ({column_string}) SELECT {column_string} FROM {TABLE_NAME}_{chunk}"
            tasks.append(
                asyncio.ensure_future(
                    async_run_creates(
                        sql,
                        wrapper=Timer(f"Insert into table from transaction_search_{chunk}"),
                    ),
                    loop=loop,
                )
            )

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

    @transaction.atomic
    def swap_tables(self, rename_indexes):

        swap_sql = (self.matview_dir / "componentized" / f"{TABLE_NAME}__renames.sql").read_text()

        swap_sql += "\n".join(rename_indexes)

        logger.debug(swap_sql)

        with connection.cursor() as cursor:
            cursor.execute(swap_sql)
