import logging
import asyncio
import sqlparse
from pathlib import Path

from django.db import connection, transaction
from django.core.management.base import BaseCommand
from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.common.matview_manager import DEFAULT_CHUNKED_MATIVEW_DIR

logger = logging.getLogger("script")


class Command(BaseCommand):

    help = """
    This script combines the chunked Universal Transaction Matviews and
    combines them into a single table.
    """

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

    def handle(self, *args, **options):
        chunk_count = options["chunk_count"]
        index_concurrency = options["index_concurrency"]
        self.matview_dir = options["matview_dir"]

        logger.info(f"Chunk Count: {chunk_count}")

        with Timer("Recreating table"):
            self.recreate_matview()

        with Timer("Inserting data into table"):
            self.insert_matview_data(chunk_count)

        with Timer("Creating table indexes"):
            self.create_indexes(index_concurrency)

        with Timer("Swapping Tables/Indexes"):
            self.swap_matviews()

        if not options["keep_old_data"]:
            with Timer("Clearing old table"):
                self.remove_old_data(chunk_count)

        if not options["keep_matview_data"]:
            with Timer("Emptying Matviews"):
                self.empty_matviews()

        with Timer("Granting Table Permissions"):
            self.grant_matview_permissions()

    def recreate_matview(self):
        with connection.cursor() as cursor:
            sql = (self.matview_dir / "componentized" / "transaction_search__create.sql").read_text()
            cursor.execute(sql)

    def insert_matview_data(self, chunk_count):
        loop = asyncio.new_event_loop()
        tasks = []

        insert_table_sql = (self.matview_dir / "componentized" / "transaction_search__inserts.sql").read_text().strip()

        for i, sql in enumerate(sqlparse.split(insert_table_sql)):
            tasks.append(
                asyncio.ensure_future(async_run_creates(sql, wrapper=Timer(f"Insert into table {i}"),), loop=loop,)
            )

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

    async def index_with_concurrency(self, index_concurrency):
        semaphore = asyncio.Semaphore(index_concurrency)
        tasks = []

        async def create_with_sem(sql, index):
            async with semaphore:
                return await async_run_creates(sql, wrapper=Timer(f"Creating Index {index}"),)

        index_table_sql = (self.matview_dir / "componentized" / "transaction_search__indexes.sql").read_text().strip()

        for i, sql in enumerate(sqlparse.split(index_table_sql)):
            logger.info(f"Creating future for index: {i} - SQL: {sql}")
            tasks.append(create_with_sem(sql, i))

        return await asyncio.gather(*tasks)

    def create_indexes(self, index_concurrency):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.index_with_concurrency(index_concurrency))
        loop.close()

    @transaction.atomic
    def swap_matviews(self):

        swap_sql = (self.matview_dir / "componentized" / "transaction_search__renames.sql").read_text()

        with connection.cursor() as cursor:
            cursor.execute(swap_sql)

    def remove_old_data(self, chunk_count):

        drop_sql = (self.matview_dir / "componentized" / "transaction_search__drops.sql").read_text()

        with connection.cursor() as cursor:
            cursor.execute(drop_sql)

    def empty_matviews(self):
        empty_sql = (self.matview_dir / "componentized" / "transaction_search__empty.sql").read_text()

        with connection.cursor() as cursor:
            cursor.execute(empty_sql)

    def grant_matview_permissions(self):
        mods_sql = (self.matview_dir / "componentized" / "transaction_search__mods.sql").read_text()

        with connection.cursor() as cursor:
            cursor.execute(mods_sql)
