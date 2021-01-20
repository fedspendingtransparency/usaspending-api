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

    def handle(self, *args, **options):
        chunk_count = options["chunk_count"]
        index_concurrency = options["index_concurrency"]
        self.matview_dir = options["matview_dir"]

        logger.info(f"Chunk Count: {chunk_count}")

        # Constraints must be read first, so indexes created by pk/fk are not repeated
        create_temp_constraints, rename_constraints = self.read_constraint_definitions()
        create_temp_indexes, rename_indexes = self.read_index_definitions()

        with Timer("Recreating table"):
            self.recreate_matview()

        with Timer("Inserting data into table"):
            self.insert_matview_data(chunk_count)

        with Timer("Creating table constraints"):
            self.create_constraints(create_temp_constraints)

        with Timer("Creating table indexes"):
            self.create_indexes(create_temp_indexes, index_concurrency)

        with Timer("Swapping Tables/Indexes"):
            self.swap_matviews(rename_indexes, rename_constraints)

        if not options["keep_old_data"]:
            with Timer("Clearing old table"):
                self.remove_old_data(chunk_count)

        if not options["keep_matview_data"]:
            with Timer("Emptying Matviews"):
                self.empty_matviews()

        with Timer("Granting Table Permissions"):
            self.grant_matview_permissions()

    def read_constraint_definitions(self):
        indexes_sql = (self.matview_dir / "componentized" / "transaction_search__constraints.sql").read_text()

        with connection.cursor() as cursor:
            cursor.execute(indexes_sql)
            rows = cursor.fetchall()

        create_temp_constraints = []
        rename_constraints_old = []
        rename_constraints_temp = []
        for row in rows:
            constraint_name = row[0]
            constraint_name_temp = constraint_name + "_temp"
            constraint_name_old = constraint_name + "_old"

            create_temp_constraints.append(f"ALTER TABLE public.{TABLE_NAME}_temp ADD CONSTRAINT {constraint_name_temp} {row[1]};")
            rename_constraints_old.append(f"ALTER TABLE public.{TABLE_NAME}_old RENAME CONSTRAINT {constraint_name} TO {constraint_name_old};")
            rename_constraints_temp.append(f"ALTER TABLE public.{TABLE_NAME} RENAME CONSTRAINT {constraint_name_temp} TO {constraint_name};")

            self.constraint_names.append(constraint_name)

        return create_temp_constraints, rename_constraints_old + rename_constraints_temp

    def read_index_definitions(self):
        indexes_sql = (self.matview_dir / "componentized" / "transaction_search__indexes.sql").read_text()

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
                create_temp_index_sql = create_temp_index_sql.replace(f"public.{TABLE_NAME}", f"public.{TABLE_NAME}_temp")
                create_temp_indexes.append(create_temp_index_sql)

                rename_indexes_old.append(f"ALTER INDEX IF EXISTS {index_name} RENAME TO {index_name_old};")
                rename_indexes_temp.append(f"ALTER INDEX IF EXISTS {index_name_temp} RENAME TO {index_name};")

        return create_temp_indexes, rename_indexes_old + rename_indexes_temp

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

    def create_constraints(self, constraint_definitions):

        constraint_sql = "\n".join(constraint_definitions)

        logger.debug(constraint_sql)

        with connection.cursor() as cursor:
            cursor.execute(constraint_sql)

    def create_indexes(self, index_definitions, index_concurrency):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.index_with_concurrency(index_definitions, index_concurrency))
        loop.close()

    async def index_with_concurrency(self, index_definitions, index_concurrency):
        semaphore = asyncio.Semaphore(index_concurrency)
        tasks = []

        async def create_with_sem(sql, index):
            async with semaphore:
                return await async_run_creates(sql, wrapper=Timer(f"Creating Index {index}"),)

        for i, sql in enumerate(index_definitions):
            logger.info(f"Creating future for index: {i} - SQL: {sql}")
            tasks.append(create_with_sem(sql, i))

        return await asyncio.gather(*tasks)

    @transaction.atomic
    def swap_matviews(self, rename_indexes, rename_constraints):

        swap_sql = (self.matview_dir / "componentized" / "transaction_search__renames.sql").read_text()

        swap_sql += "\n".join(rename_indexes + rename_constraints)

        logger.debug(swap_sql)

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
