import logging
import re
import asyncio
from django import db
from django.core.management.base import BaseCommand

from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.database_scripts.matview_generator.chunked_matview_sql_generator import (
    make_read_constraints,
    make_read_indexes,
)

logger = logging.getLogger("script")


def create_indexes(self, index_definitions, index_concurrency):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(self.index_with_concurrency(index_definitions, index_concurrency))
    loop.close()


async def index_with_concurrency(self, index_definitions, index_concurrency):
    semaphore = asyncio.Semaphore(index_concurrency)
    tasks = []

    async def create_with_sem(sql, index):
        async with semaphore:
            return await async_run_creates(
                sql,
                wrapper=Timer(f"Creating Index {index}"),
            )

    for i, sql in enumerate(index_definitions):
        logger.info(f"Creating future for index: {i} - SQL: {sql}")
        tasks.append(create_with_sem(sql, i))

    return await asyncio.gather(*tasks)


def make_copy_constraints(cursor, source_table, dest_table, drop_foreign_keys=False):
    # read the existing indexes
    cursor.execute(make_read_constraints(source_table)[0])
    src_constrs = dictfetchall(cursor)

    # build the destination index sql
    dest_constr_sql = []
    for constr_dict in src_constrs:
        create_constr_name = constr_dict["conname"]
        create_constr_content = constr_dict["pg_get_constraintdef"]
        if "FOREIGN KEY" in create_constr_content and drop_foreign_keys:
            continue
        dest_constr_sql.append(
            f"ALTER TABLE {dest_table} ADD CONSTRAINT {create_constr_name}_temp" f" {create_constr_content}"
        )
    return dest_constr_sql


def make_copy_indexes(cursor, source_table, dest_table):
    # read the existing indexes of source table
    cursor.execute(make_read_indexes(source_table)[0])
    src_indexes = dictfetchall(cursor)

    # reading the existing indexes of destination table (to not duplicate anything)
    cursor.execute(make_read_indexes(dest_table)[0])
    dest_indexes = [ix_dict["indexname"] for ix_dict in dictfetchall(cursor)]

    # build the destination index sql
    dest_ix_sql = []
    for ix_dict in src_indexes:
        ix_name = ix_dict["indexname"]
        dest_ix_name = f"{ix_name}_temp"
        if dest_ix_name in dest_indexes:
            logger.info(f"Index {dest_ix_name} already in {dest_table}. Skipping.")
            continue

        create_ix_sql = ix_dict["indexdef"]
        ix_regex = r"CREATE\s.*INDEX\s\S+\sON\s(\S+)\s.*"
        # this *should* match source_table, but can get funky with/without the schema included and regex
        # for example, a table 'x' in the public schema could be provided and the string will include `public.x'
        src_table = re.findall(ix_regex, create_ix_sql)[0]
        create_ix_sql = create_ix_sql.replace(ix_name, dest_ix_name)
        create_ix_sql = create_ix_sql.replace(src_table, dest_table)
        dest_ix_sql.append(create_ix_sql)
    return dest_ix_sql


class Command(BaseCommand):

    help = """
    This command simply copies the constraints and indexes from one table to another in postgres
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--source-table",
            type=str,
            required=True,
            help="The source postgres table to read the metadata",
        )
        parser.add_argument(
            "--dest-table",
            type=str,
            required=True,
            help="The source postgres table to copy the metadata to",
        )
        parser.add_argument(
            "--skip-constraints",
            action="store_true",
            help="If provided, skips copying over the constraints to the destination table.",
        )
        parser.add_argument(
            "--skip-indexes",
            action="store_true",
            help="If provided, skips copying over the indexes to the destination table.",
        )
        parser.add_argument(
            "--max-parallel-maintenance-workers",
            type=int,
            required=False,
            help="Postgres session setting for max parallel workers for creating indexes."
            " Only applicable if copy-indexes is provided.",
        )
        parser.add_argument(
            "--maintenance-work-mem",
            type=int,
            required=False,
            help="Postgres session setting for max memory to use for creating indexes (in GBs)."
            "Only applicable if copy-indexes is provided.",
        )
        parser.add_argument(
            "--index-concurrency",
            type=int,
            default=5,
            help="Concurrency limit for index creation. Only applicable if copy-indexes is provided.",
        )

    def handle(self, *args, **options):
        # Resolve Parameters
        source_table = options["source_table"]
        dest_table = options["dest_table"]
        skip_constraints = options["skip_constraints"]
        skip_indexes = options["skip_indexes"]
        max_parallel_maintenance_workers = options["max_parallel_maintenance_workers"]
        maintenance_work_mem = options["maintenance_work_mem"]
        index_concurrency = options["index_concurrency"]

        logger.info(f"Copying metadata from table {source_table} to table {dest_table}.")

        copy_index_sql = None
        with db.connection.cursor() as cursor:
            if not skip_constraints:
                logger.info(f"Copying constraints over from {source_table}")
                copy_constraint_sql = make_copy_constraints(cursor, source_table, dest_table, drop_foreign_keys=True)
                if copy_constraint_sql:
                    cursor.execute("; ".join(copy_constraint_sql))
                logger.info(f"Constraints from {source_table} copied over")

            if not skip_indexes:
                # Ensuring we're using the max cores available when generating indexes
                if max_parallel_maintenance_workers:
                    logger.info(f"Setting max_parallel_maintenance_workers to {max_parallel_maintenance_workers}")
                    cursor.execute(f"SET max_parallel_maintenance_workers = {max_parallel_maintenance_workers}")
                if maintenance_work_mem:
                    logger.info(f"Setting maintenance_work_mem to '{maintenance_work_mem}GB'")
                    cursor.execute(f"SET maintenance_work_mem = '{maintenance_work_mem}GB'")
                copy_index_sql = make_copy_indexes(cursor, source_table, dest_table)
        if copy_index_sql:
            logger.info(f"Copying indexes over from {source_table}.")
            create_indexes(copy_index_sql, index_concurrency)
            logger.info(f"Indexes from {source_table} copied over.")
