import logging
from django import db
from django.core.management.base import BaseCommand

from usaspending_api.etl.management.commands.combine_transaction_search_chunks import Command as CTSC_Command
from usaspending_api.database_scripts.matview_generator.chunked_matview_sql_generator import (
    make_copy_indexes,
    make_copy_constraints,
)

logger = logging.getLogger("script")


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
            CTSC_Command().create_indexes(copy_index_sql, index_concurrency)
            logger.info(f"Indexes from {source_table} copied over.")
