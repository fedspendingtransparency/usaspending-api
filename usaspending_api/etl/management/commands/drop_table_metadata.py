"""
Django Management Command module that is similar to copy_table_metadata.py, but used for the use case of DROPPING
table metadata (constraints and indexes so far) for the specified table. Also supports partition metadata.
"""

import logging
from pprint import pformat
from django.db import connection
from django.core.management.base import BaseCommand

from usaspending_api.common.helpers.sql_helpers import get_parent_partitioned_table
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.commands.copy_table_metadata import make_read_indexes, make_read_constraints

logger = logging.getLogger("script")


def drop_table_metadata(target_table, is_potential_partition=False, keep_constraints=False, keep_indexes=False):
    table_name = target_table
    if "." in target_table:
        schema_name, table_name = target_table[: target_table.index(".")], target_table[target_table.index(".") + 1 :]
    else:
        schema_name = "public"
    with connection.cursor() as cursor:
        # Check if the table exists first
        cursor.execute(
            f"""
            SELECT EXISTS (SELECT FROM pg_tables
            WHERE schemaname = '{schema_name}' AND tablename = '{table_name}');
        """
        )
        exists_results = cursor.fetchone()
        if not exists_results[0]:
            logger.warning(f"Target table '{target_table}' does not exist. Not doing anything.")
            return

        if is_potential_partition:
            # Check that it is attached to a parent
            parent_partitioned_table = get_parent_partitioned_table(f"{schema_name}.{table_name}", cursor)
            if parent_partitioned_table:
                logger.info(
                    f"First detaching table '{target_table}' (a partition) from parent table "
                    f"'{parent_partitioned_table}' before dropping metadata on it."
                )
                cursor.execute(f"ALTER TABLE {parent_partitioned_table} DETACH PARTITION {target_table}")
            else:
                logger.info(f"'{target_table}' is not currently attached to a parent as a partition")

        if not keep_constraints:
            drop_constr_sql = make_drop_constraints(cursor, target_table)
            if drop_constr_sql:
                logger.info(f"About to run drop constraints SQL:\n{pformat(drop_constr_sql)}")
                cursor.execute("; ".join(drop_constr_sql))
            else:
                logger.info(f"No constraints discovered to be dropped on '{target_table}'")
        if not keep_indexes:
            drop_idx_sql = make_drop_indexes(cursor, target_table)
            if drop_idx_sql:
                logger.info(f"About to run drop indexes SQL:\n{pformat(drop_idx_sql)}")
                cursor.execute("; ".join(drop_idx_sql))
            else:
                logger.info(f"No indexes discovered to be dropped on '{target_table}'")


def make_drop_constraints(cursor, target_table, drop_foreign_keys=False):
    # read the existing constraints
    cursor.execute(make_read_constraints(target_table)[0])
    src_constrs = dictfetchall(cursor)

    # build the destination constraint sql
    drop_constr_sql = []
    for src_constr_dict in src_constrs:
        src_constr_name = src_constr_dict["conname"]
        create_constr_content = src_constr_dict["pg_get_constraintdef"]
        if "FOREIGN KEY" in create_constr_content and drop_foreign_keys:
            continue
        drop_constr_sql.append(f"ALTER TABLE {target_table} DROP CONSTRAINT IF EXISTS {src_constr_name}")
    return drop_constr_sql


def make_drop_indexes(cursor, target_table):
    if "." in target_table:
        schema_name, _ = target_table[: target_table.index(".")], target_table[target_table.index(".") + 1 :]
    else:
        schema_name = "public"

    # read the existing indexes of source table
    cursor.execute(make_read_indexes(target_table)[0])
    src_indexes = dictfetchall(cursor)

    # build the drop index sql
    drop_ix_sql = []
    for src_ix_dict in src_indexes:
        src_ix_name = src_ix_dict["indexname"]
        drop_ix_sql.append(f"DROP INDEX IF EXISTS {schema_name}.{src_ix_name}")
    return drop_ix_sql


class Command(BaseCommand):

    help = """
        Django Management Command module that is similar to copy_table_metadata.py, but used for the use case of
        DROPPING table metadata (constraints and indexes so far) for the specified table. Also supports partition
        metadata.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--table",
            type=str,
            required=True,
            help="The target postgres table on which to drop metadata",
        )
        parser.add_argument(
            "--is-potential-partition",
            action="store_true",
            help="Check if the target table is a partition of a parent table and deal with it if so.",
        )
        parser.add_argument(
            "--keep-constraints",
            action="store_true",
            help="If provided, skips dropping constraints (keeps them).",
        )
        parser.add_argument(
            "--keep-indexes",
            action="store_true",
            help="If provided, skips dropping indexes (keeps them).",
        )

    def handle(self, *args, **options):
        # Resolve Parameters
        table = options["table"]
        keep_constraints = options["keep_constraints"]
        keep_indexes = options["keep_indexes"]
        is_potential_partition = options["is_potential_partition"]

        logger.info(f"Dropping metadata from table {table}.")

        drop_table_metadata(
            target_table=table,
            is_potential_partition=is_potential_partition,
            keep_constraints=keep_constraints,
            keep_indexes=keep_indexes,
        )
