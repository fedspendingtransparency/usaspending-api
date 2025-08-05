import json
import logging
import re

from datetime import datetime
from pprint import pformat
from typing import List, OrderedDict, Optional
from django.core.management import BaseCommand
from django.db import connection, ProgrammingError, transaction

from usaspending_api.common.helpers.sql_helpers import (
    ordered_dictionary_fetcher,
    is_table_partitioned,
    get_parent_partitioned_table,
)

logger = logging.getLogger("script")


class Command(BaseCommand):
    help = """
    This command is used to swap two tables. It uses a base table name and then discriminating suffixes (possibly
    empty string) for the tables to be swapped (defaults to swapping <table>_temp in for <table>).
    Validation is run against the new table to ensure that after the swap is complete all of the indexes, constraints,
    columns, and table name will be the same.

    NOTE: This entire process IS NOT ATOMIC (only the final swap of tables)!
    This choice was made to prevent potential deadlock scenarios since the swapping / renaming of indexes, constraints,
    and tables should only take milliseconds. If this command fails then any cleanup will have to be done manually.

    Current API and Download functionality is not affected until the step that actually renames the old table. That
    change will take an ACCESS EXCLUSIVE LOCK and any future queries following it will hit the new table. On a Primary
    database where typical query performance is under 1 minute this could cause some queries to take longer as they wait
    on the LOCK to be released, depending on what the lock is waiting on. For a Replica database this will cause some
    queries to cancel if they are blocking the ACCESS EXCLUSIVE LOCK for too long and could impede replication.

    At the time of the actual DROP TABLE command there should be no activity present on the old table since the
    ALTER TABLE for the rename would have blocked all activity and routed to the new table following the rename.
    """

    # Values are set in the beginning of "handle()"
    curr_schema_name: str
    curr_table_name: str
    dest_suffix: str  # suffix of the ("curr") table to be replaced
    temp_schema_name: str
    temp_table_name: str
    source_suffix: str  # suffix of the ("temp") table to be swapped-in
    is_temp_table_partitioned: bool
    temp_table_parent_partitioned_table: Optional[str]
    is_curr_table_partitioned: bool
    curr_table_parent_partitioned_table: Optional[str]
    old_suffix: str  # suffix for old tables/objects
    dep_views: List[OrderedDict]

    # Query values are populated as they are run during validation and saved for re-use
    query_result_lookup = {
        "temp_table_constraints": [],
        "curr_table_constraints": [],
        "temp_table_indexes": [],
        "curr_table_indexes": [],
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--table",
            type=str,
            help="The active Postgres table to swap with another containing the same name with '_temp' appended",
        )
        parser.add_argument(
            "--keep-old-data",
            action="store_true",
            default=False,
            help="Indicates whether or not to drop old table at the end of the command",
        )
        parser.add_argument(
            "--allow-foreign-key",
            action="store_true",
            default=False,
            help="A guard is enabled / disabled depending on the value of this flag. When 'FALSE' Foreign Keys are not"
            " allowed and both the active and new table are searched for any Foreign Keys before proceeding."
            " It is advised to not allow Foreign Key constraints since they can cause deadlock.",
        )
        parser.add_argument(
            "--source-suffix",
            type=str,
            required=False,
            nargs="?",
            const="",  # value if flag provided but no arg values given
            default="temp",
            help="The assumed suffix on the name of the source table to be swapped in, and all its objects (like "
            "indexes, constraints).",
        )
        parser.add_argument(
            "--dest-suffix",
            type=str,
            required=False,
            nargs="?",
            const="",  # value if flag provided but no arg values given
            default="",
            help="The assumed suffix on the name of the table to be replaced, and all its objects (like "
            "indexes, constraints).",
        )
        parser.add_argument(
            "--undo",
            action="store_true",
            default=False,
            help="Reverse the most recent swap (that it can find) on the given --table. Only works if some old data "
            "was saved using --keep-old-data when the swap or a prior swap was run. If you know specifically "
            "which old table you want to swap, just run a regular command instead, supplying --source-suffix and "
            "possibly --dest-suffix",
        )

    def handle(self, *args, **options):
        is_undo = options["undo"]
        if is_undo:
            old_table = self.get_most_recent_old_table(options["table"])
            old_suffix = re.sub(rf"{options['table']}_", "", old_table)
            logger.info(
                f"Overwriting --source-suffix value of '{options['source_suffix']}' with '{old_suffix}' for undo."
            )
            options["source_suffix"] = old_suffix

        # init object attributes
        self.source_suffix = f"_{options['source_suffix']}" if options["source_suffix"] else ""
        self.dest_suffix = f"_{options['dest_suffix']}" if options["dest_suffix"] else ""
        self.curr_table_name = f"{options['table']}{self.dest_suffix}"
        self.temp_table_name = f"{self.curr_table_name}{self.source_suffix}"
        self.old_suffix = f"_old{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

        with connection.cursor() as cursor:
            # Go ahead and retrieve the schema for both table; used for some validation checks and the final swap
            cursor.execute(
                f"""
                SELECT tablename, schemaname
                FROM pg_tables
                WHERE tablename = '{self.curr_table_name}' OR tablename = '{self.temp_table_name}'
                UNION
                SELECT matviewname AS tablename, schemaname
                FROM pg_matviews
                WHERE matviewname = '{self.curr_table_name}' OR matviewname = '{self.temp_table_name}'
            """
            )
            schemas_lookup = {table: schema for (table, schema) in cursor.fetchall()}
            self.curr_schema_name = schemas_lookup.get(self.curr_table_name)
            self.temp_schema_name = schemas_lookup.get(self.temp_table_name)
            self.dep_views = self.dependent_views(cursor)

            self.validate_tables(cursor)

            logger.info(
                f"Starting swap procedures for table {self.temp_schema_name}.{self.temp_table_name} "
                f"into {self.curr_schema_name}.{self.curr_table_name}"
            )

            # Check if we're dealing with a partitioned table, or a partition of a partitioned table
            self.is_temp_table_partitioned = is_table_partitioned(
                table=f"{self.temp_schema_name}.{self.temp_table_name}", cursor=cursor
            )
            self.temp_table_parent_partitioned_table = get_parent_partitioned_table(
                table=f"{self.temp_schema_name}.{self.temp_table_name}", cursor=cursor
            )
            self.is_curr_table_partitioned = is_table_partitioned(
                table=f"{self.curr_schema_name}.{self.curr_table_name}", cursor=cursor
            )
            self.curr_table_parent_partitioned_table = get_parent_partitioned_table(
                table=f"{self.curr_schema_name}.{self.curr_table_name}", cursor=cursor
            )

            # Make sure old and new are like-like in all ways
            self.validate_state_of_tables(cursor, options)

            # Get constraints and indexes named correctly before the table is renamed
            self.swap_constraints_sql(cursor)
            self.swap_index_sql(cursor)

            # New temp views are setup to point to the temp table, and will swap-in with the table
            self.create_new_views(cursor)

            # Prep stats and privs on the staged temp table before swapping (renaming)
            self.analyze_temp_table(cursor)
            self.grant_privs_on_temp_table(cursor)

            # Do the swaps via renames
            self.swap_dependent_views(cursor)
            self.swap_table(cursor)

            if not options["keep_old_data"]:
                self.drop_old_views(cursor)
                if self.curr_table_parent_partitioned_table:
                    logger.info(
                        f"'{self.curr_table_name}' is a partition of '{self.curr_table_parent_partitioned_table}' and "
                        f"will not be dropped. Swapped-out partitions are not deleted after the swap due to the risk "
                        f"of their live parent partitioned table still receiving traffic while having one of its "
                        f"partitions deleted. If '{self.curr_table_parent_partitioned_table}' is also being swapped, "
                        f"then this partition will be deleted when it is deleted. Otherwise delete "
                        f"'{self.curr_table_name}{self.old_suffix}' manually."
                    )
                else:
                    self.drop_old_table_sql(cursor)

    def cleanup_old_data(self, cursor, old_suffix=None):
        """
        Run SQL to clean up any old data that could conflict with the swap.
        We first try to delete the "table" as though it is a table. In the event
        that this fails we ignore the error and attempt to then drop the "table"
        as though it is a Materialized View, displaying both the current and
        previous errors if this should fail again.
        """
        if not old_suffix:
            old_suffix = self.old_suffix
        logger.info(f"Dropping table {self.curr_table_name}{old_suffix} if it exists")
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {self.curr_table_name}{old_suffix} CASCADE;")
            return
        except ProgrammingError as e:
            error_log = str(e)
        try:
            cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {self.curr_table_name}{old_suffix} CASCADE;")
        except ProgrammingError as e:
            raise f"First Error:\n{error_log}\nSecond Error: {str(e)}"

    def swap_constraints_sql(self, cursor):
        logger.info("Renaming constraints of the new and old tables")
        temp_constraints = self.query_result_lookup["temp_table_constraints"]
        curr_constraints = self.query_result_lookup["curr_table_constraints"]
        rename_sql = []
        sql_template = "ALTER TABLE {table_name} RENAME CONSTRAINT {old_constraint_name} TO {new_constraint_name};"
        for val in curr_constraints:
            old_name = val["constraint_name"]
            new_name = f"{old_name}{self.old_suffix}"
            rename_sql.append(
                sql_template.format(
                    table_name=self.curr_table_name, old_constraint_name=old_name, new_constraint_name=new_name
                )
            )
        for val in temp_constraints:
            old_name = val["constraint_name"]
            new_name = re.sub(
                rf"^(.*){self.source_suffix}$", rf"\g<1>{self.dest_suffix}", old_name, count=1, flags=re.I
            )
            rename_sql.append(
                sql_template.format(
                    table_name=self.temp_table_name, old_constraint_name=old_name, new_constraint_name=new_name
                )
            )

        if rename_sql:
            cursor.execute("\n".join(rename_sql))

    def swap_index_sql(self, cursor):
        logger.info("Renaming indexes of the new and old tables")

        # Some Postgres constraints (UNIQUE, PRIMARY, and EXCLUDE) are updated along with the index which means
        # there is no need to rename the index following the constraint
        temp_constraint_names = [val["constraint_name"] for val in self.query_result_lookup["temp_table_constraints"]]
        temp_indexes = [
            val
            for val in self.query_result_lookup["temp_table_indexes"]
            if val["indexname"] not in temp_constraint_names
        ]
        curr_constraint_names = [val["constraint_name"] for val in self.query_result_lookup["curr_table_constraints"]]
        curr_indexes = [
            val
            for val in self.query_result_lookup["curr_table_indexes"]
            if val["indexname"] not in curr_constraint_names
        ]
        self.query_result_lookup["temp_table_indexes"] = temp_indexes
        self.query_result_lookup["curr_table_indexes"] = curr_indexes

        rename_sql = []
        sql_template = "ALTER INDEX {old_index_name} RENAME TO {new_index_name};"
        for val in curr_indexes:
            old_name = val["indexname"]
            new_name = f"{old_name}{self.old_suffix}"
            rename_sql.append(sql_template.format(old_index_name=old_name, new_index_name=new_name))
        for val in temp_indexes:
            old_name = val["indexname"]
            new_name = re.sub(
                rf"^(.*){self.source_suffix}$", rf"\g<1>{self.dest_suffix}", old_name, count=1, flags=re.I
            )
            rename_sql.append(sql_template.format(old_index_name=old_name, new_index_name=new_name))

        if rename_sql:
            cursor.execute("\n".join(rename_sql))

    def swap_dependent_views(self, cursor):
        sql_view_template = "ALTER {mv_s}VIEW {old_view_schema}.{old_view_name} RENAME TO {new_view_name};"
        for dep_view in self.dep_views:
            # Coordinate the schema move and view renames in 1 atomic operation
            # But don't have the transaction span more than 1 view at a time to avoid any potential deadlocks with
            # ongoing queries
            with transaction.atomic():
                mv_s = "MATERIALIZED " if dep_view["is_matview"] else ""
                view_rename_sql = [
                    # 1. move temp suffixed view to the target schema
                    f"ALTER {mv_s}VIEW {self.temp_schema_name}.{dep_view['dep_view_name']}{self.source_suffix} "
                    f"SET SCHEMA {dep_view['dep_view_schema']};",
                    # 2. rename active view in target schema to have old suffix
                    sql_view_template.format(
                        mv_s=mv_s,
                        old_view_schema=dep_view["dep_view_schema"],
                        old_view_name=dep_view["dep_view_name"],
                        new_view_name=f"{dep_view['dep_view_name']}{self.old_suffix}",
                    ),
                    # 3. rename temp view (now moved into target schema) to active name
                    sql_view_template.format(
                        mv_s=mv_s,
                        old_view_schema=dep_view["dep_view_schema"],
                        old_view_name=f"{dep_view['dep_view_name']}{self.source_suffix}",
                        new_view_name=dep_view["dep_view_name"],
                    ),
                ]
                logger.info(f"Running view swap SQL in an atomic transaction:\n{pformat(view_rename_sql)}")
                cursor.execute("\n".join(view_rename_sql))

    @transaction.atomic
    def swap_table(self, cursor):
        logger.info("Renaming the new and old table")
        sql_table_template = "ALTER TABLE {old_table_name} RENAME TO {new_table_name};"

        table_rename_sql = [
            f"ALTER TABLE {self.temp_table_name} SET SCHEMA {self.curr_schema_name};",
            sql_table_template.format(
                old_table_name=self.curr_table_name, new_table_name=f"{self.curr_table_name}{self.old_suffix}"
            ),
            sql_table_template.format(old_table_name=self.temp_table_name, new_table_name=f"{self.curr_table_name}"),
        ]
        logger.info(f"Running table swap SQL in an atomic transaction:\n{pformat(table_rename_sql)}")
        cursor.execute("\n".join(table_rename_sql))

    def drop_old_table_sql(self, cursor):
        self.drop_old_table_metadata(cursor)
        self.cleanup_old_data(cursor)

    def drop_old_table_metadata(self, cursor):
        # Instead of using CASCADE, all old constraints and indexes are dropped manually
        logger.info("Manually dropping the old table metadata (constraints and indexes")
        drop_sql = []
        indexes = self.query_result_lookup["curr_table_indexes"]
        constraints = self.query_result_lookup["curr_table_constraints"]
        for val in constraints:
            name = f"{val['constraint_name']}{self.old_suffix}"
            drop_sql.append(f"ALTER TABLE {self.curr_table_name}{self.old_suffix} DROP CONSTRAINT {name};")
        for val in indexes:
            name = f"{val['indexname']}{self.old_suffix}"
            drop_sql.append(f"DROP INDEX {name};")
        if drop_sql:
            cursor.execute("\n".join(drop_sql))

    def drop_old_views(self, cursor):
        for dep_view in self.dep_views:
            mv_s = "MATERIALIZED " if dep_view["is_matview"] else ""
            logger.info(f"Dropping old dependent view: {dep_view['dep_view_fullname']}{self.old_suffix}")
            cursor.execute(f"DROP {mv_s}VIEW {dep_view['dep_view_fullname']}{self.old_suffix};")

    def analyze_temp_table(self, cursor):
        if not self.is_temp_table_partitioned:
            logger.info(f"Running ANALYZE VERBOSE {self.temp_table_name}")
            cursor.execute(f"ANALYZE VERBOSE {self.temp_table_name}")
        else:
            logger.info(
                f"Skipping ANALYZE VERBOSE {self.temp_table_name} because table is partitioned and ANALYZE "
                f"should already be run against each partition, which should be swapped before it."
            )

    def grant_privs_on_temp_table(self, cursor):
        logger.info(f"GRANT SELECT ON {self.temp_table_name} TO readonly")
        cursor.execute(f"GRANT SELECT ON {self.temp_table_name} TO readonly")

    def validate_tables(self, cursor):
        logger.info("Verifying that the current table exists")
        cursor.execute(
            f"""
            SELECT schemaname, tablename FROM pg_tables WHERE tablename = '{self.curr_table_name}'
            UNION
            SELECT schemaname, matviewname AS tablename FROM pg_matviews WHERE matviewname = '{self.curr_table_name}'
        """
        )
        curr_tables = cursor.fetchall()
        if len(curr_tables) == 0:
            logger.error(f"There are no tables matching: {self.curr_table_name}")
            raise SystemExit(1)

        logger.info("Verifying that the temp table exists")
        cursor.execute(
            f"""
            SELECT schemaname, tablename FROM pg_tables WHERE tablename = '{self.temp_table_name}'
            UNION
            SELECT schemaname, matviewname AS tablename FROM pg_matviews WHERE matviewname = '{self.temp_table_name}'
        """
        )
        temp_tables = cursor.fetchall()
        if len(temp_tables) == 0:
            logger.error(f"There are no tables matching: {self.temp_table_name}")
            raise SystemExit(1)

        logger.info("Verifying duplicate tables don't exist across schemas")
        cursor.execute(
            f"""
            WITH matched_tables AS (
                SELECT schemaname, tablename FROM pg_tables WHERE tablename IN ('{self.curr_table_name}', '{self.temp_table_name}', '{self.curr_table_name}_old', '{self.curr_table_name}{self.old_suffix}')
                UNION
                SELECT schemaname, matviewname AS tablename FROM pg_matviews WHERE matviewname IN ('{self.curr_table_name}', '{self.temp_table_name}', '{self.curr_table_name}_old', '{self.curr_table_name}{self.old_suffix}')
            )
            SELECT tablename, COUNT(*) AS schema_count
            FROM matched_tables
            GROUP BY tablename
        """
        )
        table_count = cursor.fetchall()
        for table_name, count in table_count:
            if count > 1:
                logger.error(f"There are currently duplicate tables for '{table_name}' in different schemas")
                raise SystemExit(1)

    def validate_indexes(self, cursor):
        cursor.execute(f"SELECT * FROM pg_indexes WHERE tablename = '{self.temp_table_name}' ORDER BY indexname")
        temp_indexes = ordered_dictionary_fetcher(cursor)
        cursor.execute(f"SELECT * FROM pg_indexes WHERE tablename = '{self.curr_table_name}' ORDER BY indexname")
        curr_indexes = ordered_dictionary_fetcher(cursor)

        if self.is_temp_table_partitioned and len(temp_indexes) == 0:
            # Assuming the parent partitioned table is just a shell and indexes are handled on child partitions
            logger.info(
                "Source temp table is a partitioned table with no indexes. Assuming that all/any "
                "indexes are managed on the child partitions and skipping validation."
            )
            return

        self.query_result_lookup["temp_table_indexes"] = temp_indexes
        self.query_result_lookup["curr_table_indexes"] = curr_indexes

        logger.info(
            f"Verifying that the indexes are the same except for suffixes in their name "
            f"(source_suffix='{self.source_suffix}', dest_suffix='{self.dest_suffix}')"
        )

        temp_index_specs = {}
        for val in temp_indexes:
            tindexname = re.sub(rf"{self.source_suffix}$", self.dest_suffix, val["indexname"], count=1)
            tindexdef = re.sub(
                # Name ending in source suffix, that may be quoted (due to special chars like $), that are followed
                # by whitespace or the end of the line
                rf"(.*?){self.source_suffix}(\"?\s+|\"?$)",
                rf"\g<1>{self.dest_suffix}\g<2>",
                val["indexdef"],
                flags=re.I,
            )
            if self.is_temp_table_partitioned:
                tindexdef = re.sub("ON ONLY", "ON", tindexdef, flags=re.I)
            temp_index_specs[tindexname] = tindexdef

        curr_index_specs = {}
        for val in curr_indexes:
            cindexname = val["indexname"]
            cindexdef = re.sub(rf"{self.curr_schema_name}\.", f"{self.temp_schema_name}.", val["indexdef"])
            if self.is_curr_table_partitioned:
                cindexdef = re.sub("ON ONLY", "ON", cindexdef, flags=re.I)
            curr_index_specs[cindexname] = cindexdef

        differences = []
        for idx, idx_def in temp_index_specs.items():
            if idx not in curr_index_specs:
                differences.append(
                    {"temp_index_name": idx, "curr_index_name": None, "temp_index_def": idx_def, "curr_index_def": None}
                )
                continue
            if idx_def not in curr_index_specs.values():
                differences.append(
                    {
                        "temp_index_name": idx,
                        "curr_index_name": idx,
                        "temp_index_def": idx_def,
                        "curr_index_def": curr_index_specs[idx],
                    }
                )
        # From other direction
        for idx, idx_def in curr_index_specs.items():
            if idx not in temp_index_specs:
                differences.append(
                    {"temp_index_name": None, "curr_index_name": idx, "temp_index_def": None, "curr_index_def": idx_def}
                )
                continue
            if idx_def not in temp_index_specs.values():
                diff = {
                    "temp_index_name": idx,
                    "curr_index_name": idx,
                    "temp_index_def": temp_index_specs[idx],
                    "curr_index_def": idx_def,
                }
                if diff not in differences:
                    differences.append(diff)

        if differences:
            logger.error(
                f"Indexes missing or differences found among the {len(curr_indexes)} current indexes "
                f"in {self.curr_table_name} and the {len(temp_indexes)} indexes of {self.temp_table_name} "
                f"table to be swapped in:\n{json.dumps(differences, indent=4)}"
            )
            raise SystemExit(1)

    def dependent_views(self, cursor):
        """Detects views that are dependent on the table to be swapped."""
        detect_dep_view_sql = f"""
            SELECT
                dependent_ns.nspname AS dep_view_schema,
                dependent_view.relname AS dep_view_name,
                CONCAT(dependent_ns.nspname, '.', dependent_view.relname) as dep_view_fullname,
                RTRIM(pg_get_viewdef(CONCAT(dependent_ns.nspname, '.', dependent_view.relname), TRUE), ';') AS dep_view_sql,
                dependent_view.relkind = 'm' AS is_matview
            FROM pg_depend
            JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
            JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
            JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
            JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
                AND pg_depend.refobjsubid = pg_attribute.attnum
            JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
            JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
            WHERE
                source_ns.nspname = '{self.curr_schema_name}'
                AND source_table.relname = '{self.curr_table_name}'
            GROUP BY
                CONCAT(source_ns.nspname, '.', source_table.relname),
                dependent_ns.nspname,
                dependent_view.relname,
                CONCAT(dependent_ns.nspname, '.', dependent_view.relname),
                dependent_view.relkind;
        """
        cursor.execute(detect_dep_view_sql)
        dep_views = ordered_dictionary_fetcher(cursor)
        return dep_views

    def create_new_views(self, cursor):
        for dep_view in self.dep_views:
            # Note: Despite pointing at the new temp table at first,
            #       the recreated views will automatically update and stay pointed to it in the swap via postgres
            logger.info(f"Recreating dependent view: {dep_view['dep_view_fullname']}{self.source_suffix}")
            dep_view_sql = dep_view["dep_view_sql"].replace(self.curr_table_name, self.temp_table_name)
            mv_s = "MATERIALIZED " if dep_view["is_matview"] else ""
            cursor.execute(
                f"DROP {mv_s}VIEW IF EXISTS {self.temp_schema_name}.{dep_view['dep_view_name']}{self.source_suffix};"
            )
            cursor.execute(
                f"CREATE {mv_s}VIEW {self.temp_schema_name}.{dep_view['dep_view_name']}{self.source_suffix} "
                f"AS ({dep_view_sql});"
            )

    def validate_foreign_keys(self, cursor):
        logger.info("Verifying that Foreign Key constraints are not found")
        cursor.execute(
            f"SELECT * FROM information_schema.table_constraints"
            f" WHERE table_name IN ('{self.temp_table_name}', '{self.curr_table_name}')"
            f" AND constraint_type = 'FOREIGN KEY'"
        )
        constraints = cursor.fetchall()
        if len(constraints) > 0:
            logger.error(
                f"Foreign Key constraints are not allowed on '{self.temp_table_name}' or '{self.curr_table_name}'."
                " It is advised to not allow Foreign Key constraints on swapped tables to avoid potential deadlock."
                " However, if needed they can be allowed with the `--allow-foreign-key` flag."
            )
            raise SystemExit(1)

    def validate_constraints(self, cursor):
        logger.info("Verifying that the same number of constraints exist for the old and new table")
        cursor.execute(
            f"SELECT "
            f"     table_constraints.constraint_name,"
            f"     table_constraints.constraint_type,"
            f"     check_constraints.check_clause,"
            f"     referential_constraints.unique_constraint_name,"
            f"     COALESCE(columns.is_nullable, 'YES')::BOOLEAN AS is_nullable"
            f" FROM information_schema.table_constraints"
            f" LEFT OUTER JOIN information_schema.check_constraints ON ("
            f"     table_constraints.constraint_name = check_constraints.constraint_name)"
            f" LEFT OUTER JOIN information_schema.referential_constraints ON ("
            f"     table_constraints.constraint_name = referential_constraints.constraint_name)"
            f" LEFT OUTER JOIN information_schema.columns ON ("
            f"     table_constraints.table_name = columns.table_name"
            f"     AND check_constraints.check_clause = CONCAT(columns.column_name, ' IS NOT NULL'))"
            f" WHERE table_constraints.table_name = '{self.temp_table_name}'"
        )
        temp_constraints = ordered_dictionary_fetcher(cursor)
        cursor.execute(
            f"SELECT "
            f"     table_constraints.constraint_name,"
            f"     table_constraints.constraint_type,"
            f"     check_constraints.check_clause,"
            f"     referential_constraints.unique_constraint_name,"
            f"     COALESCE(columns.is_nullable, 'YES')::BOOLEAN AS is_nullable"
            f" FROM information_schema.table_constraints"
            f" LEFT OUTER JOIN information_schema.check_constraints ON ("
            f"     table_constraints.constraint_name = check_constraints.constraint_name)"
            f" LEFT OUTER JOIN information_schema.referential_constraints ON ("
            f"     table_constraints.constraint_name = referential_constraints.constraint_name)"
            f" LEFT OUTER JOIN information_schema.columns ON ("
            f"     table_constraints.table_name = columns.table_name"
            f"     AND check_constraints.check_clause = CONCAT(columns.column_name, ' IS NOT NULL'))"
            f" WHERE table_constraints.table_name = '{self.curr_table_name}'"
        )
        curr_constraints = ordered_dictionary_fetcher(cursor)

        if self.is_temp_table_partitioned and len(temp_constraints) == 0:
            # Assuming the parent partitioned table is just a shell and constraints are handled on child partitions
            logger.info(
                "Source temp table is a partitioned table with no constraints. Assuming that all/any "
                "constraints are managed on the child partitions and skipping validation."
            )
            return

        # NOT NULL constraints are created on a COLUMN not the TABLE, this means we do not control their name.
        # As a result, we verify that the same NOT NULL constraints exist on the tables but do not handle the swap.
        self.query_result_lookup["temp_table_constraints"] = list(
            filter(lambda val: val["is_nullable"], temp_constraints)
        )
        self.query_result_lookup["curr_table_constraints"] = list(
            filter(lambda val: val["is_nullable"], curr_constraints)
        )

        logger.info(
            f"Verifying that the constraint are the same except for suffixes in their name "
            f"(source_suffix='{self.source_suffix}', dest_suffix='{self.dest_suffix}')"
        )
        temp_constr_specs = {
            (
                re.sub(rf"{self.source_suffix}$", self.dest_suffix, val["constraint_name"], count=1)
                if val["is_nullable"]
                else val["check_clause"]
            ): {
                "constraint_type": val["constraint_type"],
                "check_clause": val["check_clause"],
                "unique_constraint_name": val["unique_constraint_name"],
            }
            for val in temp_constraints
        }
        curr_constr_specs = {
            val["constraint_name"] if val["is_nullable"] else val["check_clause"]: {
                "constraint_type": val["constraint_type"],
                "check_clause": val["check_clause"],
                "unique_constraint_name": val["unique_constraint_name"],
            }
            for val in curr_constraints
        }

        differences = []
        for constr, constr_def in temp_constr_specs.items():
            if constr not in curr_constr_specs:
                differences.append(
                    {
                        "temp_constr_name": constr,
                        "curr_constr_name": None,
                        "temp_constr_def": constr_def,
                        "curr_constr_def": None,
                    }
                )
                continue
            if constr_def not in curr_constr_specs.values():
                differences.append(
                    {
                        "temp_constr_name": constr,
                        "curr_constr_name": constr,
                        "temp_constr_def": constr_def,
                        "curr_constr_def": curr_constr_specs[constr],
                    }
                )
        # From other direction
        for constr, constr_def in curr_constr_specs.items():
            if constr not in temp_constr_specs:
                differences.append(
                    {
                        "temp_constr_name": None,
                        "curr_constr_name": constr,
                        "temp_constr_def": None,
                        "curr_constr_def": constr_def,
                    }
                )
                continue
            if constr_def not in temp_constr_specs.values():
                diff = {
                    "temp_constr_name": constr,
                    "curr_constr_name": constr,
                    "temp_constr_def": constr_def,
                    "curr_constr_def": curr_constr_specs[constr],
                }
                if diff not in differences:
                    differences.append(diff)

        if differences:
            logger.error(
                f"Constraints missing or differences found among the {len(curr_constraints)} current constraints "
                f"in {self.curr_table_name} and the {len(temp_constraints)} constraints of {self.temp_table_name} "
                f"table to be swapped in:\n{json.dumps(differences, indent=4)}"
            )
            raise SystemExit(1)

    def validate_columns(self, cursor):
        logger.info("Verifying that the same number of columns exist for the old and new table")
        columns_to_compare = [
            "pg_attribute.attname",  # column name
            "pg_attribute.atttypid",  # data type
            "pg_attribute.atttypmod",  # type modifiers such as length of VARCHAR
        ]
        cursor.execute(
            f"""
            SELECT {','.join(columns_to_compare)}
            FROM pg_attribute
            INNER JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
            WHERE pg_class.relname = '{self.temp_table_name}' AND pg_attribute.attnum > 0
            AND pg_attribute.attisdropped = FALSE
            ORDER BY pg_attribute.attname
        """
        )
        temp_columns = ordered_dictionary_fetcher(cursor)
        cursor.execute(
            f"""
            SELECT {','.join(columns_to_compare)}
            FROM pg_attribute
            INNER JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
            WHERE pg_class.relname = '{self.curr_table_name}' AND pg_attribute.attnum > 0
            AND pg_attribute.attisdropped = FALSE
            ORDER BY pg_attribute.attname
        """
        )
        curr_columns = ordered_dictionary_fetcher(cursor)
        if len(temp_columns) != len(curr_columns):
            logger.error(
                f"The number of columns are different for the tables: {self.temp_table_name} and {self.curr_table_name}"
            )
            raise SystemExit(1)

        logger.info("Verifying that the columns are the same")
        if temp_columns != curr_columns:
            logger.error(
                f"The column definitions are different for the tables: {self.temp_table_name} and {self.curr_table_name}"
            )
            raise SystemExit(1)

    def validate_state_of_tables(self, cursor, options):
        logger.info(f"Running validation to swap: {self.curr_table_name} with {self.temp_table_name}")

        self.validate_tables(cursor)
        if not options["allow_foreign_key"]:
            self.validate_foreign_keys(cursor)
        self.validate_constraints(cursor)
        self.validate_indexes(cursor)
        self.validate_columns(cursor)

    @staticmethod
    def get_most_recent_old_table(table, old_table_suffix=None):
        # Use the supplied old table suffix, or lookup the most recent old table made from `table`
        table_match = f"{table}_{old_table_suffix}" if old_table_suffix else rf"{table}_old[0-9]{{14}}$"
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                    SELECT t.tablename
                    FROM pg_tables t
                    WHERE t.tablename ~ '{table_match}'
                    ORDER BY t.tablename DESC LIMIT 1;
                """
            )
            results = cursor.fetchone()
            old_table = results[0] if results else None
            if not old_table:
                raise RuntimeError(f"Could not find any old tables matching: ~ {table_match} to undo the swap with.")
            return old_table
