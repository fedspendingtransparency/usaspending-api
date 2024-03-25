from abc import ABC, abstractmethod
from argparse import ArgumentError
from datetime import datetime, timezone
import json
from pprint import pformat
import re
from django.db import ProgrammingError, connection, transaction
import logging
from usaspending_api.broker.helpers.last_delta_table_load_version import (
    get_last_delta_table_load_versions,
    update_last_live_load_version,
)
from usaspending_api.broker.lookups import LoadTrackerLoadTypeEnum, LoadTrackerStepEnum
from usaspending_api.common.helpers.sql_helpers import (
    get_parent_partitioned_table,
    is_table_partitioned,
    ordered_dictionary_fetcher,
)
from usaspending_api.common.load_tracker import LoadTracker

from django.core.management import BaseCommand
from typing import List
from usaspending_api.etl.management.commands.load_query_to_delta import TABLE_SPEC
from usaspending_api.search.delta_models.award_search import AWARD_SEARCH_COLUMNS
from usaspending_api.etl.management.sql.swap_in_new_table.upsert_live_tables import upsert_live_tables_sql
from usaspending_api.etl.management.sql.swap_in_new_table.delete_from_live_tables import delete_from_live_tables_sql
from usaspending_api.etl.management.sql.swap_in_new_table.dependent_views import detect_dep_view_sql


class SwapInNewTableStrategy(ABC):
    """A composable class that can be used according to the Strategy software design pattern.
    This class establishes the common interface for a suite of strategies used by the swap in table
    Django command. Implement this abstract class by defining use case specific
    swap in new table behaviors and wrap those behaviors in a class that inherits from this base class.
    """

    logger = logging.getLogger("script")

    def __init__(self):
        self._load_tracker: LoadTracker = None

    @property
    def load_tracker(
        self,
    ) -> LoadTracker:
        return self._load_tracker

    @load_tracker.setter
    def load_tracker(self, load_tracker: LoadTracker):
        self._load_tracker = load_tracker

    @abstractmethod
    def swap_in_new_table(self):
        """Facilitates the "swapping" of data between tables."""
        pass


class IncrementalLoadSwapInTableStrategy(SwapInNewTableStrategy):

    def __init__(self, swap_table_options: dict, django_command: BaseCommand):
        """
        Args:
            swap_table_options: A collection of self._options that are used by the logic to
                swap in table.
            django_command: An instance of a Django command that will be using this strategy.
        """
        self._options = swap_table_options
        self._django_command = django_command

    def swap_in_new_table(
        self,
    ):
        if self._load_tracker is None:
            raise ValueError("LoadTracker property of this istance must be set before invoking swap in new table.")

        self._validate_options()

        table = self._options["table"]
        # TODO: PIPE-513 developed a solution that forces this knowledge duplication hopefully that other implementation can be fixed in the future.
        # Fixing that other implementation would enable the suite of swap_in_new_table classes to not duplicate knowledge.
        upsert_table_name = f"{table}_temp_upserts"
        delete_table_name = f"{table}_temp_deletes"
        temp_table_list = [upsert_table_name, delete_table_name]
        with connection.cursor() as cursor:
            # Go ahead and retrieve the schema for both table; used for some validation checks and the final swap
            cursor.execute(
                f"""
                SELECT tablename, schemaname
                FROM pg_tables
                WHERE tablename = '{table}' OR tablename = '{delete_table_name}' OR tablename = '{upsert_table_name}'
                UNION
                SELECT matviewname AS tablename, schemaname
                FROM pg_matviews
                WHERE matviewname = '{table}' OR tablename = '{delete_table_name}' OR tablename = '{upsert_table_name}'
            """
            )
            schemas_lookup = {table: schema for (table, schema) in cursor.fetchall()}

            # Begin fetching schemas of the tables involved in this operation
            curr_schema_name = schemas_lookup.get(table)
            qualified_dest_table = f"{curr_schema_name}.{table}"
            temp_schema_name = schemas_lookup.get(delete_table_name)

            # We assume both temp tables have the same schema, but let's double check
            if temp_schema_name != schemas_lookup.get(upsert_table_name):
                raise ValueError(
                    f"The temp tables by the names {delete_table_name} and {upsert_table_name} must reside in the same schema. Currently, they do not."
                )

            qualified_upsert_postgres_table = f"{temp_schema_name}.{upsert_table_name}"
            qualified_delete_postgres_table = f"{temp_schema_name}.{delete_table_name}"
            self._validate_live_tables(cursor, table)
            self._validate_temp_tables(cursor, temp_table_list)

            self._execute_upserts(cursor, qualified_dest_table, qualified_upsert_postgres_table)
            self._execute_deletes(cursor, qualified_dest_table, qualified_delete_postgres_table)

            # Begin refreshing views
            self.dep_views = self._dependent_views(cursor, curr_schema_name, table)
            self.old_suffix = f"_old{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            # We only need to call this once, not per temp table. Right now we have more than one temp table
            # so we are just arbitrarly using one of them.
            self._create_new_views(cursor, table, temp_schema_name, upsert_table_name)
            self._swap_dependent_views(cursor, temp_schema_name)
            self._drop_old_views(cursor)

    def _create_new_views(self, cursor, table, temp_schema_name, temp_table_name):
        for dep_view in self.dep_views:
            # Note: Despite pointing at the new temp table at first,
            #       the recreated views will automatically update and stay pointed to it in the swap via postgres
            self.logger.info(f"Recreating dependent view: {dep_view['dep_view_fullname']}")
            dep_view_sql = dep_view["dep_view_sql"].replace(table, temp_table_name)
            mv_s = "MATERIALIZED " if dep_view["is_matview"] else ""
            cursor.execute(f"DROP {mv_s}VIEW IF EXISTS {temp_schema_name}.{dep_view['dep_view_name']};")
            cursor.execute(f"CREATE {mv_s}VIEW {temp_schema_name}.{dep_view['dep_view_name']} " f"AS ({dep_view_sql});")

    def _drop_old_views(self, cursor):
        for dep_view in self.dep_views:
            mv_s = "MATERIALIZED " if dep_view["is_matview"] else ""
            self.logger.info(f"Dropping old dependent view: {dep_view['dep_view_fullname']}{self.old_suffix}")
            cursor.execute(f"DROP {mv_s}VIEW {dep_view['dep_view_fullname']}{self.old_suffix};")

    def _dependent_views(self, cursor, curr_schema_name, curr_table_name):
        """Detects views that are dependent on the table to be swapped."""
        cursor.execute(detect_dep_view_sql.format(curr_schema_name=curr_schema_name, curr_table_name=curr_table_name))
        dep_views = ordered_dictionary_fetcher(cursor)
        return dep_views

    def _swap_dependent_views(self, cursor, temp_schema_name):
        sql_view_template = "ALTER {mv_s}VIEW {old_view_schema}.{old_view_name} RENAME TO {new_view_name};"
        for dep_view in self.dep_views:
            # Coordinate the schema move and view renames in 1 atomic operation
            # But don't have the transaction span more than 1 view at a time to avoid any potential deadlocks with
            # ongoing queries
            with transaction.atomic():
                mv_s = "MATERIALIZED " if dep_view["is_matview"] else ""
                view_rename_sql = [
                    # 1. move temp suffixed view to the target schema
                    f"ALTER {mv_s}VIEW {temp_schema_name}.{dep_view['dep_view_name']} "
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
                        old_view_name=f"{dep_view['dep_view_name']}",
                        new_view_name=dep_view["dep_view_name"],
                    ),
                ]
                self.logger.info(f"Running view swap SQL in an atomic transaction:\n{pformat(view_rename_sql)}")
                cursor.execute("\n".join(view_rename_sql))

    def _validate_options(self) -> None:
        """Validates the combination of options provided to this command. Will
        raise an exception if any are invalid
        """
        is_undo = self._options["undo"]
        keep_old_data = self._options["keep-old-data"]
        foreign_key = self._options["allow-foreign-key"]
        if is_undo is not None or keep_old_data is not None or foreign_key is not None:
            raise ArgumentError(
                f"The swap in new table command is using the IncrementalLoadSwapInTableStrategy strategy class. IncrementalLoadSwapInTableStrategy does not support these options '--keep-old-data', '--undo', and '--allow-foreign-key'."
            )

    def _validate_live_tables(self, cursor, table: str):
        """Checks various things that need to be true before this strategy can work.

        Args:
            table: The live table name to validate.
        """
        table = self._options["table"]
        validation_query = f"""
            SELECT EXISTS (
            SELECT FROM pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  c.relname = '{table}'
            AND    c.relkind = 'r'    -- only tables
            );
        """
        cursor.execute(validation_query)
        validation_results = ordered_dictionary_fetcher(cursor)
        if len(validation_results) == 0 or validation_results[0]["exists"] is False:
            raise ValueError(f"Swap in new table command cannot proceed because {table} does not exist.")

    def _validate_temp_tables(self, cursor, temp_table_list: List):
        """Checks various things that need to be true before this strategy can work.

        Args:
            temp_table_list: A list of temp tables that need validation.
        """
        for temp_table_name in temp_table_list:
            validation_query = f"""
                SELECT EXISTS (
                SELECT FROM pg_catalog.pg_class c
                JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE  c.relname = {temp_table_name}
                AND    c.relkind = 'r'
                );
            """
            cursor.execute(validation_query)
            validation_results = ordered_dictionary_fetcher(cursor)
            if len(validation_results) == 0 or validation_results[0]["exists"] is False:
                raise ValueError(f"Swap in new table command cannot proceed because {temp_table_name} does not exist.")

    def _execute_upserts(self, cursor, qualified_dest_table, qualified_upsert_postgres_table):
        """Facilitates the process of upserting into the live tables from the temp tables."""
        load_datetime = datetime.now(timezone.utc)
        set_cols = [f"d.{col_name} = s.{col_name}" for col_name in AWARD_SEARCH_COLUMNS]
        insert_col_name_list = [col_name for col_name in AWARD_SEARCH_COLUMNS]
        insert_value_list = insert_col_name_list[:-3]
        insert_value_list.extend(["NULL"])
        insert_value_list.extend([f"""'{load_datetime.isoformat(" ")}'"""] * 2)
        insert_values = ", ".join([value for value in insert_value_list])
        upsert_live_tables_sql.format(
            dest_table=qualified_dest_table,
            upsert_temp_table=qualified_upsert_postgres_table,
            join_condition="d.generated_unique_award_id = s.generated_unique_award_id",
            set_cols={", ".join(set_cols)},
            insert_col_names=", ".join([col_name for col_name in insert_col_name_list]),
            insert_values=insert_values,
        )
        cursor.execute(cursor)

    def _execute_deletes(self, cursor, qualified_dest_table, qualified_delete_postgres_table):
        """Facilitates the process of deleting from our live tables based on the temp tables."""
        delete_from_live_tables_sql.format(
            dest_table=qualified_dest_table,
            delete_temp_table=qualified_delete_postgres_table,
            join_condition="d.award_id = s.award_id",
        )
        cursor.execute(cursor)


class FullLoadSwapInTableStrategy(SwapInNewTableStrategy):
    """
    This class is used to swap two tables. It uses a base table name and then discriminating suffixes (possibly
    empty string) for the tables to be swapped (defaults to swapping <table>_temp in for <table>).
    Validation is run against the new table to ensure that after the swap is complete all of the indexes, constraints,
    columns, and table name will be the same.

    NOTE: This entire process IS NOT ATOMIC (only the final swap of tables)!
    This choice was made to prevent potential deadlock scenarios since the swapping / renaming of indexes, constraints,
    and tables should only take milliseconds. If this class fails then any cleanup will have to be done manually.

    Current API and Download functionality is not affected until the step that actually renames the old table. That
    change will take an ACCESS EXCLUSIVE LOCK and any future queries following it will hit the new table. On a Primary
    database where typical query performance is under 1 minute this could cause some queries to take longer as they wait
    on the LOCK to be released, depending on what the lock is waiting on. For a Replica database this will cause some
    queries to cancel if they are blocking the ACCESS EXCLUSIVE LOCK for too long and could impede replication.

    At the time of the actual DROP TABLE logic there should be no activity present on the old table since the
    ALTER TABLE for the rename would have blocked all activity and routed to the new table following the rename.
    """

    def __init__(self, swap_table_options: dict, django_command: BaseCommand):
        """
        Args:
            swap_table_options: A collection of self._options that are used by the logic to
                swap in table.
            django_command: An instance of a Django command that will be using this strategy.
        """
        self._options = swap_table_options
        self._django_command = django_command

    def swap_in_new_table(self):
        if self._load_tracker is None:
            raise ValueError("LoadTracker property of this istance must be set before invoking swap in new table.")

        self._validate_options()

        is_undo = self._options["undo"]
        if is_undo:
            old_table = self._get_most_recent_old_table(self._options["table"])
            old_suffix = re.sub(rf"{self._options['table']}_", "", old_table)
            self.logger.info(
                f"Overwriting --source-suffix value of '{self._options['source_suffix']}' with '{old_suffix}' for undo."
            )
            self._options["source_suffix"] = old_suffix

        # init object attributes
        self.source_suffix = f"_{self._options['source_suffix']}" if self._options["source_suffix"] else ""
        self.dest_suffix = f"_{self._options['dest_suffix']}" if self._options["dest_suffix"] else ""
        self.curr_table_name = f"{self._options['table']}{self.dest_suffix}"
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
            self.dep_views = self._dependent_views(cursor, self.curr_schema_name, self.curr_table_name)

            self._validate_tables(cursor)

            self.logger.info(
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
            self._validate_state_of_tables(cursor)

            # Get constraints and indexes named correctly before the table is renamed
            self._swap_constraints_sql(cursor)
            self._swap_index_sql(cursor)

            # New temp views are setup to point to the temp table, and will swap-in with the table
            self._create_new_views(cursor)

            # Prep stats and privs on the staged temp table before swapping (renaming)
            self._analyze_temp_table(cursor)
            self._grant_privs_on_temp_table(cursor)

            # Do the swaps via renames
            self._swap_dependent_views(cursor)
            self._swap_table(cursor)

            if not self._options["keep_old_data"]:
                self._drop_old_views(cursor)
                if self.curr_table_parent_partitioned_table:
                    self.logger.info(
                        f"'{self.curr_table_name}' is a partition of '{self.curr_table_parent_partitioned_table}' and "
                        f"will not be dropped. Swapped-out partitions are not deleted after the swap due to the risk "
                        f"of their live parent partitioned table still receiving traffic while having one of its "
                        f"partitions deleted. If '{self.curr_table_parent_partitioned_table}' is also being swapped, "
                        f"then this partition will be deleted when it is deleted. Otherwise delete "
                        f"'{self.curr_table_name}{self.old_suffix}' manually."
                    )
                else:
                    self._drop_old_table_sql(cursor)

            # If `delta_table_load_version_key` exists in the table_spec, this table could potentially be loaded incrementally,
            # therefore we need to keep track of the last version of the table persisted to the live table. In this case, that
            # would be the last version swapped from temp to live. The `last_version_to_staging` represents the last version
            # either that was either fully copied to the temp table, or the last version incrementally copied to staging tables,
            # so here we can use it as the new version copied to live.
            table_spec = TABLE_SPEC.get(self._options["table"])
            delta_table_load_version_key = (
                table_spec["delta_table_load_version_key"] if table_spec is not None else None
            )

            if delta_table_load_version_key is not None:
                last_version_to_staging, last_version_to_live = get_last_delta_table_load_versions(
                    delta_table_load_version_key
                )
                update_last_live_load_version(delta_table_load_version_key, last_version_to_staging)

    def _validate_options(self) -> None:
        """Validates the combination of options provided to this command. Will
        raise an exception if any are invalid
        """
        is_undo = self._options["undo"]
        table = self._options["table"]
        load_step = LoadTrackerStepEnum(self._load_tracker.load_tracker_step.name)
        latest_load_tracker_load_type = LoadTracker.get_latest_load_tracker_type(load_step.value)
        if latest_load_tracker_load_type == LoadTrackerLoadTypeEnum.INCREMENTAL_LOAD and is_undo:
            raise ArgumentError(
                f"The load tracker table indicates the last load type for {table} was an {LoadTrackerLoadTypeEnum.INCREMENTAL_LOAD.value}. You cannot use the option undo when the last load was an {LoadTrackerLoadTypeEnum.INCREMENTAL_LOAD.value}"
            )

    def _create_new_views(self, cursor):
        for dep_view in self.dep_views:
            # Note: Despite pointing at the new temp table at first,
            #       the recreated views will automatically update and stay pointed to it in the swap via postgres
            self.logger.info(f"Recreating dependent view: {dep_view['dep_view_fullname']}{self.source_suffix}")
            dep_view_sql = dep_view["dep_view_sql"].replace(self.curr_table_name, self.temp_table_name)
            mv_s = "MATERIALIZED " if dep_view["is_matview"] else ""
            cursor.execute(
                f"DROP {mv_s}VIEW IF EXISTS {self.temp_schema_name}.{dep_view['dep_view_name']}{self.source_suffix};"
            )
            cursor.execute(
                f"CREATE {mv_s}VIEW {self.temp_schema_name}.{dep_view['dep_view_name']}{self.source_suffix} "
                f"AS ({dep_view_sql});"
            )

    def _dependent_views(self, cursor, curr_schema_name, curr_table_name):
        """Detects views that are dependent on the table to be swapped."""
        cursor.execute(detect_dep_view_sql.format(curr_schema_name=curr_schema_name, curr_table_name=curr_table_name))
        dep_views = ordered_dictionary_fetcher(cursor)
        return dep_views

    def _validate_state_of_tables(self, cursor):
        self.logger.info(f"Running validation to swap: {self.curr_table_name} with {self.temp_table_name}")

        self._validate_tables(cursor)
        if not self._options["allow_foreign_key"]:
            self._validate_foreign_keys(cursor)
        self._validate_constraints(cursor)
        self._validate_indexes(cursor)
        self._validate_columns(cursor)

    def _validate_indexes(self, cursor):
        cursor.execute(f"SELECT * FROM pg_indexes WHERE tablename = '{self.temp_table_name}' ORDER BY indexname")
        temp_indexes = ordered_dictionary_fetcher(cursor)
        cursor.execute(f"SELECT * FROM pg_indexes WHERE tablename = '{self.curr_table_name}' ORDER BY indexname")
        curr_indexes = ordered_dictionary_fetcher(cursor)

        if self.is_temp_table_partitioned and len(temp_indexes) == 0:
            # Assuming the parent partitioned table is just a shell and indexes are handled on child partitions
            self.logger.info(
                "Source temp table is a partitioned table with no indexes. Assuming that all/any "
                "indexes are managed on the child partitions and skipping validation."
            )
            return

        self._django_command.query_result_lookup["temp_table_indexes"] = temp_indexes
        self._django_command.query_result_lookup["curr_table_indexes"] = curr_indexes

        self.logger.info(
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
            self.logger.error(
                f"Indexes missing or differences found among the {len(curr_indexes)} current indexes "
                f"in {self.curr_table_name} and the {len(temp_indexes)} indexes of {self.temp_table_name} "
                f"table to be swapped in:\n{json.dumps(differences, indent=4)}"
            )
            raise SystemExit(1)

    def _validate_foreign_keys(self, cursor):
        self.logger.info("Verifying that Foreign Key constraints are not found")
        cursor.execute(
            f"SELECT * FROM information_schema.table_constraints"
            f" WHERE table_name IN ('{self.temp_table_name}', '{self.curr_table_name}')"
            f" AND constraint_type = 'FOREIGN KEY'"
        )
        constraints = cursor.fetchall()
        if len(constraints) > 0:
            self.logger.error(
                f"Foreign Key constraints are not allowed on '{self.temp_table_name}' or '{self.curr_table_name}'."
                " It is advised to not allow Foreign Key constraints on swapped tables to avoid potential deadlock."
                " However, if needed they can be allowed with the `--allow-foreign-key` flag."
            )
            raise SystemExit(1)

    def _validate_constraints(self, cursor):
        self.logger.info("Verifying that the same number of constraints exist for the old and new table")
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
            self.logger.info(
                "Source temp table is a partitioned table with no constraints. Assuming that all/any "
                "constraints are managed on the child partitions and skipping validation."
            )
            return

        # NOT NULL constraints are created on a COLUMN not the TABLE, this means we do not control their name.
        # As a result, we verify that the same NOT NULL constraints exist on the tables but do not handle the swap.
        self._django_command.query_result_lookup["temp_table_constraints"] = list(
            filter(lambda val: val["is_nullable"], temp_constraints)
        )
        self._django_command.query_result_lookup["curr_table_constraints"] = list(
            filter(lambda val: val["is_nullable"], curr_constraints)
        )

        self.logger.info(
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
            self.logger.error(
                f"Constraints missing or differences found among the {len(curr_constraints)} current constraints "
                f"in {self.curr_table_name} and the {len(temp_constraints)} constraints of {self.temp_table_name} "
                f"table to be swapped in:\n{json.dumps(differences, indent=4)}"
            )
            raise SystemExit(1)

    def _validate_columns(self, cursor):
        self.logger.info("Verifying that the same number of columns exist for the old and new table")
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
            self.logger.error(
                f"The number of columns are different for the tables: {self.temp_table_name} and {self.curr_table_name}"
            )
            raise SystemExit(1)

        self.logger.info("Verifying that the columns are the same")
        if temp_columns != curr_columns:
            self.logger.error(
                f"The column definitions are different for the tables: {self.temp_table_name} and {self.curr_table_name}"
            )
            raise SystemExit(1)

    def _validate_tables(self, cursor):
        self.logger.info("Verifying that the current table exists")
        cursor.execute(
            f"""
            SELECT schemaname, tablename FROM pg_tables WHERE tablename = '{self.curr_table_name}'
            UNION
            SELECT schemaname, matviewname AS tablename FROM pg_matviews WHERE matviewname = '{self.curr_table_name}'
        """
        )
        curr_tables = cursor.fetchall()
        if len(curr_tables) == 0:
            self.logger.error(f"There are no tables matching: {self.curr_table_name}")
            raise SystemExit(1)

        self.logger.info("Verifying that the temp table exists")
        cursor.execute(
            f"""
            SELECT schemaname, tablename FROM pg_tables WHERE tablename = '{self.temp_table_name}'
            UNION
            SELECT schemaname, matviewname AS tablename FROM pg_matviews WHERE matviewname = '{self.temp_table_name}'
        """
        )
        temp_tables = cursor.fetchall()
        if len(temp_tables) == 0:
            self.logger.error(f"There are no tables matching: {self.temp_table_name}")
            raise SystemExit(1)

        self.logger.info("Verifying duplicate tables don't exist across schemas")
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
                self.logger.error(f"There are currently duplicate tables for '{table_name}' in different schemas")
                raise SystemExit(1)

    def _cleanup_old_data(self, cursor, old_suffix=None):
        """
        Run SQL to clean up any old data that could conflict with the swap.
        We first try to delete the "table" as though it is a table. In the event
        that this fails we ignore the error and attempt to then drop the "table"
        as though it is a Materialized View, displaying both the current and
        previous errors if this should fail again.
        """
        if not old_suffix:
            old_suffix = self.old_suffix
        self.logger.info(f"Dropping table {self.curr_table_name}{old_suffix} if it exists")
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {self.curr_table_name}{old_suffix} CASCADE;")
            return
        except ProgrammingError as e:
            error_log = str(e)
        try:
            cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {self.curr_table_name}{old_suffix} CASCADE;")
        except ProgrammingError as e:
            raise f"First Error:\n{error_log}\nSecond Error: {str(e)}"

    def _swap_constraints_sql(self, cursor):
        self.logger.info("Renaming constraints of the new and old tables")
        temp_constraints = self._django_command.query_result_lookup["temp_table_constraints"]
        curr_constraints = self._django_command.query_result_lookup["curr_table_constraints"]
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

    def _swap_index_sql(self, cursor):
        self.logger.info("Renaming indexes of the new and old tables")

        # Some Postgres constraints (UNIQUE, PRIMARY, and EXCLUDE) are updated along with the index which means
        # there is no need to rename the index following the constraint
        temp_constraint_names = [
            val["constraint_name"] for val in self._django_command.query_result_lookup["temp_table_constraints"]
        ]
        temp_indexes = [
            val
            for val in self._django_command.query_result_lookup["temp_table_indexes"]
            if val["indexname"] not in temp_constraint_names
        ]
        curr_constraint_names = [
            val["constraint_name"] for val in self._django_command.query_result_lookup["curr_table_constraints"]
        ]
        curr_indexes = [
            val
            for val in self._django_command.query_result_lookup["curr_table_indexes"]
            if val["indexname"] not in curr_constraint_names
        ]
        self._django_command.query_result_lookup["temp_table_indexes"] = temp_indexes
        self._django_command.query_result_lookup["curr_table_indexes"] = curr_indexes

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

    def _swap_dependent_views(self, cursor):
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
                self.logger.info(f"Running view swap SQL in an atomic transaction:\n{pformat(view_rename_sql)}")
                cursor.execute("\n".join(view_rename_sql))

    @transaction.atomic
    def _swap_table(self, cursor):
        self.logger.info("Renaming the new and old table")
        sql_table_template = "ALTER TABLE {old_table_name} RENAME TO {new_table_name};"

        table_rename_sql = [
            f"ALTER TABLE {self.temp_table_name} SET SCHEMA {self.curr_schema_name};",
            sql_table_template.format(
                old_table_name=self.curr_table_name, new_table_name=f"{self.curr_table_name}{self.old_suffix}"
            ),
            sql_table_template.format(old_table_name=self.temp_table_name, new_table_name=f"{self.curr_table_name}"),
        ]
        self.logger.info(f"Running table swap SQL in an atomic transaction:\n{pformat(table_rename_sql)}")
        cursor.execute("\n".join(table_rename_sql))

    def _drop_old_table_sql(self, cursor):
        self._drop_old_table_metadata(cursor)
        self._cleanup_old_data(cursor)

    def _drop_old_table_metadata(self, cursor):
        # Instead of using CASCADE, all old constraints and indexes are dropped manually
        self.logger.info("Manually dropping the old table metadata (constraints and indexes")
        drop_sql = []
        indexes = self._django_command.query_result_lookup["curr_table_indexes"]
        constraints = self._django_command.query_result_lookup["curr_table_constraints"]
        for val in constraints:
            name = f"{val['constraint_name']}{self.old_suffix}"
            drop_sql.append(f"ALTER TABLE {self.curr_table_name}{self.old_suffix} DROP CONSTRAINT {name};")
        for val in indexes:
            name = f"{val['indexname']}{self.old_suffix}"
            drop_sql.append(f"DROP INDEX {name};")
        if drop_sql:
            cursor.execute("\n".join(drop_sql))

    def _drop_old_views(self, cursor):
        for dep_view in self.dep_views:
            mv_s = "MATERIALIZED " if dep_view["is_matview"] else ""
            self.logger.info(f"Dropping old dependent view: {dep_view['dep_view_fullname']}{self.old_suffix}")
            cursor.execute(f"DROP {mv_s}VIEW {dep_view['dep_view_fullname']}{self.old_suffix};")

    def _analyze_temp_table(self, cursor):
        if not self.is_temp_table_partitioned:
            self.logger.info(f"Running ANALYZE VERBOSE {self.temp_table_name}")
            cursor.execute(f"ANALYZE VERBOSE {self.temp_table_name}")
        else:
            self.logger.info(
                f"Skipping ANALYZE VERBOSE {self.temp_table_name} because table is partitioned and ANALYZE "
                f"should already be run against each partition, which should be swapped before it."
            )

    def _grant_privs_on_temp_table(self, cursor):
        self.logger.info(f"GRANT SELECT ON {self.temp_table_name} TO readonly")
        cursor.execute(f"GRANT SELECT ON {self.temp_table_name} TO readonly")

    def _get_most_recent_old_table(self, table, old_table_suffix=None):
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
