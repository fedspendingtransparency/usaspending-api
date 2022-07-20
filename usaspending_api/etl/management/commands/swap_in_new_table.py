import logging
import re

from django.conf import settings
from django.core.management import BaseCommand
from django.db import connection, transaction

from usaspending_api.common.helpers.sql_helpers import ordered_dictionary_fetcher

logger = logging.getLogger("script")


VIEWS_TO_UPDATE = {
    "award_search": [
        settings.APP_DIR / "database_scripts" / "matviews" / "vw_es_award_search.sql",
    ]
}


class Command(BaseCommand):
    help = """
    This command is used to swap two tables; the current and a new table with "_temp" appended.
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
    temp_schema_name: str
    temp_table_name: str

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
            help="The active Postgres Table to swap with another containing the same name with '_temp' appended",
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

    def handle(self, *args, **options):
        self.curr_table_name = options["table"]
        self.temp_table_name = f"{self.curr_table_name}_temp"

        with connection.cursor() as cursor:
            # Go ahead and retrieve the schema for both table; used for some validation checks and the final swap
            cursor.execute(
                f"SELECT table_name, table_schema"
                f" FROM information_schema.tables"
                f" WHERE table_name = '{self.curr_table_name}' OR table_name = '{self.temp_table_name}'"
            )
            schemas_lookup = {table: schema for (table, schema) in cursor.fetchall()}
            self.curr_schema_name = schemas_lookup.get(self.curr_table_name)
            self.temp_schema_name = schemas_lookup.get(self.temp_table_name)

            self.validate_state_of_tables(cursor, options)
            self.cleanup_old_data(cursor)
            self.swap_constraints_sql(cursor)
            self.swap_index_sql(cursor)
            self.swap_table_sql(cursor)
            if not options["keep_old_data"]:
                self.drop_old_table_sql(cursor)
            self.extra_sql(cursor)

    def cleanup_old_data(self, cursor):
        """Run SQL to clean up any old data that could conflict with the swap"""
        cursor.execute(f"DROP TABLE IF EXISTS {self.curr_table_name}_old CASCADE;")

    def swap_constraints_sql(self, cursor):
        logging.info("Renaming constraints of the new and old tables.")
        temp_constraints = self.query_result_lookup["temp_table_constraints"]
        curr_constraints = self.query_result_lookup["curr_table_constraints"]
        rename_sql = []
        sql_template = "ALTER TABLE {table_name} RENAME CONSTRAINT {old_constraint_name} TO {new_constraint_name};"
        for val in curr_constraints:
            old_name = val["constraint_name"]
            new_name = f"{old_name}_old"
            rename_sql.append(
                sql_template.format(
                    table_name=self.curr_table_name, old_constraint_name=old_name, new_constraint_name=new_name
                )
            )
        for val in temp_constraints:
            old_name = val["constraint_name"]
            new_name = re.match("^(.*)_temp$", old_name, flags=re.I)[1]
            rename_sql.append(
                sql_template.format(
                    table_name=self.temp_table_name, old_constraint_name=old_name, new_constraint_name=new_name
                )
            )

        if rename_sql:
            cursor.execute("\n".join(rename_sql))

    def swap_index_sql(self, cursor):
        logging.info("Renaming indexes of the new and old tables.")

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
            new_name = f"{old_name}_old"
            rename_sql.append(sql_template.format(old_index_name=old_name, new_index_name=new_name))
        for val in temp_indexes:
            old_name = val["indexname"]
            new_name = re.match("^(.*)_temp$", old_name, flags=re.I)[1]
            rename_sql.append(sql_template.format(old_index_name=old_name, new_index_name=new_name))

        if rename_sql:
            cursor.execute("\n".join(rename_sql))

    @transaction.atomic
    def swap_table_sql(self, cursor):
        logging.info("Renaming the new and old tables")
        sql_template = "ALTER TABLE {old_table_name} RENAME TO {new_table_name};"

        rename_sql = [
            f"ALTER TABLE {self.temp_table_name} SET SCHEMA {self.curr_schema_name};",
            sql_template.format(old_table_name=self.curr_table_name, new_table_name=f"{self.curr_table_name}_old"),
            sql_template.format(old_table_name=self.temp_table_name, new_table_name=f"{self.curr_table_name}"),
        ]
        cursor.execute("\n".join(rename_sql))

        # Update views to use the new "current" table following the swap
        for view_sql in VIEWS_TO_UPDATE.get(self.curr_table_name, []):
            cursor.execute(view_sql.read_text())

    def drop_old_table_sql(self, cursor):
        # Instead of using CASCADE, all old constraints and indexes are dropped manually
        logging.info("Dropping the old table.")
        drop_sql = []
        indexes = self.query_result_lookup["curr_table_indexes"]
        constraints = self.query_result_lookup["curr_table_constraints"]
        for val in constraints:
            name = f"{val['constraint_name']}_old"
            drop_sql.append(f"ALTER TABLE {self.curr_table_name}_old DROP CONSTRAINT {name};")
        for val in indexes:
            name = f"{val['indexname']}_old"
            drop_sql.append(f"DROP INDEX {name};")
        drop_sql.append(f"DROP TABLE {self.curr_table_name}_old;")
        cursor.execute("\n".join(drop_sql))

    def extra_sql(self, cursor):
        cursor.execute(f"ANALYZE VERBOSE {self.curr_table_name}")
        cursor.execute(f"GRANT SELECT ON {self.curr_table_name} TO readonly")

    def validate_tables(self, cursor):
        logger.info("Verifying that the old table exists.")
        cursor.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{self.curr_table_name}'")
        temp_tables = cursor.fetchall()
        if len(temp_tables) == 0:
            raise ValueError(f"There are no tables matching: {self.curr_table_name}")

        logger.info("Verifying that the new table exists.")
        cursor.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{self.temp_table_name}'")
        temp_tables = cursor.fetchall()
        if len(temp_tables) == 0:
            raise ValueError(f"There are no tables matching: {self.temp_table_name}")

    def validate_indexes(self, cursor):
        logger.info("Verifying that the same number of indexes exist for the old and new table.")
        cursor.execute(f"SELECT * FROM pg_indexes WHERE tablename = '{self.temp_table_name}'")
        temp_indexes = ordered_dictionary_fetcher(cursor)
        self.query_result_lookup["temp_table_indexes"] = temp_indexes
        cursor.execute(f"SELECT * FROM pg_indexes WHERE tablename = '{self.curr_table_name}'")
        curr_indexes = ordered_dictionary_fetcher(cursor)
        self.query_result_lookup["curr_table_indexes"] = curr_indexes
        if len(temp_indexes) != len(curr_indexes):
            raise ValueError(
                f"The number of indexes are different for the tables: {self.temp_table_name} and {self.curr_table_name}"
            )

        logger.info("Verifying that the indexes are the same except for '_temp' in the index and table name.")
        temp_indexes = [
            {"indexname": val["indexname"].replace("_temp", ""), "indexdef": val["indexdef"].replace("_temp", "")}
            for val in temp_indexes
        ]
        curr_index_names = [val["indexname"] for val in curr_indexes]
        # Index Definition include the <schema_name>.<table_table> to compare these the schema names are normalized
        curr_index_defs = [
            val["indexdef"].replace(f"{self.curr_schema_name}.", f"{self.temp_schema_name}.") for val in curr_indexes
        ]
        for index in temp_indexes:
            if index["indexname"] not in curr_index_names or index["indexdef"] not in curr_index_defs:
                raise ValueError(
                    f"The index definitions are different for the tables: {self.temp_table_name} and {self.curr_table_name}"
                )

    def validate_foreign_keys(self, cursor):
        logger.info("Verifying that Foreign Key constraints are not found.")
        cursor.execute(
            f"SELECT * FROM information_schema.table_constraints"
            f" WHERE table_name IN ('{self.temp_table_name}', '{self.curr_table_name}')"
            f" AND constraint_type = 'FOREIGN KEY'"
        )
        constraints = cursor.fetchall()
        if len(constraints) > 0:
            raise ValueError(
                f"Foreign Key constraints are not allowed on '{self.temp_table_name}' or '{self.curr_table_name}'."
                " It is advised to not allow Foreign Key constraints on swapped tables to avoid potential deadlock."
                " However, if needed they can be allowed with the `--allow-foreign-key` flag."
            )

    def validate_constraints(self, cursor):
        # Used to sort constraints for comparison since sorting in the original SQL query that retrieves them
        # would not be taking into account that some would have "_temp" appended
        def _sort_key(val):
            return val["constraint_name"]

        logger.info("Verifying that the same number of constraints exist for the old and new table.")
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

        if len(temp_constraints) != len(curr_constraints):
            raise ValueError(
                f"The number of constraints are different for the tables: {self.temp_table_name} and {self.curr_table_name}."
            )

        # NOT NULL constraints are created on a COLUMN not the TABLE, this means we do not control their name.
        # As a result, we verify that the same NOT NULL constraints exist on the tables but do not handle the swap.
        self.query_result_lookup["temp_table_constraints"] = list(
            filter(lambda val: val["is_nullable"], temp_constraints)
        )
        self.query_result_lookup["curr_table_constraints"] = list(
            filter(lambda val: val["is_nullable"], curr_constraints)
        )

        logger.info("Verifying that the constraints are the same except for '_temp' in the name.")
        temp_constraints = [
            {
                "constraint_name": val["constraint_name"].replace("_temp", "")
                if val["is_nullable"]
                else val["check_clause"],
                "constraint_type": val["constraint_type"],
                "check_clause": val["check_clause"],
                "unique_constraint_name": val["unique_constraint_name"],
            }
            for val in temp_constraints
        ]
        curr_constraints = [
            {
                "constraint_name": val["constraint_name"] if val["is_nullable"] else val["check_clause"],
                "constraint_type": val["constraint_type"],
                "check_clause": val["check_clause"],
                "unique_constraint_name": val["unique_constraint_name"],
            }
            for val in curr_constraints
        ]
        if sorted(temp_constraints, key=_sort_key) != sorted(curr_constraints, key=_sort_key):
            raise ValueError(
                f"The constraint definitions are different for the tables: {self.temp_table_name} and {self.curr_table_name}."
            )

    def validate_columns(self, cursor):
        logger.info("Verifying that the same number of columns exist for the old and new table.")
        columns_to_compare = [
            "column_name",
            "is_nullable",
            "data_type",
            "character_maximum_length",
            "character_octet_length",
            "numeric_precision",
            "numeric_precision_radix",
            "numeric_scale",
            "datetime_precision",
            "udt_name",
        ]
        cursor.execute(
            f"SELECT {','.join(columns_to_compare)}"
            f" FROM information_schema.columns"
            f" WHERE table_name = '{self.temp_table_name}'"
            f" ORDER BY column_name"
        )
        temp_columns = ordered_dictionary_fetcher(cursor)
        cursor.execute(
            f"SELECT {','.join(columns_to_compare)}"
            f" FROM information_schema.columns"
            f" WHERE table_name = '{self.curr_table_name}'"
            f" ORDER BY column_name"
        )
        curr_columns = ordered_dictionary_fetcher(cursor)
        if len(temp_columns) != len(curr_columns):
            raise ValueError(
                f"The number of columns are different for the tables: {self.temp_table_name} and {self.curr_table_name}."
            )

        logger.info("Verifying that the columns are the same.")
        if temp_columns != curr_columns:
            raise ValueError(
                f"The column definitions are different for the tables: {self.temp_table_name} and {self.curr_table_name}."
            )

    def validate_state_of_tables(self, cursor, options):
        logger.info(f"Running validation to swap: {self.curr_table_name} with {self.temp_table_name}.")

        self.validate_tables(cursor)
        if not options["allow_foreign_key"]:
            self.validate_foreign_keys(cursor)
        self.validate_constraints(cursor)
        self.validate_indexes(cursor)
        self.validate_columns(cursor)
