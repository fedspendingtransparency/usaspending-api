import logging

from django.core.management.base import BaseCommand
from functools import partial
from psycopg2.sql import Identifier, Literal, SQL
from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string, get_connection
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("console")


# All of our timers log to logger.info and logger.error.
Timer = partial(Timer, success_logger=logger.info, failure_logger=logger.error)


DIFF_LIMIT = 50


class Command(BaseCommand):

    schema_name = "public"

    help = (
        "Compare materialized view to table. A little tool we're using to monitor "
        "our materialized view to table conversion. Uses row hash to compare, so the "
        "columns need to be in the same order between the two."
    )

    def add_arguments(self, parser):

        parser.add_argument(
            "table_name",
            metavar="TABLE",
            help="Name of the table to be used for comparison."
        )

        parser.add_argument(
            "materialized_view_name",
            metavar="MATVIEW",
            help="Name of the materialized view to be used for comparison."
        )

        parser.add_argument(
            "primary_key",
            metavar="PK",
            help="Name of the column to treat like a primary key."
        )

        parser.add_argument(
            "--top-n-ids",
            metavar="TOP",
            type=int,
            default=0,
            help="By default, this script compares ALL rows in a table. As you "
            "can imagine, this could take an extremely long time. If you wish "
            "to just get a quick comparison, feel free to provide a number here "
            "and only that many top ids will be compared. For example, "
            "providing 10 will only compare the 10 largest ids."
        )

        parser.add_argument(
            "--schema-name",
            metavar="SCHEMA",
            default="public",
            help="The schema to which the table and materialized view exist. Defaults to '{}'.".format(self.schema_name)
        )

        parser.add_argument(
            "--sql",
            action="store_true",
            help="For debugging. Log all SQL statements."
        )

    def _execute_sql(self, sql):
        sql = convert_composable_query_to_string(sql)

        if self.log_sql:
            logger.info(sql)

        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(sql)
            if cursor.rowcount > -1:
                logger.info("{:,} rows affected".format(cursor.rowcount))
                return cursor.fetchall()

    def _get_starting_id(self):
        if self.top_n_ids > 0:
            rows = self._execute_sql(SQL("""
                select min({pk}) from (
                    select {pk} from {schema}.{table} order by {pk} desc limit {limit}
                ) t
            """).format(
                pk=Identifier(self.primary_key),
                schema=Identifier(self.schema_name),
                table=Identifier(self.table_name),
                limit=Literal(self.top_n_ids),
            ))
            if rows:
                return rows[0][0]
        return None

    @staticmethod
    def _log_differences(message, columns):
        if columns:
            logger.error(message)
            for c in columns:
                logger.error("    {}".format(c))

    def _diff_columns(self):
        results = self._execute_sql(SQL("""
            with
            table_columns as (
                select  column_name
                from    information_schema.columns
                where   table_schema = {schema} and
                        table_name = {table}
            ),
            view_columns as (
                select  attname column_name
                from    pg_attribute
                where   attrelid = {materialized_view}::regclass and
                        attnum > 0 and
                        not attisdropped
            )
            select  tc.column_name, vc.column_name
            from    table_columns tc
                    full outer join view_columns vc on vc.column_name = tc.column_name
            where   tc.column_name is null or vc.column_name is null
            order   by 1
        """).format(
            schema=Literal(self.schema_name),
            table=Literal(self.table_name),
            materialized_view=Literal("{}.{}".format(self.schema_name, self.materialized_view_name))
        ))

        if results:
            self._log_differences(
                "The following columns are in the table but not the materialized view:",
                [r[0] for r in results if r[0]]
            )
            self._log_differences(
                "The following columns are in the materialized view but not the table:",
                [r[1] for r in results if r[1]]
            )
            exit(1)

        logger.info("All columns match")

    def _diff_rows(self, starting_id):
        results = self._execute_sql(SQL("""
            select  t.{pk}, v.{pk}
            from    {schema}.{table} t
                    full outer join {schema}.{materialized_view} v on v.{pk} = t.{pk}
            where   md5(t::text) is distinct from md5(v::text) {more_where}
            limit   {diff_limit}
        """).format(
            schema=Identifier(self.schema_name),
            table=Identifier(self.table_name),
            materialized_view=Identifier(self.materialized_view_name),
            pk=Identifier(self.primary_key),
            more_where=(
                SQL("and t.{pk} >= {id} and v.{pk} >= {id}").format(
                    pk=Identifier(self.primary_key), id=Literal(starting_id)
                ) if starting_id is not None else
                SQL("")
            ),
            diff_limit=Literal(DIFF_LIMIT),
        ))

        if results:
            self._log_differences(
                "The following primary keys are in the table but not the materialized view:",
                [r[0] for r in results if r[0] and not r[1]]
            )
            self._log_differences(
                "The following primary keys are in the materialized view but not the table:",
                [r[1] for r in results if r[1] and not r[0]]
            )
            self._log_differences(
                "The following primary keys do not match between the materialized and the table:",
                [r[0] for r in results if r[0] and r[1]]
            )
            if len(results) >= DIFF_LIMIT:
                logger.error("Stopping after {:,} differences".format(DIFF_LIMIT))
            exit(1)

        logger.info("All rows match")

    def handle(self, *args, **options):

        self.table_name = options["table_name"]
        self.materialized_view_name = options["materialized_view_name"]
        self.primary_key = options["primary_key"]
        self.top_n_ids = options["top_n_ids"]
        self.schema_name = options["schema_name"]
        self.log_sql = options["sql"]

        module_name = __name__.split(".")[-1]
        with Timer(module_name):
            with Timer("Diff columns"):
                self._diff_columns()
            with Timer("Getting starting id"):
                starting_id = self._get_starting_id()
                logger.info("No starting id" if starting_id is None else "Starting with id {}".format(starting_id))
            with Timer("Diff rows"):
                self._diff_rows(starting_id)
