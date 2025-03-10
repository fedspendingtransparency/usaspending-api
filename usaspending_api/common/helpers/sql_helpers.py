import os

from collections import namedtuple, OrderedDict
from django.conf import settings
from django.db import connection, connections, router, DEFAULT_DB_ALIAS
from psycopg2.sql import Composable, Identifier, SQL
from usaspending_api.awards.models import Award
from usaspending_api.common.exceptions import InvalidParameterException
from pathlib import Path
import re


def build_dsn_string(db_settings):
    """
    This function parses the Django database configuration in settings.py and
    returns a string DSN (https://en.wikipedia.org/wiki/Data_source_name) for
    a PostgreSQL database
    """
    return "postgres://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}".format(**db_settings)


def get_database_dsn_string(db_alias: str = DEFAULT_DB_ALIAS):
    if not db_alias:
        raise ValueError("Parameter 'db_alias' must have a value, but was None or empty")
    if db_alias in settings.DATABASES:
        return build_dsn_string(settings.DATABASES[db_alias])
    else:
        raise Exception(f'No valid database connection is configured with alias "{db_alias}"')


def get_broker_dsn_string():
    if settings.DATA_BROKER_DB_ALIAS in settings.DATABASES:  # Primary DB connection in a deployed environment
        return build_dsn_string(settings.DATABASES[settings.DATA_BROKER_DB_ALIAS])
    else:
        raise Exception("No valid Broker database connection is configured")


def read_sql_file_to_text(file_path: Path) -> str:
    """Open file and return text with most whitespace removed"""
    p = re.compile(r"\s\s+")
    return p.sub(" ", str(file_path.read_text().replace("\n", "  ")))


def read_sql_file(file_path):
    # Read in SQL file and extract commands into a list
    _, file_extension = os.path.splitext(file_path)

    if file_extension != ".sql":
        raise InvalidParameterException("Invalid file provided. A file with extension '.sql' is required.")

    # Open and read the file as a single buffer
    with open(file_path, "r") as fd:
        sql_file = fd.read()

    # all SQL commands (split on ';') and trimmed for whitespaces
    return [command.strip() for command in sql_file.split(";") if command.strip()]


def _build_order_by_column(sort_column, sort_order=None, sort_null=None):
    """
    Build a single column of the order by clause.  This takes one column and
    turns it into something like:

        "my_table"."my_column" asc nulls first

    sort_column - The column name in question.  Can be qualified (e.g. "awards.id").
    sort_order  - "asc" for ascending, "desc" for descending, None for default
                  database ordering.
    sort_null   - "first" if you want to see nulls first in the results, "last"
                  to see nulls last, or None for the default database behavior.

    Returns a string with the freshly built order by.
    """
    if type(sort_column) is not str:
        raise ValueError("Provided sort_column is not a string")

    # Split to handle column qualifiers (awards.id => "awards"."id").
    bits = [SQL(".").join([Identifier(c) for c in sort_column.split(".")])]

    if sort_order is not None:
        if sort_order not in ("asc", "desc"):
            raise ValueError('sort_order must be either "asc" or "desc"')
        bits.append(SQL(sort_order))

    if sort_null is not None:
        if sort_null not in ("first", "last"):
            raise ValueError('sort_null must be either "first" or "last"')
        bits.append(SQL("nulls %s" % sort_null))

    return SQL(" ").join(bits)


def build_composable_order_by(sort_columns, sort_orders=None, sort_nulls=None):
    """
    Given columns, sort orders, and null ordering directives, build a SQL
    "order by" clause as a Composable object.

    sort_columns - Either a single column name as a string or an iterable of
                   column name strings.  Column names can include table or
                   table alias qualifiers ("id", "awards.id", "a.id", etc).
    sort_orders  - Either None, a single sort order as a string, or an iterable
                   of sort order strings ("asc" or "desc").  If none are
                   supplied, sort order will not be incorporated into the
                   SQL statement causing default database sort ordering to
                   occur.  If one is supplied, every sort column will be sorted
                   in the same order.  If more than one sort order is supplied,
                   the number must match the number of sort columns provided.
    sort_nulls   - Either None, a single NULL handling directive as a string,
                   or an iterable of NULL handling directive strings ("first"
                   or "last").  If none are supplied, NULL handling will not be
                   incorporated into the SQL statement causing default database
                   NULL handling to occur.  If one is supplied, every sort
                   column will handle NULLs the same way.  If more than one
                   sort order is supplied, the number must match the number of
                   sort columns provided.

    Returns a Composable object that contains the entire order by clause or an
    empty Composable object if no sort columns were provided.
    """
    # Shortcut everything if there's nothing to do.
    if not sort_columns:
        return SQL("")

    # To simplify processing, make all of our parameters iterables of the same length.
    if type(sort_columns) is str:
        sort_columns = [sort_columns]

    column_count = len(sort_columns)

    if type(sort_orders) in (str, type(None)):
        sort_orders = [sort_orders] * column_count

    if type(sort_nulls) in (str, type(None)):
        sort_nulls = [sort_nulls] * column_count

    if len(sort_orders) != column_count:
        raise ValueError(
            "Number of sort_orders (%s) does not match number of sort_columns (%s)" % (len(sort_orders), column_count)
        )

    if len(sort_nulls) != column_count:
        raise ValueError(
            "Number of sort_nulls (%s) does not match number of sort_columns (%s)" % (len(sort_nulls), column_count)
        )

    order_bys = []
    for column, order, null in zip(sort_columns, sort_orders, sort_nulls):
        order_bys.append(_build_order_by_column(column, order, null))

    return SQL("order by ") + SQL(", ").join(order_bys)


def convert_composable_query_to_string(sql, model=Award, cursor=None):
    """
    A composable query is one built using psycopg2 Identifier, Literal, and SQL
    helper objects.  While Django itself seems to have no problem understanding
    composable queries, the django-debug-toolbar chokes on them so we need to
    convert them to string queries before running them.

    sql    - Can be either a sql string statement or a psycopg2 Composable
             object (Identifier, Literal, SQL, etc).
    model  - A Django model that represents a database table germane to your
             query.  If one is not supplied, Award will be used since it is
             fairly central to the database as a whole.
    cursor - If you happen to have a database cursor, feel free to pass that in.

    """
    if isinstance(sql, Composable):
        if cursor is None:
            connection = get_connection(model)
            with connection.cursor() as _cursor:
                return sql.as_string(_cursor.connection)
        else:
            return sql.as_string(cursor.connection)
    return sql


def cursor_fetcher(cursor):
    """Fetcher that simply returns the cursor."""
    return cursor


def fetchall_fetcher(cursor):
    """
    Fetcher that returns the default fetchall().  Return value will be
    roughly equivalent to:

        [
            (54360982, None),
            (54360880, 54360982)
        ]

    """
    return cursor.fetchall()


def named_tuple_fetcher(cursor):
    """
    Return all rows from a cursor as a list of named tuples.  Return
    value will be roughly equivalent to:

        [
            Result(id=54360982, parent_id=None),
            Result(id=54360880, parent_id=54360982)
        ]

    """
    columns = namedtuple("Result", [col[0] for col in cursor.description])
    return [columns(*row) for row in cursor.fetchall()]


def ordered_dictionary_fetcher(cursor):
    """
    Return all rows from a cursor as a list of ordered dictionaries.  Return
    value will be roughly equivalent to:

        [
            {'id': 54360982, 'parent_id': None},
            {'id': 54360880, 'parent_id': 54360982}
        ]

    """
    columns = [col[0] for col in cursor.description]
    return [OrderedDict(zip(columns, row)) for row in cursor.fetchall()]


def rowcount_fetcher(cursor):
    """Return the rowcount returned by the cursor."""
    return cursor.rowcount


def single_value_fetcher(cursor):
    """Return the first value in the first row of the cursor."""
    return cursor.fetchall()[0][0]


def execute_sql(sql, model=Award, fetcher=fetchall_fetcher, read_only=True):
    """
    Executes a sql query against a database.

    sql         - Can be either a sql string statement or a psycopg2 Composable
                  object (Identifier, Literal, SQL, etc).
    model       - A Django model that represents a database table germane to your
                  query.  If one is not supplied, Award will be used since it is
                  fairly central to the database as a whole.
    fetcher     - A function that accepts a live, post-execute cursor and returns
                  whatever it wants.  This determines the return results of the
                  function as a whole.
    read_only   - Supply false here if you intend to perform updates against the
                  database.

    Returns query results in the format dictated by the fetcher.
    """
    connection = get_connection(model, read_only=read_only)
    with connection.cursor() as cursor:
        # Because django-debug-toolbar does not understand Composable queries,
        # we need to convert the query to a string before executing it.
        cursor.execute(convert_composable_query_to_string(sql, cursor=cursor))
        return fetcher(cursor)


def execute_sql_simple(sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)


def execute_dml_sql(sql, model=Award):
    """
    Convenience function to execute a sql query against a database that performs
    some sort of data manipulation (INSERT, UPDATE, DELETE, etc).  Returns the number
    of rows affected.
    """
    return execute_sql(sql=sql, model=model, fetcher=rowcount_fetcher, read_only=False)


def execute_sql_to_ordered_dictionary(sql, model=Award, read_only=True):
    """Convenience function to return execute_sql results as a list of ordered dictionaries."""
    return execute_sql(sql, model=model, fetcher=ordered_dictionary_fetcher, read_only=read_only)


def execute_sql_to_named_tuple(sql, model=Award, read_only=True):
    """Convenience function to return execute_sql results as a list of named tuples."""
    return execute_sql(sql, model=model, fetcher=named_tuple_fetcher, read_only=read_only)


def execute_sql_return_single_value(sql, model=Award, read_only=True):
    """Convenience function to return execute_sql results as a list of named tuples."""
    return execute_sql(sql, model=model, fetcher=single_value_fetcher, read_only=read_only)


def get_connection(model=Award, read_only=True):
    """
    As of this writing, USAspending alternates database reads between multiple
    databases using usaspending_api.routers.replicas.ReadReplicaRouter.  Django
    will not take advantage of this router when executing raw SQL against a
    connection.  This function will help with that by using the database router
    to choose an appropriate connection.

    Both db_for_read and db_for_write need a model to help them decide which
    database connection to choose.  My advice is to supply the model associated
    with the primary table in your query.  If you do not supply a model, the
    Award model will be used as it is fairly central to the database as a whole
    and will work for nearly all queries.

    model     - A Django model that represents a database table germane to
                your query.  If one is not supplied, Award will be used since
                it is fairly central to the database as a whole.
    read_only - Unfortunately, Django cannot understand SQL so we need to
                explicitly tell it whether or not we intend to make changes to
                the database.  read_only = True if we only intend to query.
                read_only = False if we will be making any changes (UPDATE,
                DELETE, etc.)  The router uses this and model when determining
                which database connection to return.

    Returns an appropriate Django database connection.
    """
    if read_only:
        _connection = connections[router.db_for_read(model)]
    else:
        _connection = connections[router.db_for_write(model)]
    return _connection


def close_all_django_db_conns() -> None:
    """
    Helper function to close all DB connections
    Sometimes we have to kill any DB connections before forking processes
    as Django will want to share the single connection with all processes
    and we don't want to have any deadlock/SSL problems due to that.
    """

    connections.close_all()


def is_table_partitioned(table, cursor):
    if "." in table:
        schema, table = table.split(".")[0], table.split(".")[1]
    else:
        schema = "public"
    cursor.execute(
        f"""
            SELECT DISTINCT pg_partition_root(partrelid) AS partitioned_table
            FROM pg_partitioned_table
            WHERE pg_partition_root(partrelid) = '{schema}.{table}'::regclass;
            """
    )
    return cursor.fetchone() is not None


def get_parent_partitioned_table(table, cursor):
    """Return the parent partitioned table if the provided table is a partition, otherwise None"""
    if "." in table:
        schema, table = table.split(".")[0], table.split(".")[1]
    else:
        schema = "public"
    cursor.execute(
        f"""
            SELECT pg_partition_root(inhrelid) FROM pg_inherits
            WHERE inhrelid = '{schema}.{table}'::regclass;
        """
    )
    parent_partition_results = cursor.fetchone()
    return parent_partition_results[0] if parent_partition_results else None
