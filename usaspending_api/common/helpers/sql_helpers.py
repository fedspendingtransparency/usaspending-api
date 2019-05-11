import datetime
import logging
import os

from collections import OrderedDict
from django.conf import settings
from django.db import connections, router
from django.db import DEFAULT_DB_ALIAS
from django.db.models import Func, IntegerField
from psycopg2.sql import Composable, Identifier, SQL

from usaspending_api.awards.models import Award
from usaspending_api.common.exceptions import InvalidParameterException


logger = logging.getLogger('console')

TYPES_TO_QUOTE_IN_SQL = (str, datetime.date)


def get_database_dsn_string():
    """
        This function parses the Django database configuration in settings.py and
        returns a string DSN (https://en.wikipedia.org/wiki/Data_source_name) for
        a PostgreSQL database
        Will return a different database configuration for local vs deployed.
    """

    if "db_source" in settings.DATABASES:  # Primary DB connection in a deployed environment
        db = settings.DATABASES["db_source"]
    elif "default" in settings.DATABASES:  # For single DB connections used in scripts and local dev
        db = settings.DATABASES["default"]
    else:
        raise Exception("No valid database connection is configured")

    return "postgres://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}".format(**db)


def read_sql_file(file_path):
    # Read in SQL file and extract commands into a list
    _, file_extension = os.path.splitext(file_path)

    if file_extension != '.sql':
        raise InvalidParameterException("Invalid file provided. A file with extension '.sql' is required.")

    # Open and read the file as a single buffer
    with open(file_path, 'r') as fd:
        sql_file = fd.read()

    # all SQL commands (split on ';') and trimmed for whitespaces
    return [command.strip() for command in sql_file.split(';') if command]


def generate_raw_quoted_query(queryset):
    """
    Generates the raw sql from a queryset with quotable types quoted.
    This function provided benefit since the Django queryset.query doesn't quote
        some types such as dates and strings. If Django is updated to fix this,
        please use that instead.

    Note: To add new python data types that should be quoted in queryset.query output,
        add them to TYPES_TO_QUOTE_IN_SQL global
    """
    sql, params = queryset.query.get_compiler(DEFAULT_DB_ALIAS).as_sql()
    str_fix_params = []
    for param in params:
        if isinstance(param, TYPES_TO_QUOTE_IN_SQL):
            # single quotes are escaped with two '' for strings in sql
            param = param.replace('\'', '\'\'') if isinstance(param, str) else param
            str_fix_param = '\'{}\''.format(param)
        elif isinstance(param, list):
            str_fix_param = 'ARRAY{}'.format(param)
        else:
            str_fix_param = param
        str_fix_params.append(str_fix_param)
    return sql % tuple(str_fix_params)


class FiscalMonth(Func):
    function = "EXTRACT"
    template = "%(function)s(MONTH from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalQuarter(Func):
    function = "EXTRACT"
    template = "%(function)s(QUARTER from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalYear(Func):
    function = "EXTRACT"
    template = "%(function)s(YEAR from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


def _build_order_by_column(column, order, null):
    """
    Build a single column of the order by clause.  This takes one column and
    turns it into something like:

        my_column asc nulls first

    """
    bits = []

    if type(column) is not str:
        raise ValueError('Provided sort_column is not a string')

    # Split to handle column qualifiers (awards.id => "awards"."id").
    bits.append(SQL('.').join([Identifier(c) for c in column.split('.')]))

    if order is not None:
        if order not in ('asc', 'desc'):
            raise ValueError('sort_orders must be either "asc" or "desc"')
        bits.append(SQL(order))

    if null is not None:
        if null not in ('first', 'last'):
            raise ValueError('nulls must be either "first" or "last"')
        bits.append(SQL('nulls %s' % null))

    return SQL(' ').join(bits)


def build_composable_order_by(sort_columns, sort_orders=None, nulls=None):
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
    nulls        - Either None, a single NULL handling directive as a string,
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
        return SQL('')

    # To simplify processing, make all of our parameters iterables of the same length.
    if type(sort_columns) is str:
        sort_columns = [sort_columns]

    column_count = len(sort_columns)

    if type(sort_orders) in (str, type(None)):
        sort_orders = [sort_orders] * column_count

    if type(nulls) in (str, type(None)):
        nulls = [nulls] * column_count

    if len(sort_orders) != column_count:
        raise ValueError(
            'Number of sort_orders (%s) does not match number of sort_columns (%s)' % (len(sort_orders), column_count)
        )

    if len(nulls) != column_count:
        raise ValueError(
            'Number of nulls (%s) does not match number of sort_columns (%s)' % (len(nulls), column_count)
        )

    order_bys = []
    for column, order, null in zip(sort_columns, sort_orders, nulls):
        order_bys.append(_build_order_by_column(column, order, null))

    return SQL('order by ') + SQL(', ').join(order_bys)


def execute_sql_to_ordered_dictionary(sql, model=Award):
    """
    Executes a read only sql query against a database.

    sql   - Can be either a sql string statement or a psycopg2 Composable
            object (Identifier, Literal, SQL, etc).
    model - A Django model that represents a database table germaine to your
            query.  If one is not supplied, Award will be used since it is
            fairly central to the database as a whole.

    !! IMPORTANT !! IMPORTANT !! IMPORTANT !! IMPORTANT !! IMPORTANT !!

    This function will read all query results into memory before returning
    them.  Only use this function if you are POSITIVE your query will not
    consume all available resources on the server.

    Additionally, this function assumes a single result from the cursor.  If
    running multiple queries in a single statement, please do not use this
    function.

    !! END IMPORTANT !! END IMPORTANT !! END IMPORTANT !! END IMPORTANT !!

    Returns query results as a list of ordered dictionaries.
    """
    connection = get_connection(model)
    with connection.cursor() as cursor:
        # Because django-debug-toolbar does not understand Composable queries,
        # we need to convert the query to a string before executing it.
        if isinstance(sql, Composable):
            _sql = sql.as_string(cursor.connection)
        else:
            _sql = sql
        cursor.execute(_sql)
        return fetchall_to_ordered_dictionary(cursor)


def fetchall_to_ordered_dictionary(cursor):
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

    model     - A Django model that represents a database table germaine to
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
