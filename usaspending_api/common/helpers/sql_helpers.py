import logging
import os

from collections import OrderedDict, namedtuple
from django.db import connections, router
from django.db.models import Func, IntegerField
from psycopg2.sql import Composable, Identifier, SQL

from usaspending_api.awards.models import Award
from usaspending_api.common.exceptions import InvalidParameterException


logger = logging.getLogger('console')


def read_sql_file(file_path):
    # Read in SQL file and extract commands into a list
    _, file_extension = os.path.splitext(file_path)

    if file_extension != '.sql':
        raise InvalidParameterException("Invalid file provided. A file with extension '.sql' is required.")

    # Open and read the file as a single buffer
    fd = open(file_path, 'r')
    sql_file = fd.read()
    fd.close()

    # all SQL commands (split on ';') and trimmed for whitespaces
    return [command.strip() for command in sql_file.split(';') if command]


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


def build_order_by(sort_columns, sort_orders=None, nulls=None, model=Award, read_only=True):
    """
    Given columns, sort orders, and null ordering directives, build a SQL
    "order by" clause.

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
    model        - A Django model that represents a database table germaine to
                   your query.  If one is not supplied, Award will be used
                   since it is fairly central to the database as a whole.
    read_only    - Unfortunately, Django cannot understand SQL so we need to
                   explicitly tell it whether or not we intend to make changes
                   to the database.  read_only = True if we only intend to
                   query.  read_only = False if we will be making any changes
                   (UPDATE, DELETE, etc.)  The router uses this and model when
                   determining which database connection to return.

    Returns a string that contains the entire order by clause

        "order by a.whatever desc nulls first, b.whatever asc nulls last"

    or an empty string if no sort columns were provided.
    """
    # Shortcut everything if there's nothing to do.
    if not sort_columns:
        return ''

    # To simplify processing, make all of our parameters iterables of the same length.
    _sort_columns = [sort_columns] if type(sort_columns) is str else sort_columns
    _len = len(_sort_columns)
    _sort_orders = [sort_orders] * _len if type(sort_orders) in (str, type(None)) else list(sort_orders)
    _nulls = [nulls] * _len if type(nulls) in (str, type(None)) else list(nulls)

    if len(_sort_orders) != _len:
        raise ValueError(
            'Number of sort_orders (%s) does not match number of sort_columns (%s)' % (len(_sort_orders), _len)
        )
    if len(_nulls) != _len:
        raise ValueError(
            'Number of nulls (%s) does not match number of sort_columns (%s)' % (len(_nulls), _len)
        )

    order_by = []
    for column, order, null in zip(_sort_columns, _sort_orders, _nulls):
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

        order_by.append(SQL(' ').join(bits))

    return convert_composable_to_string(SQL('order by ') + SQL(', ').join(order_by), model=model, read_only=read_only)


def convert_composable_to_string(sql, connection=None, cursor=None, model=Award, read_only=True):
    """
    psycopg2 provides a nice little paradigm for safely building SQL strings
    using Composable objects.  Unfortunately, we need to cast Composable
    queries to strings before calling cursor.execute or model.objects.raw
    because the django-debug-toolbar is expecting string queries so it can
    display raw queries in its little toolbar thingy.

    On top of that, django-debug-toolbar wraps Django connections and cursors
    in an incompatible wrapper making simply calling .as_string(connection)
    on a Composable object a non-starter.

    Finally, because all of that wasn't annoying enough, cursor.execute does
    not take advantage of our custom router when choosing a Django connection.

    SO thanks to django-debug-toolbar, and Django in general, we need to jump
    through some extra hoops when converting our Composable objects into SQL
    strings.

    This function is designed to hide all that mess from the developer to some
    extent.

    sql        - A Composable SQL object.
    connection - If you already have a database connection, provide it here to
                 save us the trouble of grabbing one for you.
    cursor     - Likewise, if you have a cursor, provide it here.
    model      - A Django model that represents a database table germaine to
                 your query.  If one is not supplied, Award will be used since
                 it is fairly central to the database as a whole.
    read_only  - Unfortunately, Django cannot understand SQL so we need to
                 explicitly tell it whether or not we intend to make changes to
                 the database.  read_only = True if we only intend to query.
                 read_only = False if we will be making any changes (UPDATE,
                 DELETE, etc.)  The router uses this and model when determining
                 which database connection to return.

    So everything except sql is optional.  If connection is provided, it will
    be used above all others to convert sql to a string.  If cursor is provided
    and connection is not, it will be used.  If neither connection nor cursor
    is provided, a connection will be looked up using model and read_only.

    Returns your Composable query converted to a string.  If sql is not a
    Composable object, it will be returned as provided.
    """
    if isinstance(sql, Composable):
        if connection is not None:
            connection.ensure_connection()
            _connection = connection.connection
        elif cursor is not None:
            _connection = cursor.connection
        else:
            _connection = get_connection(model, read_only).connection
        _sql = sql.as_string(_connection)
    else:
        _sql = sql

    return _sql


def fetchall_to_ordered_dictionary(cursor):
    """
    Return all rows from a cursor as a list of ordered dictionaries.  Return
    value will be roughly equivalent to:

        [
            {'id': 54360982, 'parent_id': None},
            {'id': 54360880, 'parent_id': 54360982}
        ]

    To access values:

        my_id = rows[0]['id']
          OR
        my_id = rows[0].get('id')

    Code idea borrowed from:

        https://docs.djangoproject.com/en/2.1/topics/db/sql/#executing-custom-sql-directly

    """
    columns = [col[0] for col in cursor.description]
    return [OrderedDict(zip(columns, row)) for row in cursor.fetchall()]


def fetchall_to_named_tuple(cursor):
    """
    Return all rows from a cursor as a list of named tuples.  Return value
    will be equivalent to:

        [
            Result(id=54360982, parent_id=None),
            Result(id=54360880, parent_id=54360982)
        ]

    To access values:

        my_id = rows[0].id

    IMPORTANT:  Named tuples have restrictions on element names.  They must
    follow python identifier rules and cannot be reserved words.  Be sure to
    test your queries to ensure column names do not violate these rules.

    Code idea borrowed from:

        https://docs.djangoproject.com/en/2.1/topics/db/sql/#executing-custom-sql-directly

    """
    columns = namedtuple('Result', [col[0] for col in cursor.description])
    return [columns(*row) for row in cursor.fetchall()]


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
    and will work for most queries.

    model     - A Django model that represents a database table germaine to
                your query.  If one is not supplied, Award will be used since
                it is fairly central to the database as a whole.
    read_only - Unfortunately, Django cannot understand SQL so we need to
                explicitly tell it whether or not we intend to make changes to
                the database.  read_only = True if we only intend to query.
                read_only = False if we will be making any changes (UPDATE,
                DELETE, etc.)  The router uses this and model when determining
                which database connection to return.

    Returns an open database connection.
    """
    if read_only:
        _connection = connections[router.db_for_read(model)]
    else:
        _connection = connections[router.db_for_write(model)]
    _connection.ensure_connection()
    return _connection


def open_cursor(model=Award, read_only=True):
    """
    Shortcut for opening a cursor that takes advantage of get_connection above.
    See get_connection for more details.

    model     - A Django model that represents a database table germaine to
                your query.  If one is not supplied, Award will be used since
                it is fairly central to the database as a whole.
    read_only - Unfortunately, Django cannot understand SQL so we need to
                explicitly tell it whether or not we intend to make changes to
                the database.  read_only = True if we only intend to query.
                read_only = False if we will be making any changes (UPDATE,
                DELETE, etc.)  The router uses this and model when determining
                which database connection to return.

    Returns an open database cursor.
    """
    return get_connection(model, read_only).cursor()


def execute_sql(sql, model=Award, read_only=True, close_cursor=True):
    """
    Executes a SQL statement and returns the resulting cursor.

    sql          - Can be either a sql string statement or a psycopg2
                   Composable object (Identifier, Literal, SQL, etc).
    model        - A Django model that represents a database table germaine
                   to your query.  If one is not supplied, Award will be used
                   since it is fairly central to the database as a whole.
    read_only    - Unfortunately, Django cannot understand SQL so we need to
                   explicitly tell it whether or not we intend to make changes
                   to the database.  read_only = True if we only intend to
                   query.  read_only = False if we will be making any changes
                   (UPDATE, DELETE, etc.)  The router uses this and model when
                   determining which database connection to return.
    close_cursor - If True, the cursor is closed before it is returned,
                   otherwise it is the caller's responsibility to close the
                   cursor.  Generally speaking, if you're running a query that
                   returns results, do not close the cursor.  If performing an
                   update or delete where you do not care about any results
                   that may be returned, feel free to close the cursor.

    Returns the cursor generated as a result of running the provided sql.
    """
    cursor = open_cursor(model, read_only)
    try:
        cursor.execute(convert_composable_to_string(sql, cursor=cursor))
    finally:
        if close_cursor:
            cursor.close()
    return cursor


def fetch_all_as_ordered_dictionary(sql, model=Award):
    """
    Executes a read only sql query against a database and returns the results
    a list of ordered dictionaries.

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
    """
    with execute_sql(sql, model, read_only=True, close_cursor=False) as cursor:
        return fetchall_to_ordered_dictionary(cursor)


def fetch_all_as_named_tuple(sql, model=Award):
    """
    Executes a read only sql query against a database and returns the results
    as a list of named tuples.

    sql   - Can be either a sql string statement or a psycopg2 Composable
            object (Identifier, Literal, SQL, etc).
    model - A Django model that represents a database table germaine to your
            query.  If one is not supplied, Award will be used since it is
            fairly central to the database as a whole.

    !! IMPORTANT !! IMPORTANT !! IMPORTANT !! IMPORTANT !! IMPORTANT !!

    Named tuples have restrictions on element names.  They must follow python
    identifier rules and cannot be reserved words.  Be sure to test your
    queries to ensure your column names are legal.

    This function will read all query results into memory before returning
    them.  Only use this function if you are POSITIVE your query will not
    consume all available resources on the server.

    Additionally, this function assumes a single result from the cursor.  If
    running multiple queries in a single statement, please do not use this
    function.

    !! END IMPORTANT !! END IMPORTANT !! END IMPORTANT !! END IMPORTANT !!
    """
    with execute_sql(sql, model, read_only=True, close_cursor=False) as cursor:
        return fetchall_to_named_tuple(cursor)
