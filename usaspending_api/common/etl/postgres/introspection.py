from collections import OrderedDict
from psycopg2.sql import SQL
from typing import List, Optional
from usaspending_api.common.etl.postgres.primatives import (
    ColumnDefinition,
    DataTypes,
    make_composed_qualified_table_name,
    wrap_dblink_query,
)
from usaspending_api.common.helpers import sql_helpers


_columns = OrderedDict([("column_name", ColumnDefinition("column_name", "text", False))])

_data_types = OrderedDict(
    (
        ("column_name", ColumnDefinition("column_name", "text", False)),
        ("data_type", ColumnDefinition("data_type", "text", False)),
        ("not_nullable", ColumnDefinition("not_nullable", "boolean", False)),
    )
)


def _get_whatever(table_name: str, schema_name: str, dblink_name: str, sql: str, data_types: DataTypes):
    """The common bits of subsequent functions."""
    table = make_composed_qualified_table_name(table_name, schema_name)
    sql = SQL(sql).format(table=table)
    if dblink_name is not None:
        sql = wrap_dblink_query(dblink_name, sql, "r", list(data_types), data_types)
    # IMPORTANT:  Even though this is a read only operation, since this is being run in support of
    # a writable operation, we need to run it against the writable connection else we will be
    # unable to see objects living in our transaction if there is one.
    return sql_helpers.execute_sql_to_ordered_dictionary(sql, read_only=False)


def get_columns(table_name: str, schema_name: Optional[str] = None, dblink_name: Optional[str] = None) -> List[str]:
    """Grab column names from the table."""
    sql = """
        select    attname as column_name
        from      pg_attribute
        where     attnum > 0 and attisdropped is false and attrelid = '{table}'::regclass
        order by  attnum
    """
    rows = _get_whatever(table_name, schema_name, dblink_name, sql, _columns)
    return [r["column_name"] for r in rows]


def get_data_types(table_name: str, schema_name: Optional[str] = None, dblink_name: Optional[str] = None) -> DataTypes:
    """Grab column names and database data types."""
    sql = """
        select    attname as column_name,
                  pg_catalog.format_type(atttypid, atttypmod) as data_type,
                  attnotnull as not_nullable
        from      pg_attribute
        where     attnum > 0 and attisdropped is false and attrelid = '{table}'::regclass
        order by  attnum
    """
    rows = _get_whatever(table_name, schema_name, dblink_name, sql, _data_types)
    return OrderedDict(
        tuple(
            (
                r["column_name"],
                ColumnDefinition(name=r["column_name"], data_type=r["data_type"], not_nullable=r["not_nullable"]),
            )
            for r in rows
        )
    )


def get_primary_key_columns(
    table_name: str, schema_name: Optional[str] = None, dblink_name: Optional[str] = None
) -> List[str]:
    """Grab column names comprising the primary key."""
    sql = """
        select  a.attname as column_name
        from    pg_constraint as c
                cross join unnest(c.conkey) as cols(column_number)
                inner join pg_attribute as a on a.attrelid = c.conrelid and cols.column_number = a.attnum
        where   c.contype = 'p' and c.conrelid = '{table}'::regclass
    """
    rows = _get_whatever(table_name, schema_name, dblink_name, sql, _columns)
    return [r["column_name"] for r in rows]


def get_query_columns(sql: str) -> List[str]:
    """Run a NOOP version of the query so we can ascertain its columns."""
    sql = SQL("select * from ({}) as t where false").format(SQL(sql))
    # IMPORTANT:  Even though this is a read only operation, since this is being run in support of
    # a writable operation, we need to run it against the writable connection else we will be
    # unable to see objects living in our transaction if there is one.
    cursor = sql_helpers.execute_sql(sql, fetcher=sql_helpers.cursor_fetcher, read_only=False)
    return [col[0] for col in cursor.description]


__all__ = ["get_columns", "get_data_types", "get_primary_key_columns", "get_query_columns"]
