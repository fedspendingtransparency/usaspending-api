from collections import OrderedDict
from psycopg2.sql import SQL
from typing import List
from usaspending_api.common.etl.primatives import DataTypes, make_composed_qualified_table_name, wrap_dblink_query
from usaspending_api.common.helpers import sql_helpers


def get_columns_and_data_types(table_name: str, schema_name: str = None, dblink_name: str = None) -> DataTypes:
    """ Grab column names and database data types. """
    table = make_composed_qualified_table_name(table_name, schema_name)
    sql = """
        select    attname as column_name, pg_catalog.format_type(atttypid, atttypmod) as data_type
        from      pg_attribute
        where     attnum > 0 and attisdropped is false and attrelid = '{table}'::regclass
        order by  attnum
    """
    sql = SQL(sql).format(table=table)
    if dblink_name is not None:
        data_types = OrderedDict((("column_name", "text"), ("data_type", "text")))
        sql = wrap_dblink_query(dblink_name, sql, "r", list(data_types), data_types)
    rows = sql_helpers.execute_sql_to_ordered_dictionary(sql)
    return OrderedDict(tuple((r["column_name"], r["data_type"]) for r in rows))


def get_primary_key_columns(table_name: str, schema_name: str = None, dblink_name: str = None) -> List[str]:
    """ Grab column names comprising the primary key. """
    table = make_composed_qualified_table_name(table_name, schema_name)
    sql = """
        select      a.attname as column_name
        from        pg_constraint as c
                    cross join unnest(c.conkey) as cols(column_number)
                    inner join pg_attribute as a on a.attrelid = c.conrelid and cols.column_number = a.attnum
        where       c.contype = 'p' and c.conrelid = '{table}'::regclass
    """
    sql = SQL(sql).format(table=table)
    if dblink_name is not None:
        data_types = OrderedDict([("column_name", "text")])
        sql = wrap_dblink_query(dblink_name, sql, "r", list(data_types), data_types)
    rows = sql_helpers.execute_sql_to_ordered_dictionary(sql)
    return [r["column_name"] for r in rows]


__all__ = ["get_columns_and_data_types", "get_primary_key_columns"]
