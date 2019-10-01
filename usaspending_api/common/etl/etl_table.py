from collections import OrderedDict
from django.utils.functional import cached_property
from psycopg2.sql import Identifier, Literal, SQL
from typing import List, MutableMapping, Optional
from usaspending_api.common.helpers import sql_helpers


class ETLTable:
    """
    Represents a table in the database.  Can be a local permanent table, a local
    temporary table, or a dblinked permanent table.  Really just abstracts away much of
    the database introspection bits and encapsulates table properties to reduce function
    call interfaces.
    """

    def __init__(self, table_name: str, schema_name: Optional[str] = None, dblink_name: Optional[str] = None) -> None:
        """
        Initialize the ETL table.

            table_name  - The name of the table this object represents.
            schema_name - The schema in which the table lives.  IMPORTANT: Do not provide a
                          schema for temporary tables as they do not have a fixed schema.
            dblink_name - If the table lives in a remote database that can be queried via a
                          PostgreSQL dblink, provide its name here.

        """
        self.table_name = table_name
        self.schema_name = schema_name
        self.dblink_name = dblink_name

        if schema_name is None:
            self.qualified_table_name = SQL("{}").format(Identifier(table_name))
        else:
            self.qualified_table_name = SQL("{}.{}").format(Identifier(schema_name), Identifier(table_name))

    @cached_property
    def columns(self) -> List[str]:
        """ Returns the list of columns names from the table. """
        return list(self.data_types)

    @cached_property
    def data_types(self) -> MutableMapping[str, str]:
        """ Returns a mapping of columns names to database data types. """
        sql = """
            select      attname as column_name, pg_catalog.format_type(atttypid, atttypmod) as data_type
            from        pg_attribute
            where       attnum > 0 and attisdropped is false and attrelid = '{qualified_table_name}'::regclass
            order by    attnum
        """
        sql = SQL(sql).format(qualified_table_name=self.qualified_table_name)
        if self.is_dblink:
            inner_sql = sql_helpers.convert_composable_query_to_string(sql)
            sql = """
                select      column_name, data_type
                from        dblink({dblink_name}, {remote_sql}) as r (column_name text, data_type text)
            """
            sql = SQL(sql).format(dblink_name=Literal(self.dblink_name), remote_sql=Literal(inner_sql))
        rows = sql_helpers.execute_sql_to_ordered_dictionary(sql)
        if not rows:
            raise RuntimeError("No columns found in table.  Are you sure you have permission to see the table?")
        return OrderedDict(tuple((r["column_name"], r["data_type"]) for r in rows))

    @cached_property
    def is_dblink(self) -> bool:
        return self.dblink_name is not None

    @cached_property
    def non_key_columns(self) -> List[str]:
        """ Returns the list of columns minus the columns comprising the primary key. """
        return [c for c in self.columns if c not in self.key_columns]

    @cached_property
    def key_columns(self) -> List[str]:
        """ Returns the list of columns comprising the primary key. """
        sql = """
            select      a.attname as column_name
            from        pg_constraint as c
                        cross join unnest(c.conkey) as cols(column_number)
                        inner join pg_attribute as a on a.attrelid = c.conrelid and cols.column_number = a.attnum
            where       c.contype = 'p' and c.conrelid = '{qualified_table_name}'::regclass
        """
        sql = SQL(sql).format(qualified_table_name=self.qualified_table_name)
        if self.is_dblink:
            inner_sql = sql_helpers.convert_composable_query_to_string(sql)
            sql = """
                select      column_name
                from        dblink({dblink_name}, {remote_sql}) as r (column_name text)
            """
            sql = SQL(sql).format(dblink_name=Literal(self.dblink_name), remote_sql=Literal(inner_sql))
        rows = sql_helpers.execute_sql_to_ordered_dictionary(sql)
        return [r["column_name"] for r in rows]


__all__ = ["ETLTable"]
