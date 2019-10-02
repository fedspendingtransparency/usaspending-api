from django.utils.functional import cached_property
from psycopg2.sql import Identifier, SQL
from typing import List, Optional
from usaspending_api.common.etl.introspection import get_columns_and_data_types, get_primary_key_columns
from usaspending_api.common.etl.primatives import DataTypes


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
    def data_types(self) -> DataTypes:
        """ Returns a mapping of columns names to database data types. """
        data_types = get_columns_and_data_types(self.table_name, self.schema_name, self.dblink_name)
        if not data_types:
            raise RuntimeError("No columns found in table.  Are you sure you have permission to see the table?")
        return data_types

    @cached_property
    def is_dblink(self) -> bool:
        """ Abstract away how we determine if this table represents a dblinked table. """
        return self.dblink_name is not None

    @cached_property
    def non_key_columns(self) -> List[str]:
        """ Returns the list of columns minus the columns comprising the primary key. """
        return [c for c in self.columns if c not in self.key_columns]

    @cached_property
    def key_columns(self) -> List[str]:
        """ Returns the list of columns comprising the primary key. """
        return get_primary_key_columns(self.table_name, self.schema_name, self.dblink_name)


__all__ = ["ETLTable"]
