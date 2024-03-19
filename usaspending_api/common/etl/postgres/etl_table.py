from psycopg2.sql import Composed, Identifier, SQL
from typing import List, Optional
from usaspending_api.common.etl.postgres.etl_writable_object_base import ETLWritableObjectBase
from usaspending_api.common.etl.postgres.introspection import get_columns, get_data_types, get_primary_key_columns
from usaspending_api.common.etl.postgres.primatives import ColumnOverrides, DataTypes


class ETLTable(ETLWritableObjectBase):
    """ Represents a local permanent database table. """

    def __init__(
        self,
        table_name: str,
        schema_name: str = "public",
        key_overrides: Optional[List[str]] = None,
        insert_overrides: Optional[ColumnOverrides] = None,
        update_overrides: Optional[ColumnOverrides] = None,
    ) -> None:
        self.table_name = table_name
        self.schema_name = schema_name
        super(ETLTable, self).__init__(key_overrides, insert_overrides, update_overrides)

    def _get_columns(self) -> List[str]:
        return get_columns(self.table_name, self.schema_name)

    def _get_primary_key_columns(self) -> List[str]:
        return get_primary_key_columns(self.table_name, self.schema_name)

    def _get_data_types(self) -> DataTypes:
        return get_data_types(self.table_name, self.schema_name)

    def _get_object_representation(self) -> Composed:
        return SQL("{}.{}").format(Identifier(self.schema_name), Identifier(self.table_name))


__all__ = ["ETLTable"]
