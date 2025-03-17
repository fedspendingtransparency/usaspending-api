from psycopg2.sql import Composed, Identifier, SQL, Literal
from typing import List
from usaspending_api.common.etl.postgres import primatives
from usaspending_api.common.etl.postgres.etl_object_base import ETLObjectBase
from usaspending_api.common.etl.postgres.introspection import get_columns
from usaspending_api.common.etl.postgres.primatives import DataTypes


class ETLDBLinkTable(ETLObjectBase):
    """Represents a remote permanent database table accessed via dblink."""

    def __init__(self, table_name: str, dblink_name: str, data_types: DataTypes, schema_name: str = "public") -> None:
        self.table_name = table_name
        self.schema_name = schema_name
        self.dblink_name = dblink_name
        self.data_types = data_types
        super(ETLDBLinkTable, self).__init__()

    def _get_columns(self) -> List[str]:
        return [c for c in get_columns(self.table_name, self.schema_name, self.dblink_name) if c in self.data_types]

    def _get_object_representation(self, custom_predicate: List[dict] = None) -> Composed:
        """To help us treat a dblink table like any other object, let's wrap it in a subquery."""
        remote_table = SQL("{}.{}").format(Identifier(self.schema_name), Identifier(self.table_name))
        remote_sql = SQL("select {columns} from {remote_table}").format(
            columns=primatives.make_column_list(self.columns), remote_table=remote_table
        )
        if custom_predicate:
            predicate = self._custom_predicate(custom_predicate)
            remote_sql = remote_sql + predicate
        return SQL("({})").format(
            primatives.wrap_dblink_query(self.dblink_name, remote_sql, "r", self.columns, self.data_types)
        )

    def _custom_predicate(self, custom_predicate: List[dict]) -> Composed:
        """Add a predicate to the object representation"""
        predicate = []
        for item in custom_predicate:
            if item.get("op", "") == "IN":
                predicate.append(Identifier(item["field"]) + SQL(f" {item['op']} ") + Literal(item["values"]))
            elif item.get("op", "") == "EQUAL":
                predicate.append(Identifier(item["field"]) + SQL(" = ") + Literal(item["value"]))
            else:
                raise NotImplementedError(
                    "object_representation_custom_predicate() isn't that complex. Please add new functionality"
                )

        return SQL(" where ") + SQL(" and ").join(predicate)


__all__ = ["ETLDBLinkTable"]
