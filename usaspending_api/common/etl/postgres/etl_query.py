from psycopg2.sql import Composed, SQL
from typing import List
from usaspending_api.common.etl.postgres import ETLObjectBase
from usaspending_api.common.etl.postgres.introspection import get_query_columns


class ETLQuery(ETLObjectBase):
    """
    Represents a read only SQL query.  Very similar to the concept of a common table
    expression in that this query will be treated like a table as much as is feasible.
    """

    def __init__(self, sql: str) -> None:
        self.sql = sql
        super(ETLQuery, self).__init__()

    def _get_columns(self) -> List[str]:
        return get_query_columns(self.sql)

    def _get_object_representation(self) -> Composed:
        return SQL("({})").format(SQL(self.sql))


__all__ = ["ETLQuery"]
