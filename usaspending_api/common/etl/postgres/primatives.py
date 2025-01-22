"""
Lower level functions to help with building ETL queries.  Not intended to be used outside
of this module.
"""

from collections import namedtuple
from psycopg2.sql import Composable, Composed, Identifier, Literal, SQL
from typing import List, MutableMapping, Optional, Union
from usaspending_api.common.helpers.sql_helpers import convert_composable_query_to_string


ColumnOverrides = MutableMapping[str, Composable]  # e.g. {"updated_at": SQL("now()")}
ColumnDefinition = namedtuple("DataType", ["name", "data_type", "not_nullable"])
DataTypes = MutableMapping[str, ColumnDefinition]  # e.g. {"my_column": ("my_column", "numeric(23, 2)", True)}
KeyColumns = List[ColumnDefinition]  # e.g. [("my_column", "numeric(23, 2)", True)]


def make_cast_column_list(columns: List[str], data_types: DataTypes, alias: Optional[str] = None) -> Composed:
    """
    Turn a list of columns into a SQL safe string containing the comma separated list of
    columns cast to their appropriate data type.

    if alias provided

        cast(t.id1 as integer) as id, cast(t.name as text) as name

    if no alias provided

        cast(id1 as integer) as id, cast(name as text) as name

    """
    composed_alias = SQL("") if alias is None else SQL("{}.").format(Identifier(alias))
    template = "cast({alias}{column} as {data_type}) as {column}"
    composed_columns = [
        SQL(template).format(alias=composed_alias, column=Identifier(c), data_type=SQL(data_types[c].data_type))
        for c in columns
    ]
    return SQL(", ").join(composed_columns)


def make_change_detector_conditional(columns: List[str], left_alias: str, right_alias: str) -> Composed:
    """
    Turn a list of columns in a SQL safe string containing an ORed together list of
    conditionals for detecting changes between tables.

        s.name is distinct from d.name or
        s.description is distinct from d.description

    """
    composed_aliases = {"left_alias": Identifier(left_alias), "right_alias": Identifier(right_alias)}
    template = "{left_alias}.{column} is distinct from {right_alias}.{column}"
    composed_conditionals = [SQL(template).format(column=Identifier(c), **composed_aliases) for c in columns]
    return SQL(" or ").join(composed_conditionals)


def make_column_list(
    columns: List[str], alias: Optional[str] = None, overrides: Optional[ColumnOverrides] = None
) -> Composed:
    """
    Turn a list of columns into a SQL safe string containing the comma separated list of
    columns.

    if alias provided

        t.id1, t.id2, t.name

    if no alias provided

        id1, id2, name

    if we have overrides, potentially

        id1, id2, now()

    """
    composed_alias = SQL("") if alias is None else SQL("{}.").format(Identifier(alias))
    overrides = overrides or {}
    composed_columns = [(overrides.get(c) or SQL("{}{}").format(composed_alias, Identifier(c))) for c in columns]
    return SQL(", ").join(composed_columns)


def make_column_setter_list(columns: List[str], alias: str, overrides: Optional[ColumnOverrides] = None) -> Composed:
    """
    Turn a list of columns in a SQL safe string containing a comma separated list of
    column setters for an update statement.

        name = s.name, description = s.description

    or if we have overrides, potentially

        name = s.name, updated_at = now()

    """
    composed_alias = Identifier(alias)
    overrides = overrides or {}
    template = "{column} = {alias}.{column}"
    override_template = "{column} = {override}"
    composed_setters = [
        (
            SQL(override_template).format(column=Identifier(c), override=overrides[c])
            if c in overrides
            else SQL(template).format(alias=composed_alias, column=Identifier(c))
        )
        for c in columns
    ]
    return SQL(", ").join(composed_setters)


def make_composed_qualified_table_name(
    table_name: str, schema_name: Optional[str] = None, alias: Optional[str] = None
) -> Composed:
    """
    Turns table name and optional schema name into a Composed, qualified table name
    with optional alias suitable for insertion in Composable queries.

        "table1"
        "public"."table1"
        "public"."table1" as "t"

    """
    template = "{}"
    if schema_name is not None:
        template += ".{}"
    if alias is not None:
        template = template + " as {}"
    objects = [Identifier(o) for o in [schema_name, table_name, alias] if o is not None]
    return SQL(template).format(*objects)


def make_join_conditional(key_columns: KeyColumns, left_alias: str, right_alias: str) -> Composed:
    """
    Turn a pair of aliases and a list of key columns into a SQL safe string containing
    join conditionals ANDed together.

        s.id1 is not distinct from d.id1 and s.id2 is not distinct from d.id2

    """
    composed_aliases = {"left_alias": Identifier(left_alias), "right_alias": Identifier(right_alias)}
    template = "{left_alias}.{column} {equality} {right_alias}.{column}"
    composed_conditionals = [
        SQL(template).format(
            column=Identifier(c.name),
            equality=SQL("=" if c.not_nullable else "is not distinct from"),
            **composed_aliases,
        )
        for c in key_columns
    ]
    return SQL(" and ").join(composed_conditionals)


def make_join_excluder_conditional(key_columns: KeyColumns, alias: str) -> Composed:
    """
    Turn a list of key columns into a SQL safe string containing join excluder
    conditionals ANDed together.

        s.id1 is null and s.id2 is null

    """
    composed_alias = Identifier(alias)
    return SQL(" and ").join([SQL("{}.{} is null").format(composed_alias, Identifier(c.name)) for c in key_columns])


def make_join_to_table_conditional(key_columns: KeyColumns, alias: str, object_representation: Composed) -> Composed:
    """
    Turn an alias, table, and a list of key columns into a SQL safe string containing
    join conditionals ANDed together.

        d.id1 is not distinct from public.table1.id1 and d.id2 is not distinct from public.table1.id2

    """
    composed_aliases = {"left_alias": Identifier(alias), "right_alias": object_representation}
    template = "{left_alias}.{column} {equality} {right_alias}.{column}"
    composed_conditionals = [
        SQL(template).format(
            column=Identifier(c.name),
            equality=SQL("=" if c.not_nullable else "is not distinct from"),
            **composed_aliases,
        )
        for c in key_columns
    ]
    return SQL(" and ").join(composed_conditionals)


def make_typed_column_list(columns: List[str], data_types: DataTypes) -> Composed:
    """
    Turn a list of columns into a SQL safe string containing the comma separated list of
    typed columns.  data_types must be a mapping of column names to data types that are
    safe for SQL as they will be injected untouched.

        id integer, name text

    """
    composed_columns = [SQL("{} {}").format(Identifier(c), SQL(data_types[c].data_type)) for c in columns]
    return SQL(", ").join(composed_columns)


def wrap_dblink_query(
    dblink_name: str, sql: Union[str, Composed], alias: str, columns: List[str], data_types: DataTypes
) -> Composed:
    """Wraps a query in a dblink compatible query so that it can be run on a remote server."""
    inner_sql = convert_composable_query_to_string(sql)
    select_columns = make_column_list(columns, alias)
    typed_columns = make_typed_column_list(columns, data_types)
    sql = """
        select {select_columns}
        from   dblink({dblink}, {remote_sql}) as {alias} ({typed_columns})
    """
    return SQL(sql).format(
        select_columns=select_columns,
        dblink=Literal(dblink_name),
        remote_sql=Literal(inner_sql),
        alias=Identifier(alias),
        typed_columns=typed_columns,
    )


__all__ = [
    "ColumnOverrides",
    "ColumnDefinition",
    "DataTypes",
    "KeyColumns",
    "make_cast_column_list",
    "make_change_detector_conditional",
    "make_column_list",
    "make_column_setter_list",
    "make_composed_qualified_table_name",
    "make_join_conditional",
    "make_join_excluder_conditional",
    "make_join_to_table_conditional",
    "make_typed_column_list",
    "wrap_dblink_query",
]
