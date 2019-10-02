""" Functions to generate SQL for several higher level ETL operations. """


from psycopg2.sql import Literal, SQL
from typing import List
from usaspending_api.common.etl import ETLTable, primatives
from usaspending_api.common.helpers import sql_helpers


def _get_shared_columns(columns: List[str], other_columns: List[str]) -> List[str]:
    """ Return the list of columns contained in both lists. """

    shared_columns = [c for c in columns if c in other_columns]
    if not shared_columns:
        raise RuntimeError("No shared columns between tables.")
    return shared_columns


def delete_obsolete_rows(source: ETLTable, destination: ETLTable) -> int:
    """ Delete rows from destination that do not exist in source and return the number of rows deleted. """

    if source.is_dblink or destination.is_dblink:
        raise NotImplementedError("dblink tables not currently supported.")

    if not destination.key_columns:
        raise RuntimeError("The destination table must have keys defined.")

    sql = """
        delete from {destination_qualified_table_name}
        where not exists (
            select from {source_qualified_table_name} s where {join}
        )
    """

    sql = SQL(sql).format(
        destination_qualified_table_name=destination.qualified_table_name,
        source_qualified_table_name=source.qualified_table_name,
        join=primatives.make_join_to_table_conditional(destination.key_columns, "s", destination.qualified_table_name),
    )

    return sql_helpers.execute_dml_sql(sql)


def identify_new_or_updated(source: ETLTable, destination: ETLTable, staging: ETLTable) -> int:
    """
    Create a temporary staging table containing keys of rows in source that are new or
    updated from destination and return the number of rows affected.
    """

    if staging.is_dblink or destination.is_dblink or staging.is_dblink:
        raise NotImplementedError("dblink tables not currently supported.")

    if not destination.key_columns:
        raise RuntimeError("The destination table must have keys defined.")

    shared_columns = _get_shared_columns(source.columns, destination.columns)

    sql = """
        create temporary table {staging_qualified_table_name} as
        select {select_columns}
        from   {source_qualified_table_name} as s
               left outer join {destination_qualified_table_name} as d on {join}
        where  ({excluder}) or ({detect_changes})
    """

    sql = SQL(sql).format(
        staging_qualified_table_name=staging.qualified_table_name,
        select_columns=primatives.make_column_list(destination.key_columns, "s"),
        source_qualified_table_name=source.qualified_table_name,
        destination_qualified_table_name=destination.qualified_table_name,
        join=primatives.make_join_conditional(destination.key_columns, "d", "s"),
        excluder=primatives.make_join_excluder_conditional(destination.key_columns, "d"),
        detect_changes=primatives.make_change_detector_conditional(shared_columns, "s", "d"),
    )

    return sql_helpers.execute_dml_sql(sql)


def insert_missing_rows(source: ETLTable, destination: ETLTable) -> int:
    """ Insert rows from source that do not exist in destination and return the number of rows inserted. """

    if source.is_dblink or destination.is_dblink:
        raise NotImplementedError("dblink tables not currently supported.")

    if not destination.key_columns:
        raise RuntimeError("The destination table must have keys defined.")

    shared_columns = _get_shared_columns(source.columns, destination.columns)

    sql = """
        insert into {destination_qualified_table_name} ({insert_columns})
        select      {select_columns}
        from        {source_qualified_table_name} as s
                    left outer join {destination_qualified_table_name} as d on {join}
        where       {excluder}
    """

    sql = SQL(sql).format(
        destination_qualified_table_name=destination.qualified_table_name,
        insert_columns=primatives.make_column_list(shared_columns),
        select_columns=primatives.make_column_list(shared_columns, "s"),
        source_qualified_table_name=source.qualified_table_name,
        join=primatives.make_join_conditional(destination.key_columns, "d", "s"),
        excluder=primatives.make_join_excluder_conditional(destination.key_columns, "d"),
    )

    return sql_helpers.execute_dml_sql(sql)


def stage_dblink_table(source: ETLTable, destination: ETLTable, staging: ETLTable) -> int:
    """ Copy dblink source table contents to local staging table and return the number of rows copied. """

    if not source.is_dblink:
        raise RuntimeError("Source table must be a dblink table.")

    if destination.is_dblink or staging.is_dblink:
        raise NotImplementedError("Copying to a dblink table is not currently supported.")

    shared_columns = _get_shared_columns(source.columns, destination.columns)

    remote_sql = sql_helpers.convert_composable_query_to_string(
        SQL("select {columns} from {source_qualified_table_name}").format(
            columns=primatives.make_column_list(shared_columns), source_qualified_table_name=source.qualified_table_name
        )
    )

    sql = """
        create temporary table {staging_qualified_table_name} as
        select {select_columns}
        from   dblink({dblink}, {remote_sql}) as s ({typed_columns})
    """

    sql = SQL(sql).format(
        staging_qualified_table_name=staging.qualified_table_name,
        select_columns=primatives.make_column_list(shared_columns, "s"),
        dblink=Literal(source.dblink_name),
        remote_sql=Literal(remote_sql),
        typed_columns=primatives.make_typed_column_list(shared_columns, destination.data_types),
    )

    return sql_helpers.execute_dml_sql(sql)


def update_changed_rows(source: ETLTable, destination: ETLTable) -> int:
    """ Update rows in destination that have changed in source and return the number of rows updated. """

    if source.is_dblink or destination.is_dblink:
        raise NotImplementedError("dblink tables not currently supported.")

    if not destination.key_columns:
        raise RuntimeError("The destination table must have keys defined.")

    shared_columns = _get_shared_columns(source.non_key_columns, destination.non_key_columns)

    sql = """
        update      {destination_qualified_table_name} as d
        set         {set}
        from        {source_qualified_table_name} as s
        where       {where} and ({detect_changes})
    """

    sql = SQL(sql).format(
        destination_qualified_table_name=destination.qualified_table_name,
        set=primatives.make_column_setter_list(shared_columns, "s"),
        source_qualified_table_name=source.qualified_table_name,
        where=primatives.make_join_conditional(destination.key_columns, "s", "d"),
        detect_changes=primatives.make_change_detector_conditional(shared_columns, "s", "d"),
    )

    return sql_helpers.execute_dml_sql(sql)


__all__ = [
    "delete_obsolete_rows",
    "identify_new_or_updated",
    "insert_missing_rows",
    "stage_dblink_table",
    "update_changed_rows",
]
