import pytest

from psycopg2.sql import Identifier, SQL
from usaspending_api.common.helpers.etl_table import ETLTable, operations, primatives
from usaspending_api.common.helpers.sql_helpers import (
    convert_composable_query_to_string as cc,
    get_connection,
    execute_sql,
)


@pytest.mark.django_db
def test_primitives():

    assert cc(primatives.make_cast_column_list([], {})) == ""
    assert cc(primatives.make_cast_column_list(["test"], {"test": "int"})) == 'cast("test" as int) as "test"'
    assert cc(primatives.make_cast_column_list(["test"], {"test": "int"}, "t")) == 'cast("t"."test" as int) as "test"'
    assert (
        cc(primatives.make_cast_column_list(["test", "tube"], {"test": "int", "tube": "text"}, "t"))
        == 'cast("t"."test" as int) as "test", cast("t"."tube" as text) as "tube"'
    )

    assert cc(primatives.make_change_detector_conditional([], "a", "b")) == ""
    assert (
        cc(primatives.make_change_detector_conditional(["test"], "a", "b")) == '"a"."test" is distinct from "b"."test"'
    )
    assert (
        cc(primatives.make_change_detector_conditional(["test", "tube"], "a", "b"))
        == '"a"."test" is distinct from "b"."test" or "a"."tube" is distinct from "b"."tube"'
    )

    assert cc(primatives.make_column_list([])) == ""
    assert cc(primatives.make_column_list(["test"])) == '"test"'
    assert cc(primatives.make_column_list(["test"], "t")) == '"t"."test"'
    assert cc(primatives.make_column_list(["test", "tube"], "t")) == '"t"."test", "t"."tube"'

    assert cc(primatives.make_column_setter_list([], "t")) == ""
    assert cc(primatives.make_column_setter_list(["test"], "t")) == '"test" = "t"."test"'
    assert cc(primatives.make_column_setter_list(["test", "tube"], "t")) == '"test" = "t"."test", "tube" = "t"."tube"'

    assert cc(primatives.make_join_conditional([], "a", "b")) == ""
    assert cc(primatives.make_join_conditional(["test"], "a", "b")) == '"a"."test" = "b"."test"'
    assert (
        cc(primatives.make_join_conditional(["test", "tube"], "a", "b"))
        == '"a"."test" = "b"."test" and "a"."tube" = "b"."tube"'
    )

    assert cc(primatives.make_join_excluder_conditional([], "t")) == ""
    assert cc(primatives.make_join_excluder_conditional(["test"], "t")) == '"t"."test" is null'
    assert (
        cc(primatives.make_join_excluder_conditional(["test", "tube"], "t"))
        == '"t"."test" is null and "t"."tube" is null'
    )

    table = SQL("{}").format(Identifier("my_table"))
    assert cc(primatives.make_join_to_table_conditional([], "t", table)) == ""
    assert cc(primatives.make_join_to_table_conditional(["test"], "t", table)) == '"t"."test" = "my_table"."test"'
    assert (
        cc(primatives.make_join_to_table_conditional(["test", "tube"], "t", table))
        == '"t"."test" = "my_table"."test" and "t"."tube" = "my_table"."tube"'
    )

    assert cc(primatives.make_typed_column_list([], {})) == ""
    assert cc(primatives.make_typed_column_list(["test"], {"test": "int"})) == '"test" int'
    assert (
        cc(primatives.make_typed_column_list(["test", "tube"], {"test": "int", "tube": "text"}))
        == '"test" int, "tube" text'
    )


@pytest.mark.django_db
def test_etl_table():

    connection = get_connection(read_only=False)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            create table totally_tubular_testing_table_for_two (
                id1 int,
                id2 int,
                name text,
                description text,
                primary key (id1, id2)
            )
            """
        )

    table = ETLTable("totally_tubular_testing_table_for_two", "public")
    assert table.columns == ["id1", "id2", "name", "description"]
    assert dict(table.data_types) == {"id1": "integer", "id2": "integer", "name": "text", "description": "text"}
    assert table.is_dblink is False
    assert table.non_key_columns == ["name", "description"]
    assert table.key_columns == ["id1", "id2"]


@pytest.fixture
@pytest.mark.django_db
def operations_fixture():
    with get_connection(read_only=False).cursor() as cursor:
        cursor.execute(
            """
            create table t1 (
                id1 int,
                id2 int,
                name text,
                description text,
                primary key (id1, id2)
            );

            insert into t1 values (1, 2, 'three', 'four');
            insert into t1 values (4, 5, 'six', 'seven');
            insert into t1 values (8, 8, 'eight', 'eight');

            create table t2 (
                id1 int,
                id2 int,
                name text,
                description text,
                primary key (id1, id2)
            );

            insert into t2 values (1, 2, 'three', 'four');
            insert into t2 values (4, 5, 'not six', 'not seven');
            insert into t2 values (9, 9, 'nine', 'nine');
            """
        )


@pytest.mark.django_db
def test_delete_obsolete_rows(operations_fixture):
    assert execute_sql("select id1, id2 from t2 order by id1") == [(1, 2), (4, 5), (9, 9)]
    operations.delete_obsolete_rows(ETLTable("t1"), ETLTable("t2"))
    assert execute_sql("select id1, id2 from t2 order by id1") == [(1, 2), (4, 5)]


@pytest.mark.django_db
def test_identify_new_or_updated(operations_fixture):
    operations.identify_new_or_updated(ETLTable("t1"), ETLTable("t2"), ETLTable("t3"))
    assert execute_sql("select id1, id2 from t3 order by id1") == [(4, 5), (8, 8)]


@pytest.mark.django_db
def test_insert_missing_rows(operations_fixture):
    assert execute_sql("select id1, id2 from t2 order by id1") == [(1, 2), (4, 5), (9, 9)]
    operations.insert_missing_rows(ETLTable("t1"), ETLTable("t2"))
    assert execute_sql("select id1, id2 from t2 order by id1") == [(1, 2), (4, 5), (8, 8), (9, 9)]


@pytest.mark.django_db
def test_stage_dblink_table(operations_fixture):
    # There is currently no good way to test dblinked tables.  It's on my TODO list, though, fwiw.
    pass


@pytest.mark.django_db
def test_update_changed_rows(operations_fixture):
    assert execute_sql("select id1, id2, name, description from t2 order by id1") == [
        (1, 2, "three", "four"),
        (4, 5, "not six", "not seven"),
        (9, 9, "nine", "nine"),
    ]
    operations.update_changed_rows(ETLTable("t1"), ETLTable("t2"))
    assert execute_sql("select id1, id2, name, description from t2 order by id1") == [
        (1, 2, "three", "four"),
        (4, 5, "six", "seven"),
        (9, 9, "nine", "nine"),
    ]
