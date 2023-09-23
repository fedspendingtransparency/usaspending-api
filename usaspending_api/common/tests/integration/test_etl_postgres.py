import pytest

from collections import OrderedDict
from pathlib import Path
from psycopg2.sql import Identifier, Literal, SQL
from usaspending_api.common.etl.postgres import (
    ETLDBLinkTable,
    ETLTable,
    ETLTemporaryTable,
    ETLQueryFile,
)
from usaspending_api.common.etl.postgres import introspection, operations, primatives
from usaspending_api.common.helpers.sql_helpers import (
    convert_composable_query_to_string as cc,
    get_connection,
    execute_sql,
)
from usaspending_api.common.helpers.text_helpers import standardize_whitespace


@pytest.mark.django_db
@pytest.fixture
def make_a_table():
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


@pytest.mark.django_db
def test_primitives():

    data_types = OrderedDict(
        [
            ("test", primatives.ColumnDefinition(name="test", data_type="int", not_nullable=True)),
            ("tube", primatives.ColumnDefinition(name="tube", data_type="text", not_nullable=False)),
        ]
    )
    single_key_column = [data_types["test"]]
    double_key_column = [data_types["test"], data_types["tube"]]

    assert cc(primatives.make_cast_column_list([], {})) == ""
    assert cc(primatives.make_cast_column_list(["test"], data_types)) == 'cast("test" as int) as "test"'
    assert cc(primatives.make_cast_column_list(["test"], data_types, "t")) == 'cast("t"."test" as int) as "test"'
    assert (
        cc(primatives.make_cast_column_list(["test", "tube"], data_types, "t"))
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
    assert cc(primatives.make_column_list(["test", "tube"], "t", {"tube": SQL("now()")})) == '"t"."test", now()'

    assert cc(primatives.make_column_setter_list([], "t")) == ""
    assert cc(primatives.make_column_setter_list(["test"], "t")) == '"test" = "t"."test"'
    assert cc(primatives.make_column_setter_list(["test", "tube"], "t")) == '"test" = "t"."test", "tube" = "t"."tube"'
    assert (
        cc(primatives.make_column_setter_list(["test", "tube"], "t", {"tube": SQL("now()")}))
        == '"test" = "t"."test", "tube" = now()'
    )

    assert cc(primatives.make_composed_qualified_table_name("test")) == '"test"'
    assert cc(primatives.make_composed_qualified_table_name("test", "tube")) == '"tube"."test"'
    assert cc(primatives.make_composed_qualified_table_name("test", "tube", "t")) == '"tube"."test" as "t"'
    assert cc(primatives.make_composed_qualified_table_name("test", alias="t")) == '"test" as "t"'

    assert cc(primatives.make_join_conditional([], "a", "b")) == ""
    assert cc(primatives.make_join_conditional(single_key_column, "a", "b")) == '"a"."test" = "b"."test"'
    assert (
        cc(primatives.make_join_conditional(double_key_column, "a", "b"))
        == '"a"."test" = "b"."test" and "a"."tube" is not distinct from "b"."tube"'
    )

    assert cc(primatives.make_join_excluder_conditional([], "t")) == ""
    assert cc(primatives.make_join_excluder_conditional(single_key_column, "t")) == '"t"."test" is null'
    assert (
        cc(primatives.make_join_excluder_conditional(double_key_column, "t"))
        == '"t"."test" is null and "t"."tube" is null'
    )

    table = SQL("{}").format(Identifier("my_table"))
    assert cc(primatives.make_join_to_table_conditional([], "t", table)) == ""
    assert (
        cc(primatives.make_join_to_table_conditional(single_key_column, "t", table)) == '"t"."test" = "my_table"."test"'
    )
    assert (
        cc(primatives.make_join_to_table_conditional(double_key_column, "t", table))
        == '"t"."test" = "my_table"."test" and "t"."tube" is not distinct from "my_table"."tube"'
    )

    assert cc(primatives.make_typed_column_list([], {})) == ""
    assert cc(primatives.make_typed_column_list(["test"], data_types)) == '"test" int'
    assert cc(primatives.make_typed_column_list(["test", "tube"], data_types)) == '"test" int, "tube" text'

    assert (
        standardize_whitespace(
            cc(primatives.wrap_dblink_query("testdblink", "select now()", "r", ["test"], data_types))
        )
        == 'select "r"."test" from dblink(\'testdblink\', \'select now()\') as "r" ("test" int)'
    )


@pytest.mark.django_db
def test_etl_dblink_table():
    # We don't currently have a way to test dblink tables so we'll just initialize it to
    # make sure we can at least get that far without issues.
    ETLDBLinkTable("test_table", "test_dblink", {"id": "integer"}, "test_schema")


@pytest.mark.django_db
def test_etl_table(make_a_table):

    expected_data_types = OrderedDict(
        (
            ("id1", primatives.ColumnDefinition("id1", "integer", True)),
            ("id2", primatives.ColumnDefinition("id2", "integer", True)),
            ("name", primatives.ColumnDefinition("name", "text", False)),
            ("description", primatives.ColumnDefinition("description", "text", False)),
        )
    )

    # Happy path.
    table = ETLTable("totally_tubular_testing_table_for_two", "public")
    assert table.columns == ["id1", "id2", "name", "description"]
    assert dict(table.data_types) == expected_data_types
    assert table.update_overrides == {}
    assert table.insert_overrides == {}
    assert table.key_columns == [
        primatives.ColumnDefinition("id1", "integer", True),
        primatives.ColumnDefinition("id2", "integer", True),
    ]
    assert cc(table.object_representation) == '"public"."totally_tubular_testing_table_for_two"'

    # No schema.
    table = ETLTable("totally_tubular_testing_table_for_two")
    assert cc(table.object_representation) == '"public"."totally_tubular_testing_table_for_two"'

    # Happy overrides.
    table = ETLTable(
        "totally_tubular_testing_table_for_two",
        "public",
        key_overrides=["id1"],
        insert_overrides={"name": Literal("hello")},
        update_overrides={"description": Literal("o hi")},
    )
    assert table.columns == ["id1", "id2", "name", "description"]
    assert dict(table.data_types) == expected_data_types
    assert table.insert_overrides == {"name": Literal("hello")}
    assert table.update_overrides == {"description": Literal("o hi")}
    assert table.key_columns == [primatives.ColumnDefinition("id1", "integer", True)]
    assert cc(table.object_representation) == '"public"."totally_tubular_testing_table_for_two"'

    # Nonexistent overrides.
    table = ETLTable(
        "totally_tubular_testing_table_for_two",
        "public",
        key_overrides=["bogus"],
        insert_overrides={"bogus": SQL("now()")},
        update_overrides={"bogus": SQL("now()")},
    )
    with pytest.raises(RuntimeError):
        assert table.update_overrides is not None
    with pytest.raises(RuntimeError):
        assert table.insert_overrides is not None
    with pytest.raises(RuntimeError):
        assert table.key_columns is not None

    # Attempt to override primary keys.
    table = ETLTable(
        "totally_tubular_testing_table_for_two",
        "public",
        insert_overrides={"id1": SQL("now()")},
        update_overrides={"id1": SQL("now()")},
    )
    with pytest.raises(RuntimeError):
        assert table.update_overrides is not None
    with pytest.raises(RuntimeError):
        assert table.insert_overrides is not None

    # Everything for ETLTemporaryTable is the same as ETLTable just no schema.
    table = ETLTemporaryTable("totally_tubular_testing_table_for_two")
    assert cc(table.object_representation) == '"totally_tubular_testing_table_for_two"'


@pytest.mark.django_db
def test_etl_query_file(make_a_table, temp_file_path):
    # ETLQueryFile is 100% the same as ETLQuery, it just reads its query from a file.
    Path(temp_file_path).write_text("select id1, id2, name, description from totally_tubular_testing_table_for_two")
    query = ETLQueryFile(temp_file_path)
    assert query.columns == ["id1", "id2", "name", "description"]
    assert (
        cc(query.object_representation)
        == "(select id1, id2, name, description from totally_tubular_testing_table_for_two)"
    )


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
    operations.identify_new_or_updated(ETLTable("t1"), ETLTable("t2"), ETLTemporaryTable("t3"))
    assert execute_sql("select id1, id2 from t3 order by id1") == [(4, 5), (8, 8)]


@pytest.mark.django_db
def test_insert_missing_rows(operations_fixture):
    assert execute_sql("select id1, id2 from t2 order by id1") == [(1, 2), (4, 5), (9, 9)]
    operations.insert_missing_rows(ETLTable("t1"), ETLTable("t2"))
    assert execute_sql("select id1, id2 from t2 order by id1") == [(1, 2), (4, 5), (8, 8), (9, 9)]


@pytest.mark.django_db
def test_stage_table(operations_fixture):
    # Just copies the table.
    operations.stage_table(ETLTable("t1"), ETLTable("t2"), ETLTemporaryTable("t3"))
    assert execute_sql("select id1, id2 from t3 order by id1") == [(1, 2), (4, 5), (8, 8)]


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


@pytest.mark.django_db
def test_introspection(operations_fixture):
    expected_data_types = OrderedDict(
        (
            ("id1", primatives.ColumnDefinition("id1", "integer", True)),
            ("id2", primatives.ColumnDefinition("id2", "integer", True)),
            ("name", primatives.ColumnDefinition("name", "text", False)),
            ("description", primatives.ColumnDefinition("description", "text", False)),
        )
    )
    assert introspection.get_data_types("t1") == expected_data_types
    assert introspection.get_data_types("t1", "public") == expected_data_types
    expected = ["id1", "id2"]
    assert introspection.get_primary_key_columns("t1") == expected
    assert introspection.get_primary_key_columns("t1", "public") == expected
