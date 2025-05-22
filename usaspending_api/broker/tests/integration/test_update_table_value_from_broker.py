import pytest
from django.core.management import call_command

from django.db import connections

from usaspending_api.settings import DATA_BROKER_DB_ALIAS, DEFAULT_DB_ALIAS


@pytest.fixture
def setup_usas_test_table():
    with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE test_update_usas (
                match_field integer,
                load_field text
            );
        """
        )
        cursor.execute(
            """
            INSERT INTO test_update_usas (match_field, load_field)
            VALUES
                (1, 'ORIGINAL TEXT 1'),
                (2, 'ORIGINAL TEXT 2');
        """
        )
    yield
    with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        cursor.execute("DROP TABLE test_update_usas;")


@pytest.fixture
def setup_broker_matching_table():
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE test_update_usas (
                match_field integer,
                load_field text
            );
        """
        )
        cursor.execute(
            """
            INSERT INTO test_update_usas (match_field, load_field)
            VALUES
                (1, 'UPDATED TEXT 1'),
                (3, 'UPDATED TEXT 3');
        """
        )
    yield
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute("DROP TABLE test_update_usas;")


@pytest.fixture
def setup_broker_different_table():
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE test_update_usas_from_broker (
                match_field_from_broker integer,
                load_field_from_broker text
            );
        """
        )
        cursor.execute(
            """
            INSERT INTO test_update_usas_from_broker (match_field_from_broker, load_field_from_broker)
            VALUES
                (1, 'UPDATED TEXT 1'),
                (3, 'UPDATED TEXT 3');
        """
        )
    yield
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute("DROP TABLE test_update_usas_from_broker;")


def validate_usas_test_data(is_updated: bool):
    with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            """
            SELECT match_field, load_field
            FROM test_update_usas
            ORDER BY match_field ASC;
        """
        )
        rows = cursor.fetchall()
    if is_updated:
        assert rows == [(1, "UPDATED TEXT 1"), (2, "ORIGINAL TEXT 2")]
    else:
        assert rows == [(1, "ORIGINAL TEXT 1"), (2, "ORIGINAL TEXT 2")]


def validate_broker_test_data(is_table_match: bool):
    sql_parts_suffix = "" if is_table_match else "_from_broker"
    with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
        cursor.execute(
            f"""
            SELECT match_field{sql_parts_suffix}, load_field{sql_parts_suffix}
            FROM test_update_usas{sql_parts_suffix}
            ORDER BY match_field{sql_parts_suffix} ASC;
        """
        )
        rows = cursor.fetchall()
    assert rows == [(1, "UPDATED TEXT 1"), (3, "UPDATED TEXT 3")]


@pytest.mark.django_db(databases=[DATA_BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_update_with_only_usas_fields(broker_server_dblink_setup, setup_usas_test_table, setup_broker_matching_table):
    validate_usas_test_data(is_updated=False)
    validate_broker_test_data(is_table_match=True)

    call_command(
        "update_table_value_from_broker",
        "--load-field-type=text",
        "--table-name=test_update_usas",
        "--load-field=load_field",
        "--match-field=match_field",
    )

    validate_usas_test_data(is_updated=True)


@pytest.mark.django_db(databases=[DATA_BROKER_DB_ALIAS, DEFAULT_DB_ALIAS], transaction=True)
def test_update_with_broker_fields(broker_server_dblink_setup, setup_usas_test_table, setup_broker_different_table):
    validate_usas_test_data(is_updated=False)
    validate_broker_test_data(is_table_match=False)

    call_command(
        "update_table_value_from_broker",
        "--load-field-type=text",
        "--table-name=test_update_usas",
        "--load-field=load_field",
        "--match-field=match_field",
        "--broker-table-name=test_update_usas_from_broker",
        "--broker-load-field=load_field_from_broker",
        "--broker-match-field=match_field_from_broker",
    )

    validate_usas_test_data(is_updated=True)
