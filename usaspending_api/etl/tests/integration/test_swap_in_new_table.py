from django.core.management import call_command
from django.db import connection
from pytest import mark

from usaspending_api.common.helpers.sql_helpers import ordered_dictionary_fetcher


@mark.django_db()
def test_old_table_exists_validation():
    try:
        call_command("swap_in_new_table", "--table=test_table")
    except Exception as e:
        assert str(e) == "There are no tables matching: test_table"
    else:
        assert False, "No exception was raised"


@mark.django_db()
def test_new_table_exists_validation():
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE test_table (col1 TEXT)")
    try:
        call_command("swap_in_new_table", "--table=test_table")
    except Exception as e:
        assert str(e) == "There are no tables matching: test_table_temp"
    else:
        assert False, "No exception was raised"


@mark.django_db()
def test_index_validation():
    with connection.cursor() as cursor:
        # Test that the same number of indexes exist on the old and new table
        cursor.execute(
            "CREATE TABLE test_table (col1 TEXT, col2 INT);"
            "CREATE TABLE test_table_temp (col1 TEXT, col2 INT);"
            "CREATE INDEX test_table_col1_index ON test_table(col1);"
            "CREATE INDEX test_table_col2_index ON test_table(col2);"
            "CREATE INDEX test_table_col1_index_temp ON test_table_temp(col1);"
        )
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == "The number of indexes are different for the tables: test_table_temp and test_table"
        else:
            assert False, "No exception was raised"

        # Test that the indexes have the same name after removing appended "_temp" from the name
        cursor.execute("CREATE INDEX test_table_wrong_col2_index_temp ON test_table_temp(col2)")
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == "The index definitions are different for the tables: test_table_temp and test_table"
        else:
            assert False, "No exception was raised"

        # Test that the indexes have the same definition after removing "_temp"
        cursor.execute(
            "DROP INDEX test_table_wrong_col2_index_temp;"
            "CREATE INDEX test_table_col2_index_temp ON test_table_temp(col2 NULLS FIRST);"
        )
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == "The index definitions are different for the tables: test_table_temp and test_table"
        else:
            assert False, "No exception was raised"


@mark.django_db()
def test_constraint_validation():
    with connection.cursor() as cursor:
        # Test that Foreign Keys are not allowed by default
        cursor.execute(
            "CREATE TABLE test_table (col1 TEXT, col2 INT);"
            "CREATE TABLE test_table_temp (col1 TEXT, col2 INT);"
            "ALTER TABLE test_table_temp ADD CONSTRAINT test_table_award_fk_temp FOREIGN KEY (col2) REFERENCES awards (id);"
        )
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == (
                "Foreign Key constraints are not allowed on 'test_table_temp' or 'test_table'."
                " It is advised to not allow Foreign Key constraints on swapped tables to avoid potential deadlock."
                " However, if needed they can be allowed with the `--allow-foreign-key` flag."
            )
        else:
            assert False, "No exception was raised"

        # Test that Foreign Keys are allowed with the use of '--allow-foreign-key';
        # This causes the next validation to fail expecting an even number of constraints
        try:
            call_command("swap_in_new_table", "--table=test_table", "--allow-foreign-key")
        except Exception as e:
            assert str(e) == ("The number of constraints are different for the tables: test_table_temp and test_table.")
        else:
            assert False, "No exception was raised"

        # Test that the same number of constraints exist on the old and new table
        cursor.execute(
            "ALTER TABLE test_table_temp DROP CONSTRAINT test_table_award_fk_temp;"
            "ALTER TABLE test_table ADD CONSTRAINT test_table_col_1_constraint CHECK (col1 != 'TEST');"
        )
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == ("The number of constraints are different for the tables: test_table_temp and test_table.")
        else:
            assert False, "No exception was raised"

        # Test that the constraints have the same name after removing appended "_temp" from the name
        cursor.execute(
            "ALTER TABLE test_table_temp ADD CONSTRAINT test_table_wrong_col_1_constraint_temp CHECK (col1 != 'TEST')"
        )
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == (
                "The constraint definitions are different for the tables: test_table_temp and test_table."
            )
        else:
            assert False, "No exception was raised"

        # Test that two CHECK constraints with different CHECK CLAUSE will fail
        cursor.execute(
            "ALTER TABLE test_table_temp DROP CONSTRAINT test_table_wrong_col_1_constraint_temp;"
            "ALTER TABLE test_table_temp ADD CONSTRAINT test_table_col_1_constraint_temp CHECK (col1 != 'TEST_WRONG');"
        )
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == (
                "The constraint definitions are different for the tables: test_table_temp and test_table."
            )
        else:
            assert False, "No exception was raised"


@mark.django_db()
def test_column_validation():
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE test_table (col1 TEXT, col2 INT);"
            "CREATE TABLE test_table_temp (col1 TEXT, col2 INT, col3 INT);"
        )
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == (f"The number of columns are different for the tables: test_table_temp and test_table.")
        else:
            assert False, "No exception was raised"

        cursor.execute(
            "DROP TABLE test_table;"
            "DROP TABLE test_table_temp;"
            "CREATE TABLE test_table (col1 TEXT, col2 INT);"
            "CREATE TABLE test_table_temp (col1 TEXT, col2 TEXT);"
        )
        try:
            call_command("swap_in_new_table", "--table=test_table")
        except Exception as e:
            assert str(e) == (f"The column definitions are different for the tables: test_table_temp and test_table.")
        else:
            assert False, "No exception was raised"


@mark.django_db()
def test_happy_path():
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE test_table (col1 TEXT, col2 INT);"
            "CREATE TABLE test_table_temp (col1 TEXT, col2 INT);"
            "INSERT INTO test_table (col1, col2) VALUES ('goodbye', 1);"
            "INSERT INTO test_table_temp (col1, col2) VALUES ('hello', 2), ('world', 3);"
            "CREATE INDEX test_table_col1_index ON test_table(col1);"
            "CREATE INDEX test_table_col1_index_temp ON test_table_temp(col1);"
            "ALTER TABLE test_table ADD CONSTRAINT test_table_col_1_constraint CHECK (col1 != 'TEST');"
            "ALTER TABLE test_table_temp ADD CONSTRAINT test_table_col_1_constraint_temp CHECK (col1 != 'TEST');"
        )
        call_command("swap_in_new_table", "--table=test_table")
        cursor.execute("SELECT * FROM test_table ORDER BY col1")
        result = ordered_dictionary_fetcher(cursor)
        assert result == [{"col1": "hello", "col2": 2}, {"col1": "world", "col2": 3}]

        cursor.execute(
            "SELECT * FROM information_schema.tables WHERE table_name IN ('test_table_temp', 'test_table_old')"
        )
        result = cursor.fetchall()
        assert len(result) == 0
