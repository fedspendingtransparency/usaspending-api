from pytest import raises

from unittest.mock import patch, MagicMock

from django.core.management import call_command
from django.core.management.base import CommandError


# Argument Validation


def test_both_sql_and_file_raises_error():
    with raises(
        CommandError,
        match="Cannot use both --sql and --file. Choose one.",
    ):
        call_command("execute_spark_sql", "--sql", "SHOW DATABASES;", "--file", "test.sql")


def test_no_sql_and_file_raises_error():
    with raises(
        CommandError,
        match="Either --sql or --file must be provided",
    ):
        call_command("execute_spark_sql")


# Integration Tests


def test_sql_functionality():
    """Tests a variety of cases in a single test for Spark performance"""

    # Test --sql approach
    with patch("usaspending_api.etl.management.commands.execute_spark_sql.logger") as mock_logger:
        call_command("execute_spark_sql", "--sql", "SHOW DATABASES; SELECT 1;")
        mock_logger.info.assert_any_call("Found 2 SQL statement(s)")

    # Test --file approach with two SQL statements and a blank line
    test_file_path = "usaspending_api/etl/tests/data/test_execute_spark_files/two_queries_with_blank_line.sql"
    with patch("usaspending_api.etl.management.commands.execute_spark_sql.logger") as mock_logger:
        call_command("execute_spark_sql", "--file", test_file_path)
        mock_logger.info.assert_any_call("Found 2 SQL statement(s)")

    # Test --file approach with an empty file
    test_file_path = "usaspending_api/etl/tests/data/test_execute_spark_files/empty_file.sql"
    with patch("usaspending_api.etl.management.commands.execute_spark_sql.logger") as mock_logger:
        call_command("execute_spark_sql", "--file", test_file_path)
        mock_logger.info.assert_any_call("Found 0 SQL statement(s)")

    # Test SQL executes
    mock_spark = MagicMock()
    with patch(
        "usaspending_api.etl.management.commands.execute_spark_sql.get_active_spark_session", return_value=mock_spark
    ):
        call_command("execute_spark_sql", "--sql", "SELECT 1;")
        mock_spark.sql.assert_called()

    # Test --dry-run does not execute SQL
    mock_spark = MagicMock()
    with patch(
        "usaspending_api.etl.management.commands.execute_spark_sql.get_active_spark_session", return_value=mock_spark
    ):
        call_command("execute_spark_sql", "--sql", "SELECT 1;", "--dry-run")
        mock_spark.sql.assert_not_called()
