from pathlib import Path
from psycopg2.sql import Composed
from typing import Any, Callable, Optional, Union
from usaspending_api.common.etl.postgres import ETLObjectBase
from usaspending_api.common.etl.postgres.operations import (
    delete_obsolete_rows,
    insert_missing_rows,
    update_changed_rows,
)
from usaspending_api.common.helpers.sql_helpers import execute_dml_sql
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer


class ETLMixin:
    """Some common ETL functionality encapsulated in a nice, convenient mixin."""

    etl_logger_function = None
    etl_dml_sql_directory = None
    etl_rows_affected_template = "{:,} rows affected"
    etl_timer = Timer

    def _delete_update_insert_rows(self, what: str, source: ETLObjectBase, destination: ETLObjectBase):
        """Convenience function to run delete, update, and create ETL operations."""
        rows_affected = 0
        rows_affected += self._execute_function_and_log(
            delete_obsolete_rows, "Delete obsolete {}".format(what), source, destination
        )
        rows_affected += self._execute_function_and_log(
            update_changed_rows, "Update changed {}".format(what), source, destination
        )
        rows_affected += self._execute_function_and_log(
            insert_missing_rows, "Insert missing {}".format(what), source, destination
        )
        return rows_affected

    def _execute_dml_sql(self, sql: Union[str, Composed], timer_message: Optional[str] = None) -> int:
        """Execute some data manipulation SQL (INSERT, UPDATE, DELETE, etc)."""

        return self._log_rows_affected(self._execute_function(execute_dml_sql, timer_message, sql=sql))

    def _execute_dml_sql_file(self, file_path: [str, Path], timer_message: Optional[str] = None) -> int:
        """
        Read in a SQL file and execute it.  Assumes the file's path has already been
        determined.  If no message is provided, the file named will be logged (if logging
        is enabled)."""

        file_path = Path(file_path)
        return self._execute_dml_sql(file_path.read_text(), timer_message or file_path.stem)

    def _execute_etl_dml_sql_directory_file(
        self, file_name_no_extension: str, timer_message: Optional[str] = None
    ) -> int:
        """
        Read in a SQL file from the directory pointed to by etl_dml_sql_directory and
        execute it.  If no message is provided, the file named will be logged (if logging
        is enabled).
        """

        if self.etl_dml_sql_directory is None:
            raise RuntimeError("etl_dml_sql_directory must be defined in subclass.")

        file_path = self._get_sql_directory_file_path(file_name_no_extension)
        return self._execute_dml_sql_file(file_path, timer_message)

    def _execute_function(
        self, function: Callable, timer_message: Optional[str] = None, *args: Any, **kwargs: Any
    ) -> Any:
        """
        Execute a function and returns its results.  Times the execution if there's a
        timer_message.  Logs the result if there's a log_message.
        """

        with self.etl_timer(timer_message):
            return function(*args, **kwargs)

    def _execute_function_and_log(
        self, function: Callable, timer_message: Optional[str] = None, *args: Any, **kwargs: Any
    ) -> Any:
        """Same as _execute_function but logs rows affected much like a DML SQL statement."""

        return self._log_rows_affected(self._execute_function(function, timer_message, *args, **kwargs))

    def _get_sql_directory_file_path(self, file_name_no_extension: str) -> Path:
        return (Path(self.etl_dml_sql_directory) / file_name_no_extension).with_suffix(".sql")

    def _log_rows_affected(self, row_count: int) -> int:
        """If we have something we can log and a way to log it, log it."""

        if (
            self.etl_logger_function is not None
            and self.etl_rows_affected_template is not None
            and type(row_count) is int
            and row_count > -1
        ):
            self.etl_logger_function(self.etl_rows_affected_template.format(row_count))
        return row_count


__all__ = ["ETLMixin"]
