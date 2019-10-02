from pathlib import Path
from psycopg2.sql import Composed
from typing import Any, Callable, Union
from usaspending_api.common.helpers.sql_helpers import execute_dml_sql
from usaspending_api.common.helpers.timing_helpers import Timer


class ETLMixin:
    """
    Some common ETL functionality encapsulated in a nice, convenient mixin.

        etl_logger_function
            If defined, some strategic logging will happen automatically.  To handle all
            of your own logging, leave None.

        etl_dml_sql_directory
            Point this at your SQL directory if you intend to take advantage of the
            _execute_etl_dml_sql_directory_file function.

    """

    etl_logger_function = None
    etl_dml_sql_directory = None
    etl_rows_affected_template = "{:,} rows affected"

    def _execute_function(
        self, function: Callable, timer_message: str = None, log_message: str = None, *args: Any, **kwargs: Any
    ) -> Any:
        """
        Execute a function and returns its results.  Times the execution if there's a
        timer_message.  Logs the result if there's a log_message.
        """

        with Timer(timer_message):
            results = function(*args, **kwargs)
            if self.etl_logger_function is not None and log_message is not None:
                self.etl_logger_function(log_message.format(results))
            return results

    def _execute_function_log_rows_affected(
        self, function: Callable, timer_message: str = None, *args: Any, **kwargs: Any
    ) -> Any:
        """ Same as _execute_function but logs rows affected much like a DML SQL statement. """

        return self._execute_function(function, timer_message, self.etl_rows_affected_template, *args, **kwargs)

    def _execute_dml_sql(self, sql: Union[str, Composed], timer_message: str = None) -> int:
        """ Execute some data manipulation SQL (INSERT, UPDATE, DELETE, etc). """

        return self._execute_function(
            execute_dml_sql, timer_message, self.etl_rows_affected_template, sql=sql
        )

    def _execute_dml_sql_file(self, file_path: str, timer_message: str = None) -> int:
        """
        Read in a SQL file and execute it.  Assumes the file's path has already been
        determined.  If no message is provided, the file named will be logged (if logging
        is enabled). """

        file_path = Path(file_path)
        return self._execute_dml_sql(file_path.read_text(), timer_message or file_path.stem)

    def _execute_etl_dml_sql_directory_file(self, file_name_no_extension: str, timer_message: str = None) -> int:
        """
        Read in a SQL file from the directory pointed to by etl_dml_sql_directory and
        execute it.  If no message is provided, the file named will be logged (if logging
        is enabled).
        """

        if self.etl_dml_sql_directory is None:
            raise RuntimeError("etl_dml_sql_directory must be defined in subclass.")

        file_path = (Path(self.etl_dml_sql_directory) / file_name_no_extension).with_suffix(".sql")
        return self._execute_dml_sql_file(Path(file_path), timer_message)


__all__ = ["ETLMixin"]
