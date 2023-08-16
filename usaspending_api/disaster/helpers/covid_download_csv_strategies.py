from abc import ABC, abstractmethod
import multiprocessing
import time
import logging
from pathlib import Path

from usaspending_api.common.csv_helpers import count_rows_in_delimited_file
from usaspending_api.common.helpers.sql_helpers import read_sql_file_to_text
from usaspending_api.download.filestreaming.download_generation import (
    split_and_zip_data_files,
    wait_for_process,
    execute_psql,
    generate_export_query_temp_file,
)
from usaspending_api.download.lookups import FILE_FORMATS
from usaspending_api.download.filestreaming.download_generation import generate_export_query_temp_file


class AbstractCovidToCSVStrategy(ABC):
    """A composable class that can be used according to the Strategy software design pattern.
    The Covid-19 "to csv" strategy establishes the interface for a suite of download
    algorithms; which take data from a source and outputs the result set to a csv.
    Implement this abstract class by taking specific algorithms which pull data from a source,
    and outputs to a csv, and bundle them into separate classes called strategies which
    inherit from this base class.
    """

    @abstractmethod
    def download_to_csv(self, sql_filepath, destination_path, intermediate_data_filename) -> tuple(str, int):
        pass


class PostgresCovidToCSVStrategy(AbstractCovidToCSVStrategy):
    def __init__(self, logger: logging.Logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logger

    def download_to_csv(self, sql_filepath, destination_path, intermediate_data_filename):
        start_time = time.perf_counter()
        self._logger.info(f"Downloading data to {destination_path}")
        options = FILE_FORMATS[self.file_format]["options"]
        export_query = r"\COPY ({}) TO STDOUT {}".format(read_sql_file_to_text(sql_filepath), options)
        try:
            temp_file, temp_file_path = generate_export_query_temp_file(export_query, None, self.working_dir_path)
            # Create a separate process to run the PSQL command; wait
            psql_process = multiprocessing.Process(
                target=execute_psql, args=(temp_file_path, intermediate_data_filename, None)
            )
            psql_process.start()
            wait_for_process(psql_process, start_time, None)

            delim = FILE_FORMATS[self.file_format]["delimiter"]

            # Log how many rows we have
            self._logger.info(f"Counting rows in delimited text file {intermediate_data_filename}")
            try:
                count = count_rows_in_delimited_file(
                    filename=intermediate_data_filename, has_header=True, delimiter=delim
                )
                self._logger.info(f"{destination_path} contains {count:,} rows of data")
                self.total_download_count += count
            except Exception:
                self._logger.exception("Unable to obtain delimited text file line count")

            start_time = time.perf_counter()
            zip_process = multiprocessing.Process(
                target=split_and_zip_data_files,
                args=(
                    str(self.zip_file_path),
                    intermediate_data_filename,
                    str(destination_path),
                    self.file_format,
                    None,
                ),
            )
            zip_process.start()
            wait_for_process(zip_process, start_time, None)
        except Exception as e:
            raise e
        finally:
            Path(temp_file_path).unlink()
        return destination_path, count