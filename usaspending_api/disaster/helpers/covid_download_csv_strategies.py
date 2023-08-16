from abc import ABC, abstractmethod
import multiprocessing
import time
import logging
from pathlib import Path
from typing import Tuple

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
from pyspark.sql import SparkSession
from usaspending_api.common.etl.spark import load_csv_file_and_zip
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session


class AbstractCovidToCSVStrategy(ABC):
    """A composable class that can be used according to the Strategy software design pattern.
    The Covid-19 "to csv" strategy establishes the interface for a suite of download
    algorithms; which take data from a source and outputs the result set to a csv.
    Implement this abstract class by taking specific algorithms which pull data from a source,
    and outputs to a csv, and bundle them into separate classes called strategies which
    inherit from this base class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_format = "csv"

    @abstractmethod
    def download_to_csv(
        self,
        sql_file_path: Path,
        destination_path: Path,
        intermediate_data_filename: str,
        working_dir_path: Path,
        zip_file_path: Path,
    ) -> Tuple[str, int]:
        """
        Args:
            sql_file_path: The path to the SQL file that'll be used to source data
            destination_path: The absolute destination path of the generated data files
            intermediate_data_filename: Some path to store the temporary download in
            working_dir_path: The working directory
            zip_file_path: The path to zip the generated data files to

        """
        pass


class PostgresCovidToCSVStrategy(AbstractCovidToCSVStrategy):
    def __init__(self, logger: logging.Logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logger

    def download_to_csv(
        self, sql_file_path, destination_path, intermediate_data_filename, working_dir_path, zip_file_path
    ):
        start_time = time.perf_counter()
        self._logger.info(f"Downloading data to {destination_path}")
        options = FILE_FORMATS[self.file_format]["options"]
        export_query = r"\COPY ({}) TO STDOUT {}".format(read_sql_file_to_text(sql_file_path), options)
        try:
            temp_file, temp_file_path = generate_export_query_temp_file(export_query, None, working_dir_path)
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
            except Exception:
                self._logger.exception("Unable to obtain delimited text file line count")

            start_time = time.perf_counter()
            zip_process = multiprocessing.Process(
                target=split_and_zip_data_files,
                args=(
                    str(zip_file_path),
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


class SparkCovidToCSVStrategy(AbstractCovidToCSVStrategy):
    def __init__(self, logger: logging.Logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logger

    def download_to_csv(self, sql_file_path, destination_path, intermediate_data_filename, zip_file_path):
        try:
            extra_conf = {
                # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                # See comment below about old date and time values cannot be parsed without these
                "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
                "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
                "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
            }
            spark = get_active_spark_session()
            if not spark:
                self.spark_created_by_command = True
                self.spark = configure_spark_session(**extra_conf, spark_context=spark)  # type: SparkSession
            df = self.spark.sql(sql_file_path.read_text())
            record_count = load_csv_file_and_zip(self.spark, df, destination_path, logger=self._logger)
            self._logger.info(f"{destination_path} contains {record_count:,} rows of data")
        except Exception:
            self._logger.exception("Exception encountered. See logs")
            raise
        finally:
            if self.spark_created_by_command:
                self.spark.stop()
        return destination_path, record_count