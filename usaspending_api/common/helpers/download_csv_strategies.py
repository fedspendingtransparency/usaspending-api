import logging
import multiprocessing
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from django.conf import settings
from pyspark.sql import DataFrame
from usaspending_api.common.csv_helpers import count_rows_in_delimited_file
from usaspending_api.common.helpers.s3_helpers import delete_s3_objects, download_s3_object
from usaspending_api.download.filestreaming.download_generation import (
    EXCEL_ROW_LIMIT,
    split_and_zip_data_files,
    wait_for_process,
    execute_psql,
    generate_export_query_temp_file,
)
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.lookups import FILE_FORMATS


@dataclass
class CSVDownloadMetadata:
    filepaths: list[str]
    number_of_rows: int
    number_of_columns: Optional[int] = None


class AbstractToCSVStrategy(ABC):
    """A composable class that can be used according to the Strategy software design pattern.
    The "to csv" strategy establishes the interface for a suite of download
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
        source_sql: str | None,
        destination_path: Path,
        destination_file_name: str,
        working_dir_path: Path,
        download_zip_path: Path,
        source_df: DataFrame | None = None,
    ) -> CSVDownloadMetadata:
        """
        Args:
            source_sql: Some string that can be used as the source sql
            destination_path: The absolute destination path of the generated data files as a string
            destination_file_name: The name of the file in destination path without a file extension
            working_dir_path: The working directory path as a string
            download_zip_path: The path (as a string) to the download zip file
            source_df: A pyspark DataFrame that contains the data to be downloaded

        Returns:
            Returns a CSVDownloadMetadata object (a dataclass containing metadata about the download)
        """
        pass


class PostgresToCSVStrategy(AbstractToCSVStrategy):
    def __init__(self, logger: logging.Logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logger

    def download_to_csv(
        self, source_sql, destination_path, destination_file_name, working_dir_path, download_zip_path, source_df=None
    ):
        start_time = time.perf_counter()
        self._logger.info(f"Downloading data to {destination_path}")
        temp_data_file_name = destination_path.parent / (destination_path.name + "_temp")
        options = FILE_FORMATS[self.file_format]["options"]
        export_query = r"\COPY ({}) TO STDOUT {}".format(source_sql, options)
        try:
            temp_file, temp_file_path = generate_export_query_temp_file(export_query, None, working_dir_path)
            # Create a separate process to run the PSQL command; wait
            psql_process = multiprocessing.Process(
                target=execute_psql, args=(temp_file_path, temp_data_file_name, None)
            )
            psql_process.start()
            wait_for_process(psql_process, start_time, None)

            delim = FILE_FORMATS[self.file_format]["delimiter"]

            # Log how many rows we have
            self._logger.info(f"Counting rows in delimited text file {temp_data_file_name}")
            try:
                row_count = count_rows_in_delimited_file(filename=temp_data_file_name, has_header=True, delimiter=delim)
                self._logger.info(f"{destination_path} contains {row_count:,} rows of data")
            except Exception:
                self._logger.exception("Unable to obtain delimited text file line count")

            start_time = time.perf_counter()
            zip_process = multiprocessing.Process(
                target=split_and_zip_data_files,
                args=(
                    str(download_zip_path),
                    temp_data_file_name,
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
        return CSVDownloadMetadata([destination_path], row_count)


class SparkToCSVStrategy(AbstractToCSVStrategy):
    def __init__(self, logger: logging.Logger, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logger

    def download_to_csv(
        self, source_sql, destination_path, destination_file_name, working_dir_path, download_zip_path, source_df=None
    ):
        # These imports are here for a reason.
        #   some strategies do not require spark
        #   we do not want to force all containers where
        #   other strategies run to have pyspark installed when the strategy
        #   doesn't require it.
        from usaspending_api.common.etl.spark import hadoop_copy_merge, write_csv_file
        from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session

        self.spark = None
        destination_path_dir = str(destination_path).replace(f"/{destination_file_name}", "")
        # The place to write intermediate data files to in s3
        s3_bucket_name = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
        s3_bucket_path = f"s3a://{s3_bucket_name}"
        s3_bucket_sub_path = "temp_download"
        s3_destination_path = f"{s3_bucket_path}/{s3_bucket_sub_path}/{destination_file_name}"
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
            self.spark = get_active_spark_session()
            self.spark_created_by_command = False
            if not self.spark:
                self.spark_created_by_command = True
                self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)
            if source_df is not None:
                df = source_df
            else:
                df = self.spark.sql(source_sql)
            record_count = write_csv_file(
                self.spark,
                df,
                parts_dir=s3_destination_path,
                num_partitions=1,
                max_records_per_file=EXCEL_ROW_LIMIT,
                logger=self._logger,
            )
            column_count = len(df.columns)
            # When combining these later, will prepend the extracted header to each resultant file.
            # The parts therefore must NOT have headers or the headers will show up in the data when combined.
            header = ",".join([_.name for _ in df.schema.fields])
            self._logger.info("Concatenating partitioned output files ...")
            merged_file_paths = hadoop_copy_merge(
                spark=self.spark,
                parts_dir=s3_destination_path,
                header=header,
                logger=self._logger,
                part_merge_group_size=1,
            )
            final_csv_data_file_locations = self._move_data_csv_s3_to_local(
                s3_bucket_name, merged_file_paths, s3_bucket_path, s3_bucket_sub_path, destination_path_dir
            )
        except Exception:
            self._logger.exception("Exception encountered. See logs")
            raise
        finally:
            delete_s3_objects(s3_bucket_name, key_prefix=f"{s3_bucket_sub_path}/{destination_file_name}")
            if self.spark_created_by_command:
                self.spark.stop()
        append_files_to_zip_file(final_csv_data_file_locations, download_zip_path)
        self._logger.info(f"Generated the following data csv files {final_csv_data_file_locations}")
        return CSVDownloadMetadata(final_csv_data_file_locations, record_count, column_count)

    def _move_data_csv_s3_to_local(
        self, bucket_name, s3_file_paths, s3_bucket_path, s3_bucket_sub_path, destination_path_dir
    ) -> List[str]:
        """Moves files from s3 data csv location to a location on the local machine.

        Args:
            bucket_name: The name of the bucket in s3 where file_names and s3_path are
            s3_file_paths: A list of file paths to move from s3, name should
                include s3a:// and bucket name
            s3_bucket_path: The bucket path, e.g. s3a:// + bucket name
            s3_bucket_sub_path: The path to the s3 files in the bucket, exluding s3a:// + bucket name, e.g. temp_directory/files
            destination_path_dir: The location to move those files from s3 to, must not include the
                file name in the path. This path should be a diretory.

        Returns:
            A list of the final location on the local machine that the
            files were moved to from s3.
        """
        start_time = time.time()
        self._logger.info("Moving data files from S3 to local machine...")
        local_csv_file_paths = []
        for file_name in s3_file_paths:
            s3_key = file_name.replace(f"{s3_bucket_path}/", "")
            file_name_only = s3_key.replace(f"{s3_bucket_sub_path}/", "")
            final_path = f"{destination_path_dir}/{file_name_only}"
            download_s3_object(
                bucket_name,
                s3_key,
                final_path,
            )
            local_csv_file_paths.append(final_path)
        self._logger.info(f"Copied data files from S3 to local machine in {(time.time() - start_time):3f}s")
        return local_csv_file_paths
