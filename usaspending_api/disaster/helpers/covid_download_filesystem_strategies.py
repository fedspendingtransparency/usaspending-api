from abc import ABC, abstractmethod
import multiprocessing
import time
import logging
from pathlib import Path
from typing import Tuple

from usaspending_api.common.csv_helpers import count_rows_in_delimited_file
from usaspending_api.common.helpers.sql_helpers import read_sql_file_to_text
from usaspending_api.download.filestreaming.download_generation import (
    EXCEL_ROW_LIMIT,
    split_and_zip_data_files,
    wait_for_process,
    execute_psql,
    generate_export_query_temp_file,
)
from usaspending_api.download.lookups import FILE_FORMATS
from usaspending_api.download.filestreaming.download_generation import generate_export_query_temp_file
from pyspark.sql import SparkSession
from usaspending_api.common.etl.spark import hadoop_copy_merge, load_csv_file
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session
from usaspending_api.config import CONFIG


class AbstractCovidFileSystemStrategy(ABC):
    """A composable class that can be used according to the Strategy software design pattern.
    The Covid-19 "file system" strategy establishes the interface for a suite of file system
    methods. These methods are specific to the platform of the file system. Some file systems
    require specific representations of information in order for the file system to be worked with.
    For example, some file systems are in S3 while others are local.
    """

    @abstractmethod
    def prep_filesystem(self, covid_profile_zip_file_path: Path, working_dir_path: Path) -> None:
        """
        Args:
            covid_profile_zip_file_path: The path to the covid profile zip location
            local_working_dir: The path to work with files on the local machine of the running application
        """
        pass


class AuroraCovidFileSystemStrategy(AbstractCovidFileSystemStrategy):
    def prep_filesystem(self, covid_profile_zip_file_path, working_dir_path):
        if self.covid_profile_zip_file_path.exists():
            # Clean up a zip file that might exist from a prior attempt at this download
            self.covid_profile_zip_file_path.unlink()

        if not self.covid_profile_zip_file_path.parent.exists():
            self.covid_profile_zip_file_path.parent.mkdir()


class DatabricksCovidFileSystemStrategy(AbstractCovidFileSystemStrategy):
    def prep_filesystem(self, covid_profile_zip_file_path, working_dir_path):
        if self.local_working_dir.exists():
            # Clean up a zip file that might exist from a prior attempt at this download
            self.local_working_dir.unlink()

        if not self.local_working_dir.parent.exists():
            self.local_working_dir.parent.mkdir()