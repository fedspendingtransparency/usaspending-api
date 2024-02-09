import logging

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from pathlib import Path
from usaspending_api.common.etl.spark import create_ref_temp_views
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query

from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session
from usaspending_api.disaster.helpers.covid_download_csv_strategies import (
    SparkToCSVStrategy,
)
from enum import Enum
from usaspending_api.awards.management.sql.spark.unlinked_contracts_file_d1 import file_d1_sql_string


class ComputeTypeEnum(Enum):
    SPARK = "spark"


logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Assemble unlinked award by agency data into CSVs and Zip"
    file_format = "csv"
    filepaths_to_delete = []
    total_download_count = 0
    total_download_columns = 0
    total_download_size = 0
    working_dir_path = Path(settings.CSV_LOCAL_PATH)
    full_timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S%f")

    # TODO REMOVE
    # For testing purposes use VA
    agency_id = 37  # toptier id of Department of Veterans Affairs
    agency_name = "Department of Veterans Affairs"

    # KEY is the type of compute supported by this command
    # key's VALUE are the strategies required by the compute type
    # These strategies are used to change the behavior of this command
    #   at runtime.
    compute_types = {
        ComputeTypeEnum.SPARK.value: {
            "source_sql_strategy": {
                "unlinked_contracts_file_d1": file_d1_sql_string.format(agency_name=agency_name),
            },
            "download_to_csv_strategy": SparkToCSVStrategy(logger=logger),
        },
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--compute-type",
            choices=list(self.compute_types.keys()),
            default=ComputeTypeEnum.SPARK.value,
            help="Specify the type of compute to use when executing this command.",
        )
        parser.add_argument(
            "--skip-upload",
            action="store_true",
            help="Don't store the list of IDs for downline ETL. Automatically skipped if --dry-run is provided",
        )

    def handle(self, *args, **options):
        """
        Generates a download data package specific to unlinked awards by agency
        """
        self.upload = not options["skip_upload"]
        self.compute_type_arg = options.get("compute_type")

        if self.compute_type_arg == ComputeTypeEnum.SPARK.value:
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
            if not self.spark:
                self.spark_created_by_command = True
                self.spark = configure_spark_session(**extra_conf, spark_context=self.spark)
            create_ref_temp_views(self.spark)

        self.download_csv_strategy = self.compute_types[self.compute_type_arg]["download_to_csv_strategy"]
        self.download_source_sql = self.compute_types[self.compute_type_arg]["source_sql_strategy"]
        self.zip_file_path = (
            self.working_dir_path / f"{self.agency_name.replace(' ', '_')}_unlinked_awards_{self.full_timestamp}.zip"
        )
        try:
            self.prep_filesystem()
            self.process_data_copy_jobs()
            self.complete_zip_and_upload()
        except Exception:
            logger.exception("Exception encountered. See logs")
            raise
        finally:
            # "best-effort" attempt to cleanup temp files after a failure. Isn't 100% effective
            self.cleanup()

    def _create_data_csv_dest_path(self, file_name) -> Path:
        return self.working_dir_path / file_name

    def _create_sql_file(self, sql_file_name: str, sql: str):
        """Creates a sql file from the specified sql.

        Args:
            sql_file_name: The name of the sql file that'll be generated
            sql: The source sql to write to a file

        Returns:
            str: The path to the generated sql file
        """
        sql_file_path = self.working_dir_path / (sql_file_name + ".sql")
        with open(sql_file_path, "w+") as fh:
            fh.write(sql)
        return sql_file_path

    def process_data_copy_jobs(self):
        logger.info(f"Creating new unliked awards zip file: {self.zip_file_path}")
        self.filepaths_to_delete.append(self.zip_file_path)

        for download_source, final_name in self.download_file_list:
            # TODO: Try to create something that doesn't enforce a DownloadSource for all compute types
            columns = download_source.columns(requested=None)
            source_sql = generate_raw_quoted_query(download_source.row_emitter(columns))
            sql_file_path = self._create_sql_file(f"{final_name}_sql", source_sql)

            final_path = self._create_data_csv_dest_path(final_name)
            intermediate_data_file_path = final_path.parent / (final_path.name + "_temp")
            data_file_names, count = self.download_to_csv(
                sql_file_path, final_path, final_name, str(intermediate_data_file_path)
            )
            self.filepaths_to_delete.append(sql_file_path)
            if count <= 0:
                logger.warning(f"Empty data file generated: {final_path}!")

            self.filepaths_to_delete.extend(self.working_dir_path.glob(f"{final_path.stem}*"))

    def complete_zip_and_upload(self):
        self.finalize_zip_contents()
        if self.upload:
            logger.info("Upload final zip file to S3")
            upload_download_file_to_s3(self.zip_file_path)
            logger.info("Marking zip file for deletion in cleanup")
        else:
            logger.warn("Not uploading zip file to S3. Leaving file locally")
            self.filepaths_to_delete.remove(self.zip_file_path)
            logger.warn("Not creating database record")

    @property
    def download_file_list(self):
        short_timestamp = self.full_timestamp[:-6]
        return [
            (
                # TODO: Try to create something that doesn't enforce a DownloadSource for all compute types
                self.download_source_sql["unlinked_contracts_file_d1"],
                f"{self.agency_name}_UnlinkedContracts_{short_timestamp}",
            )
        ]

    def cleanup(self):
        for path in self.filepaths_to_delete:
            logger.info(f"Removing {path}")
            path.unlink()

    def finalize_zip_contents(self):
        self.total_download_size = self.zip_file_path.stat().st_size

    def prep_filesystem(self):
        if self.zip_file_path.exists():
            # Clean up a zip file that might exist from a prior attempt at this download
            self.zip_file_path.unlink()

        if not self.zip_file_path.parent.exists():
            self.zip_file_path.parent.mkdir()

    def download_to_csv(self, source_sql, destination_path, destination_file_name, intermediate_data_filename):
        return self.download_csv_strategy.download_to_csv(
            source_sql, destination_path, destination_file_name, self.working_dir_path, self.zip_file_path
        )
