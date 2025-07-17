import logging

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from pathlib import Path
from usaspending_api.common.etl.spark import create_ref_temp_views

from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session
from usaspending_api.common.helpers.download_csv_strategies import (
    SparkToCSVStrategy,
)
from enum import Enum
from usaspending_api.awards.management.sql.spark.unlinked_contracts_file_d1 import file_d1_sql_string
from usaspending_api.awards.management.sql.spark.unlinked_awards_summary_file import summary_file
from usaspending_api.awards.management.sql.spark.unlinked_assistance_file_d2 import file_d2_sql_string
from usaspending_api.awards.management.sql.spark.unlinked_accounts_file_c import file_c_sql_string
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.references.models.toptier_agency import ToptierAgency


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
    _agency_name = None
    _toptier_code = None

    # KEY is the type of compute supported by this command
    # key's VALUE are the strategies required by the compute type
    # These strategies are used to change the behavior of this command
    #   at runtime.
    compute_types = {
        ComputeTypeEnum.SPARK.value: {
            "source_sql_strategy": {
                "summary_file": summary_file,
                "unlinked_contracts_file_d1": file_d1_sql_string,
                "unlinked_assistance_file_d2": file_d2_sql_string,
                "unlinked_accounts_file_c": file_c_sql_string,
            },
            "download_to_csv_strategy": SparkToCSVStrategy(logger=logger),
            "readme_path": Path(settings.UNLINKED_AWARDS_DOWNLOAD_README_FILE_PATH),
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
        self.readme_path = self.compute_types[self.compute_type_arg]["readme_path"]

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
        else:
            raise CommandError(
                f"Right now, only compute type {ComputeTypeEnum.SPARK.value} is supported by this command."
            )

        self.download_csv_strategy = self.compute_types[self.compute_type_arg]["download_to_csv_strategy"]
        self.download_source_sql = self.compute_types[self.compute_type_arg]["source_sql_strategy"]

        # Obtain the agencies to generate files for
        toptier_agencies = ToptierAgency.objects.values("name", "toptier_code").distinct()

        # load_delta_table
        # Save queries as delta tables for efficiency
        for delta_table_name, sql_file, final_name in self.download_file_list:
            df = self.spark.sql(sql_file)
            df.write.format(source="delta").mode(saveMode="overwrite").option("overwriteSchema", "True").saveAsTable(
                name=delta_table_name
            )

        for agency in toptier_agencies:
            agency_name = agency["name"]
            for char in settings.UNLINKED_AWARDS_AGENCY_NAME_CHARS_TO_REPLACE:
                agency_name = agency_name.replace(char, "_")
            self._agency_name = agency_name

            self._toptier_code = agency["toptier_code"]
            zip_file_path = self.working_dir_path / f"{self._agency_name}_UnlinkedAwards_{self.full_timestamp}.zip"

            try:
                self.prep_filesystem(zip_file_path)
                self.process_data_copy_jobs(zip_file_path)
                self.complete_zip_and_upload(zip_file_path)
            except Exception:
                logger.exception("Exception encountered. See logs")
                raise
            finally:
                # "best-effort" attempt to cleanup temp files after a failure. Isn't 100% effective
                self.cleanup()

    def _create_data_csv_dest_path(self, file_name) -> Path:
        return self.working_dir_path / file_name

    def process_data_copy_jobs(self, zip_file_path):
        logger.info(f"Creating new unlinked awards download zip file: {zip_file_path}")
        self.filepaths_to_delete.append(zip_file_path)

        for delta_table_name, sql_file, final_name in self.download_file_list:
            df = self.spark.sql(f"select * from {delta_table_name} where toptier_code = '{self._toptier_code}'")
            sql_file = None
            final_path = self._create_data_csv_dest_path(final_name)
            intermediate_data_file_path = final_path.parent / (final_path.name + "_temp")
            download_metadata = self.download_to_csv(
                sql_file, final_path, final_name, str(intermediate_data_file_path), zip_file_path, df
            )
            if download_metadata.number_of_rows <= 0:
                logger.warning(f"Empty data file generated: {final_path}!")

            self.filepaths_to_delete.extend(self.working_dir_path.glob(f"{final_path.stem}*"))

    def complete_zip_and_upload(self, zip_file_path):
        self.finalize_zip_contents(zip_file_path)
        if self.upload:
            logger.info("Upload final zip file to S3")
            upload_download_file_to_s3(zip_file_path, settings.UNLINKED_AWARDS_DOWNLOAD_REDIRECT_DIR)
            logger.info("Marking zip file for deletion in cleanup")
        else:
            logger.warning("Not uploading zip file to S3. Leaving file locally")
            self.filepaths_to_delete.remove(zip_file_path)

    @property
    def download_file_list(self):
        short_timestamp = self.full_timestamp[:-6]
        return [
            (
                "summary_file",
                f"{self.download_source_sql['summary_file']}",
                f"{self._agency_name}_UnlinkedAwardsSummary_{short_timestamp}",
            ),
            (
                "unlinked_contracts_file_d1",
                f"{self.download_source_sql['unlinked_contracts_file_d1']}",
                f"{self._agency_name}_UnlinkedContracts_{short_timestamp}",
            ),
            (
                "unlinked_assistance_file_d2",
                f"{self.download_source_sql['unlinked_assistance_file_d2']}",
                f"{self._agency_name}_UnlinkedFinancialAssistance_{short_timestamp}",
            ),
            (
                "unlinked_accounts_file_c",
                f"{self.download_source_sql['unlinked_accounts_file_c']}",
                f"{self._agency_name}_UnlinkedAccountsByAward_{short_timestamp}",
            ),
        ]

    def cleanup(self):
        for path in self.filepaths_to_delete:
            self.filepaths_to_delete.remove(path)
            logger.info(f"Removing {path}")
            path.unlink()

    def finalize_zip_contents(self, zip_file_path):
        file_description = build_file_description(str(self.readme_path), dict())
        file_description_path = save_file_description(
            str(zip_file_path.parent), self.readme_path.name, file_description
        )
        append_files_to_zip_file([file_description_path], str(zip_file_path))
        self.total_download_size = zip_file_path.stat().st_size

    def prep_filesystem(self, zip_file_path):
        if zip_file_path.exists():
            # Clean up a zip file that might exist from a prior attempt at this download
            self.filepaths_to_delete.remove(zip_file_path)
            zip_file_path.unlink()

        if not zip_file_path.parent.exists():
            zip_file_path.parent.mkdir()

    def download_to_csv(
        self, source_sql, destination_path, destination_file_name, intermediate_data_filename, zip_file_path, source_df
    ):
        return self.download_csv_strategy.download_to_csv(
            source_sql, destination_path, destination_file_name, self.working_dir_path, zip_file_path, source_df
        )
