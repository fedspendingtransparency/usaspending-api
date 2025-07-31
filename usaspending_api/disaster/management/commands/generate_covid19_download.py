import json
import logging

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property
from pathlib import Path
from usaspending_api.common.etl.spark import create_ref_temp_views

from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.common.helpers.spark_helpers import configure_spark_session, get_active_spark_session
from usaspending_api.common.helpers.sql_helpers import read_sql_file_to_text
from usaspending_api.common.helpers.download_csv_strategies import (
    PostgresToCSVStrategy,
    SparkToCSVStrategy,
)
from usaspending_api.download.filestreaming.download_generation import (
    add_data_dictionary_to_zip,
)
from usaspending_api.disaster.management.sql.spark.disaster_covid19_file_a import file_a_sql_string
from usaspending_api.disaster.management.sql.spark.disaster_covid19_file_b import file_b_sql_string
from usaspending_api.disaster.management.sql.spark.disaster_covid19_file_d1_awards import d1_awards_sql_string
from usaspending_api.disaster.management.sql.spark.disaster_covid19_file_d2_awards import d2_awards_sql_string
from usaspending_api.disaster.management.sql.spark.disaster_covid19_file_f_contracts import f_contracts_sql_string
from usaspending_api.disaster.management.sql.spark.disaster_covid19_file_f_grants import f_grants_sql_string
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.helpers import get_last_closed_submission_date
from enum import Enum
from usaspending_api.config import CONFIG


class ComputeTypeEnum(Enum):
    SPARK = "spark"
    POSTGRES = "postgres"


logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Assemble raw COVID-19 Disaster Spending data into CSVs and Zip"
    file_format = "csv"
    filepaths_to_delete = []
    total_download_count = 0
    total_download_columns = 0
    total_download_size = 0
    working_dir_path = Path(settings.CSV_LOCAL_PATH)
    full_timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S%f")

    # KEY is the type of compute supported by this command
    # key's VALUE are the strategies required by the compute type
    # These strategies are used to change the behavior of this command
    #   at runtime.
    compute_types = {
        ComputeTypeEnum.POSTGRES.value: {
            "source_sql_strategy": {
                "disaster_covid19_file_a": "usaspending_api/disaster/management/sql/disaster_covid19_file_a.sql",
                "disaster_covid19_file_b": "usaspending_api/disaster/management/sql/disaster_covid19_file_b.sql",
                "disaster_covid19_file_d1_awards": "usaspending_api/disaster/management/sql/disaster_covid19_file_d1_awards.sql",
                "disaster_covid19_file_d2_awards": "usaspending_api/disaster/management/sql/disaster_covid19_file_d2_awards.sql",
                "disaster_covid19_file_f_contracts": "usaspending_api/disaster/management/sql/disaster_covid19_file_f_contracts.sql",
                "disaster_covid19_file_f_grants": "usaspending_api/disaster/management/sql/disaster_covid19_file_f_grants.sql",
            },
            "download_to_csv_strategy": PostgresToCSVStrategy(logger=logger),
            "readme_path": Path(settings.COVID19_DOWNLOAD_README_FILE_PATH),
        },
        ComputeTypeEnum.SPARK.value: {
            "source_sql_strategy": {
                "disaster_covid19_file_a": file_a_sql_string,
                "disaster_covid19_file_b": file_b_sql_string,
                "disaster_covid19_file_d1_awards": d1_awards_sql_string,
                "disaster_covid19_file_d2_awards": d2_awards_sql_string,
                "disaster_covid19_file_f_contracts": f_contracts_sql_string,
                "disaster_covid19_file_f_grants": f_grants_sql_string,
            },
            "download_to_csv_strategy": SparkToCSVStrategy(logger=logger),
            "readme_path": Path(CONFIG.SPARK_COVID19_DOWNLOAD_README_FILE_PATH),
        },
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--compute-type",
            choices=list(self.compute_types.keys()),
            default=ComputeTypeEnum.POSTGRES.value,
            help="Specify the type of compute to use when executing this command.",
        )
        parser.add_argument(
            "--skip-upload",
            action="store_true",
            help="Don't store the list of IDs for downline ETL. Automatically skipped if --dry-run is provided",
        )

    def handle(self, *args, **options):
        """
        Generates a download data package specific to COVID-19 spending
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

        self.readme_path = self.compute_types[self.compute_type_arg]["readme_path"]
        self.download_csv_strategy = self.compute_types[self.compute_type_arg]["download_to_csv_strategy"]
        self.download_source_sql = self.compute_types[self.compute_type_arg]["source_sql_strategy"]
        self.zip_file_path = (
            self.working_dir_path / f"{settings.COVID19_DOWNLOAD_FILENAME_PREFIX}_{self.full_timestamp}.zip"
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

    def _create_data_csv_dest_path(self, file_name):
        return self.working_dir_path / file_name

    def process_data_copy_jobs(self):
        logger.info(f"Creating new COVID-19 download zip file: {self.zip_file_path}")
        self.filepaths_to_delete.append(self.zip_file_path)

        for source_sql, final_name in self.download_file_list:
            final_path = self._create_data_csv_dest_path(final_name)
            intermediate_data_file_path = final_path.parent / (final_path.name + "_temp")
            if self.compute_type_arg == ComputeTypeEnum.POSTGRES.value:
                source_sql = read_sql_file_to_text(Path(source_sql))
            download_metadata = self.download_to_csv(
                source_sql, final_path, final_name, str(intermediate_data_file_path)
            )
            if download_metadata.number_of_rows <= 0:
                logger.warning(f"Empty data file generated: {final_path}!")

            self.filepaths_to_delete.extend(self.working_dir_path.glob(f"{final_path.stem}*"))

    def complete_zip_and_upload(self):
        self.finalize_zip_contents()
        if self.upload:
            logger.info("Upload final zip file to S3")
            upload_download_file_to_s3(self.zip_file_path)
            db_id = self.store_record_in_database()
            logger.info(f"Created database record {db_id} for future retrieval")
            logger.info("Marking zip file for deletion in cleanup")
        else:
            logger.warning("Not uploading zip file to S3. Leaving file locally")
            self.filepaths_to_delete.remove(self.zip_file_path)
            logger.warning("Not creating database record")

    @property
    def download_file_list(self):
        short_timestamp = self.full_timestamp[:-6]
        return [
            (
                f'{self.download_source_sql["disaster_covid19_file_a"]}',
                f"{self.get_current_fy_and_period}-Present_All_TAS_AccountBalances_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_b"]}',
                f"{self.get_current_fy_and_period}-Present_All_TAS_AccountBreakdownByPA-OC_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_d1_awards"]}',
                f"Contracts_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_d2_awards"]}',
                f"Assistance_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_f_contracts"]}',
                f"Contracts_Subawards_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_f_grants"]}',
                f"Assistance_Subawards_{short_timestamp}",
            ),
        ]

    @cached_property
    def get_current_fy_and_period(self):
        latest = get_last_closed_submission_date(is_quarter=False)
        return f"FY{latest['submission_fiscal_year']}P{str(latest['submission_fiscal_month']).zfill(2)}"

    def cleanup(self):
        for path in self.filepaths_to_delete:
            logger.info(f"Removing {path}")
            path.unlink()

    def finalize_zip_contents(self):
        self.filepaths_to_delete.append(self.working_dir_path / "Data_Dictionary_Crosswalk.xlsx")

        add_data_dictionary_to_zip(str(self.zip_file_path.parent), str(self.zip_file_path))

        file_description = build_file_description(str(self.readme_path), dict())
        file_description_path = save_file_description(
            str(self.zip_file_path.parent), self.readme_path.name, file_description
        )
        self.filepaths_to_delete.append(Path(file_description_path))
        append_files_to_zip_file([file_description_path], str(self.zip_file_path))
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

    def store_record_in_database(self):
        download_record = DownloadJob.objects.create(
            file_name=self.zip_file_path.name,
            file_size=self.total_download_size,
            job_status_id=JOB_STATUS_DICT["finished"],
            json_request=json.dumps(
                {
                    "filters": {
                        "def_codes": list(
                            DisasterEmergencyFundCode.objects.filter(group_name="covid_19")
                            .order_by("code")
                            .values_list("code", flat=True)
                        ),
                        "latest_fiscal_period": self.get_current_fy_and_period[7:],
                        "latest_fiscal_year": self.get_current_fy_and_period[2:6],
                        "start_date": "2020-04-01",
                    }
                }
            ),
            monthly_download=False,
            number_of_columns=self.total_download_columns,
            number_of_rows=self.total_download_count,
        )

        return download_record.download_job_id
