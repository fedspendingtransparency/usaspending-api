import json
import logging

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property

from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.disaster.helpers.covid_download_csv_strategies import (
    AuroraToCSVStrategy,
    DatabricksToCSVStrategy,
)
from usaspending_api.disaster.helpers.covid_download_supplemental_files_strategies import (
    AuroraSupplementalFilesStrategy,
    DatabricksSupplementalFilesStrategy,
)
from usaspending_api.disaster.helpers.covid_download_filesystem_strategies import (
    AuroraFileSystemStrategy,
)
from usaspending_api.disaster.management.spark_sql import (
    disaster_covid19_file_d1_awards,
    disaster_covid19_file_d2_awards,
)
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.helpers import get_last_closed_submission_date


logger = logging.getLogger("script")


class Command(BaseCommand):
    """DEV NOTES: It's important that we do not couple this class to a Pathlib subclass like
    Path. Otherwise, it's very difficult to use this command on different types of compute.
    """

    help = "Assemble raw COVID-19 Disaster Spending data into CSVs and Zip"
    filepaths_to_delete = []
    total_download_count = 0
    total_download_columns = 0
    total_download_size = 0
    readme_path = settings.COVID19_DOWNLOAD_README_FILE_PATH
    full_timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S%f")
    covid_profile_download_zip_file_name = f"{settings.COVID19_DOWNLOAD_FILENAME_PREFIX}_{full_timestamp}.zip"
    data_dictionary_name = "Data_Dictionary_Crosswalk.xlsx"

    # KEY is the type of compute supported by this command
    # key's VALUE are the strategies required by the compute type
    # These strategies are used to change the behavior of this command
    #   at runtime.
    compute_flavors = {
        "aurora": {
            "source_sql_strategy": {
                "disaster_covid19_file_a": "usaspending_api/disaster/management/sql/disaster_covid19_file_a.sql",
                "disaster_covid19_file_b": "usaspending_api/disaster/management/sql/disaster_covid19_file_b.sql",
                "disaster_covid19_file_d1_awards": "usaspending_api/disaster/management/sql/disaster_covid19_file_d1_awards.sql",
                "disaster_covid19_file_d2_awards": "usaspending_api/disaster/management/sql/disaster_covid19_file_d2_awards.sql",
                "disaster_covid19_file_f_contracts": "usaspending_api/disaster/management/sql/disaster_covid19_file_f_contracts.sql",
                "disaster_covid19_file_f_grants": "usaspending_api/disaster/management/sql/disaster_covid19_file_f_grants.sql",
            },
            "working_dir_path": settings.CSV_LOCAL_PATH,
            "covid_profile_download_output_dir_path": settings.CSV_LOCAL_PATH,
            "download_to_csv_strategy": AuroraToCSVStrategy(logger=logger),
            "supplemental_files_strategy": AuroraSupplementalFilesStrategy(
                data_dictionary_name=data_dictionary_name, output_dir_path=settings.CSV_LOCAL_PATH
            ),
            "final_upload_to_s3_strategy": None,
            "filesystem_strategy": AuroraFileSystemStrategy(),
        },
        "databricks": {
            "source_sql_strategy": {
                "disaster_covid19_file_a": "select 1 as test;",
                "disaster_covid19_file_b": "select 2 as test;",
                "disaster_covid19_file_d1_awards": disaster_covid19_file_d1_awards,
                "disaster_covid19_file_d2_awards": disaster_covid19_file_d2_awards,
                "disaster_covid19_file_f_contracts": "select 5 as test;",
                "disaster_covid19_file_f_grants": "select 6 as test;",
            },
            "working_dir_path": "s3a://dti-usaspending-bulk-download-qat/csv_downloads",
            "covid_profile_download_output_dir_path": "s3a://dti-usaspending-bulk-download-qat",
            "download_to_csv_strategy": DatabricksToCSVStrategy(logger=logger),
            "supplemental_files_strategy": DatabricksSupplementalFilesStrategy(
                data_dictionary_name=data_dictionary_name,
                output_dir_path=settings.CSV_LOCAL_PATH,
                covid_profile_download_file_name=covid_profile_download_zip_file_name,
                bucket_name="dti-usaspending-bulk-download-qat",
            ),
            "final_upload_to_s3_strategy": None,
            "filesystem_strategy": None,
        },
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--compute-flavor",
            choices=list(self.compute_flavors.keys()),
            default="aurora",
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
        # Setting all the strategies based on command arguments
        self.compute_flavor_arg = options.get("compute_flavor")
        self.download_csv_strategy = self.compute_flavors[self.compute_flavor_arg]["download_to_csv_strategy"]
        self.supplemental_files_strategy = self.compute_flavors[self.compute_flavor_arg]["supplemental_files_strategy"]
        self.download_source_sql = self.compute_flavors[self.compute_flavor_arg]["source_sql_strategy"]
        self.working_dir_path: str = self.compute_flavors[self.compute_flavor_arg]["working_dir_path"]
        covid_profile_download_output_dir_path: str = self.compute_flavors[self.compute_flavor_arg][
            "covid_profile_download_output_dir_path"
        ]
        self.covid_profile_download_zip_path: str = (
            f"{covid_profile_download_output_dir_path}/{self.covid_profile_download_zip_file_name}"
        )
        self._filesystem_strategy = self.compute_flavors[self.compute_flavor_arg]["filesystem_strategy"]

        self.upload = not options["skip_upload"]

        try:
            self.prep_filesystem()
            self.process_data_copy_jobs()
            self.finalize_zip_contents()
            if self.compute_flavor_arg == "aurara":
                self.upload_local_zip()
        except Exception:
            logger.exception("Exception encountered. See logs")
            raise
        finally:
            # "best-effort" attempt to cleanup temp files after a failure. Isn't 100% effective
            self.cleanup()

    def process_data_copy_jobs(self):
        logger.info(f"Creating new COVID-19 download zip file: {self.covid_profile_download_zip_path}")
        self.filepaths_to_delete.append(self.covid_profile_download_zip_path)

        for sql_file_path, destination_path in self.download_file_list:
            data_file, count = self.download_to_csv(sql_file_path, destination_path)
            if count <= 0:
                logger.warning(f"Empty data file generated: {destination_path}!")
            self.total_download_count += count
            self.filepaths_to_delete.extend(destination_path)

    def upload_local_zip(self):
        if self.upload:
            logger.info("Upload final zip file to S3")
            upload_download_file_to_s3(self.covid_profile_download_zip_path, self.covid_profile_download_zip_file_name)
            db_id = self.store_record_in_database()
            logger.info(f"Created database record {db_id} for future retrieval")
            logger.info("Marking zip file for deletion in cleanup")
        else:
            logger.warn("Not uploading zip file to S3. Leaving file locally")
            self.filepaths_to_delete.remove(self.covid_profile_download_zip_path)
            logger.warn("Not creating database record")

    @property
    def download_file_list(self):
        short_timestamp = self.full_timestamp[:-6]
        return [
            (
                f'{self.download_source_sql["disaster_covid19_file_a"]}',
                f"{self.working_dir_path}/{self.get_current_fy_and_period}-Present_All_TAS_AccountBalances_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_b"]}',
                f"{self.working_dir_path}/{self.get_current_fy_and_period}-Present_All_TAS_AccountBreakdownByPA-OC_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_d1_awards"]}',
                f"{self.working_dir_path}/Contracts_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_d2_awards"]}',
                f"{self.working_dir_path}/Assistance_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_f_contracts"]}',
                f"{self.working_dir_path}/Contracts_Subawards_{short_timestamp}",
            ),
            (
                f'{self.download_source_sql["disaster_covid19_file_f_grants"]}',
                f"{self.working_dir_path}/Assistance_Subawards_{short_timestamp}",
            ),
        ]

    @cached_property
    def get_current_fy_and_period(self):
        latest = get_last_closed_submission_date(is_quarter=False)
        return f"FY{latest['submission_fiscal_year']}P{str(latest['submission_fiscal_month']).zfill(2)}"

    def cleanup(self):
        # TODO
        return
        # for path in self.filepaths_to_delete:
        #     logger.info(f"Removing {path}")
        #     path.unlink()

    def finalize_zip_contents(self):
        if self.supplemental_files_strategy is None:
            return
        logger.info("Adding data dictionary to zip file")
        data_dictionary_path = self.supplemental_files_strategy.append_data_dictionary(
            self.covid_profile_download_zip_path
        )
        self.filepaths_to_delete.append(data_dictionary_path)
        # description_file_path = self.supplemental_files_strategy.append_description_file(
        #     self.readme_path, self.covid_profile_download_zip_path
        # )
        # self.filepaths_to_delete.append(description_file_path)
        # TODO
        self.total_download_size = 0  # self.covid_profile_download_zip_path.stat().st_size

    def prep_filesystem(self):
        if self._filesystem_strategy is None:
            return
        self._filesystem_strategy.prep_filesystem(self.covid_profile_download_zip_path, self.working_dir_path)

    def download_to_csv(self, sql_filepath, destination_path: str):
        return self.download_csv_strategy.download_to_csv(
            sql_filepath,
            destination_path,
            self.working_dir_path,
            self.covid_profile_download_zip_path,
        )

    def store_record_in_database(self):
        download_record = DownloadJob.objects.create(
            file_name=self.covid_profile_download_zip_file_name,
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
