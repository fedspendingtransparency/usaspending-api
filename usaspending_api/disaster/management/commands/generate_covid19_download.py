import json
import logging

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property
from pathlib import Path

from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.disaster.helpers.covid_download_csv_strategies import (
    PostgresCovidToCSVStrategy,
    SparkCovidToCSVStrategy,
)
from usaspending_api.download.filestreaming.download_generation import (
    add_data_dictionary_to_zip,
)
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.helpers import get_last_closed_submission_date


logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Assemble raw COVID-19 Disaster Spending data into CSVs and Zip"
    filepaths_to_delete = []
    total_download_count = 0
    total_download_columns = 0
    total_download_size = 0
    working_dir_path = Path(settings.CSV_LOCAL_PATH)
    readme_path = Path(settings.COVID19_DOWNLOAD_README_FILE_PATH)
    full_timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S%f")

    # KEY is the type of compute supported by this command
    # the key's VALUE is the strategies required by the compute type
    compute_flavors = {
        "aurora": {
            "sql_file_strategy": {
                "disaster_covid19_file_a": Path("usaspending_api/disaster/management/sql/disaster_covid19_file_a.sql"),
                "disaster_covid19_file_b": Path("usaspending_api/disaster/management/sql/disaster_covid19_file_b.sql"),
                "disaster_covid19_file_d1_awards": Path(
                    "usaspending_api/disaster/management/sql/disaster_covid19_file_d1_awards.sql"
                ),
                "disaster_covid19_file_d2_awards": Path(
                    "usaspending_api/disaster/management/sql/disaster_covid19_file_d2_awards.sql"
                ),
                "disaster_covid19_file_f_contracts": Path(
                    "usaspending_api/disaster/management/sql/disaster_covid19_file_f_contracts.sql"
                ),
                "disaster_covid19_file_f_grants": Path(
                    "usaspending_api/disaster/management/sql/disaster_covid19_file_f_grants.sql"
                ),
            },
            "download_to_csv_strategy": PostgresCovidToCSVStrategy(logger=logger),
        },
        "spark": {
            "sql_file_strategy": {
                "disaster_covid19_file_a": "select 1 as test;"
                # "disaster_covid19_file_b": "select 2 as test;",
                # "disaster_covid19_file_d1_awards": "select 3 as test;",
                # "disaster_covid19_file_d2_awards": "select 4 as test;",
                # "disaster_covid19_file_f_contracts": "select 5 as test;",
                # "disaster_covid19_file_f_grants": "select 6 as test;",
            },
            "download_to_csv_strategy": SparkCovidToCSVStrategy(logger=logger),
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
        compute_flavor = options.get("compute_flavor")
        self._download_csv_strategy = self.compute_flavors[compute_flavor]["download_to_csv_strategy"]
        self._download_file_source_sql = self.compute_flavors[compute_flavor]["sql_file_strategy"]
        self.upload = not options["skip_upload"]
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

    def process_data_copy_jobs(self):
        logger.info(f"Creating new COVID-19 download zip file: {self.zip_file_path}")
        self.filepaths_to_delete.append(self.zip_file_path)

        for sql_file_path, final_name in self.download_file_list:
            intermediate_data_file_path = final_name.parent / (final_name.name + "_temp")
            data_file, count = self.download_to_csv(sql_file_path, final_name, str(intermediate_data_file_path))
            if count <= 0:
                logger.warning(f"Empty data file generated: {final_name}!")
            self.total_download_count += count

            self.filepaths_to_delete.extend(self.working_dir_path.glob(f"{final_name.stem}*"))

    def complete_zip_and_upload(self):
        self.finalize_zip_contents()
        if self.upload:
            logger.info("Upload final zip file to S3")
            upload_download_file_to_s3(self.zip_file_path)
            db_id = self.store_record_in_database()
            logger.info(f"Created database record {db_id} for future retrieval")
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
                f'{self._download_file_source_sql["disaster_covid19_file_a"]}',
                self.working_dir_path
                / f"{self.get_current_fy_and_period}-Present_All_TAS_AccountBalances_{short_timestamp}",
            ),
            (
                f'{self._download_file_source_sql["disaster_covid19_file_b"]}',
                self.working_dir_path
                / f"{self.get_current_fy_and_period}-Present_All_TAS_AccountBreakdownByPA-OC_{short_timestamp}",
            ),
            (
                f'{self._download_file_source_sql["disaster_covid19_file_d1_awards"]}',
                self.working_dir_path / f"Contracts_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                f'{self._download_file_source_sql["disaster_covid19_file_d2_awards"]}',
                self.working_dir_path / f"Assistance_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                f'{self._download_file_source_sql["disaster_covid19_file_f_contracts"]}',
                self.working_dir_path / f"Contracts_Subawards_{short_timestamp}",
            ),
            (
                f'{self._download_file_source_sql["disaster_covid19_file_f_grants"]}',
                self.working_dir_path / f"Assistance_Subawards_{short_timestamp}",
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
        # self.filepaths_to_delete.append(self.working_dir_path / "Data_Dictionary_Crosswalk.xlsx")

        # add_data_dictionary_to_zip(str(self.zip_file_path.parent), str(self.zip_file_path))

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

    def download_to_csv(self, sql_filepath, destination_path, intermediate_data_filename):
        return self._download_csv_strategy.download_to_csv(
            sql_filepath, destination_path, intermediate_data_filename, self.working_dir_path, self.zip_file_path
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
