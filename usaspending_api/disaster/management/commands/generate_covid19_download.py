import json
import logging
import multiprocessing
import re
import time

from datetime import datetime, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.functional import cached_property
from pathlib import Path

from usaspending_api.common.csv_helpers import count_rows_in_delimited_file
from usaspending_api.common.helpers.s3_helpers import upload_download_file_to_s3
from usaspending_api.download.filestreaming.download_generation import (
    split_and_zip_data_files,
    wait_for_process,
    add_data_dictionary_to_zip,
    execute_psql,
    generate_export_query_temp_file,
)
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.download.models import DownloadJob
from usaspending_api.download.lookups import FILE_FORMATS, JOB_STATUS_DICT
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.helpers import get_last_closed_submission_date


logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Assemble raw COVID-19 Disaster Spending data into CSVs and Zip"
    file_format = "csv"
    filepaths_to_delete = []
    total_download_count = 0
    total_download_columns = 0
    total_download_size = 0
    working_dir_path = Path(settings.CSV_LOCAL_PATH)
    readme_path = Path(settings.COVID19_DOWNLOAD_README_FILE_PATH)
    full_timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S%f")

    def add_arguments(self, parser):
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

        for sql_file, final_name in self.download_file_list:
            intermediate_data_file_path = final_name.parent / (final_name.name + "_temp")
            data_file, count = self.download_to_csv(sql_file, final_name, str(intermediate_data_file_path))
            if count <= 0:
                logger.warning(f"Empty data file generated: {final_name}!")

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
        sql_dir = Path("usaspending_api/disaster/management/sql")
        return [
            (
                sql_dir / "disaster_covid19_file_a.sql",
                self.working_dir_path / f"FY2020P07-Present_All_TAS_AccountBalances_{short_timestamp}",
            ),
            (
                sql_dir / "disaster_covid19_file_b.sql",
                self.working_dir_path / f"FY2020P07-Present_All_TAS_AccountBreakdownByPA-OC_{short_timestamp}",
            ),
            (
                sql_dir / "disaster_covid19_file_d1_awards.sql",
                self.working_dir_path / f"Contracts_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                sql_dir / "disaster_covid19_file_d2_awards.sql",
                self.working_dir_path / f"Assistance_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                sql_dir / "disaster_covid19_file_f_contracts.sql",
                self.working_dir_path / f"Contracts_Subawards_{short_timestamp}",
            ),
            (
                sql_dir / "disaster_covid19_file_f_grants.sql",
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

    def download_to_csv(self, sql_filepath, destination_path, intermediate_data_filename):
        start_time = time.perf_counter()
        logger.info(f"Downloading data to {destination_path}")
        options = FILE_FORMATS[self.file_format]["options"]
        export_query = r"\COPY ({}) TO STDOUT {}".format(read_sql_file(sql_filepath), options)
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
            logger.info(f"Counting rows in delimited text file {intermediate_data_filename}")
            try:
                count = count_rows_in_delimited_file(
                    filename=intermediate_data_filename, has_header=True, delimiter=delim
                )
                logger.info(f"{destination_path} contains {count:,} rows of data")
                self.total_download_count += count
            except Exception:
                logger.exception("Unable to obtain delimited text file line count")

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


def read_sql_file(file_path: Path) -> str:
    """Open file and return text with most whitespace removed"""
    p = re.compile(r"\s\s+")
    return p.sub(" ", str(file_path.read_text().replace("\n", "  ")))
