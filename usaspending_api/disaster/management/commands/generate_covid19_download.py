import logging
import multiprocessing
import time
import os

from django.core.management.base import BaseCommand
from datetime import datetime, timezone
from django.conf import settings
from pathlib import Path

from usaspending_api.download.filestreaming.download_generation import (
    split_and_zip_data_files,
    wait_for_process,
    add_data_dictionary_to_zip,
    execute_psql,
    generate_export_query_temp_file,
)
from usaspending_api.download.filestreaming.file_description import build_file_description, save_file_description
from usaspending_api.download.lookups import FILE_FORMATS
from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file
from usaspending_api.common.csv_helpers import count_rows_in_delimited_file
from usaspending_api.download.helpers import multipart_upload

logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Assemble raw COVID-19 Disaster Spending data into CSVs and Zip"
    file_format = "csv"
    filepaths_to_delete = []

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-upload",
            action="store_true",
            help="Don't store the list of IDs for downline ETL. Automatically skipped if --dry-run is provided",
        )

    def handle(self, *args, **options):
        """
            Generate File A / GTAS
                Add to Zip File (split as necessary)
            Generate File B
                Add to Zip File (split as necessary)
            Generate File D1 Awards
                Add to Zip File (split as necessary)
            Generate File D2 Awards
                Add to Zip File (split as necessary)
            Generate File F Contracts
                Add to Zip File (split as necessary)
            Generate File F Grants
                Add to Zip File (split as necessary)
            Add Data Dictionary
            Add readme text file

            Upload

        from usaspending_api.download.filestreaming.zip_file import append_files_to_zip_file

        """
        full_timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S%f")
        short_timestamp = full_timestamp[:-6]
        self.zip_file_path = (
            Path(settings.CSV_LOCAL_PATH) / f"{settings.COVID19_DOWNLOAD_FILENAME_PREFIX}_{full_timestamp}.zip"
        )
        self.prep_filesystem()

        sql_dir = Path("usaspending_api/disaster/management/sql")
        working_dir = Path(settings.CSV_LOCAL_PATH)
        download_file_list = [
            (
                sql_dir / "disaster_covid19_file_b.sql",
                working_dir / f"FY20P07-Present_All_TAS_AccountBreakdownByPA-OC_{short_timestamp}",
            ),
            (
                sql_dir / "disaster_covid19_file_d1_awards.sql",
                working_dir / f"Contracts_PrimeAwardSummaries_{short_timestamp}",
            ),
            (
                sql_dir / "disaster_covid19_file_d2_awards.sql",
                working_dir / f"Assistance_PrimeAwardSummaries_{short_timestamp}",
            ),
            (sql_dir / "disaster_covid19_file_f_contracts.sql", working_dir / f"Contracts_Subawards_{short_timestamp}",),
            (sql_dir / "disaster_covid19_file_f_grants.sql", working_dir / f"Assistance_Subawards_{short_timestamp}",),
        ]

        for sql_file, final_name in download_file_list:
            intermediate_data_file_path = final_name.parent / (final_name.name + "_temp")
            data_file, count = self.download_to_csv(sql_file, final_name, str(intermediate_data_file_path))
            if count <= 0:
                raise Exception(f"Missing Data for {final_name}!!!!")

            self.filepaths_to_delete.extend(working_dir.glob(f"{final_name.stem}*"))

        self.finalize_zip_contents()
        if options["skip_upload"]:
            logger.warn("Not uploading Zip package to S3. Leaving file locally")
        else:
            logger.info("Upload final Zip package to S3")
            self.upload_to_s3(self.zip_file_path)
            logger.info("Marking zip file for deletion in cleanup")
            self.filepaths_to_delete.add(self.zip_file_path)

        self.cleanup()

    def cleanup(self):
        for path in self.filepaths_to_delete:
            logger.info(f"Removing {path}")
            path.unlink()

    def finalize_zip_contents(self):
        self.filepaths_to_delete.append(Path(settings.CSV_LOCAL_PATH) / "Data_Dictionary_Crosswalk.xlsx")

        add_data_dictionary_to_zip(str(self.zip_file_path.parent), str(self.zip_file_path))

        file_description = build_file_description(settings.COVID19_DOWNLOAD_README_FILE_PATH, dict())
        file_description_path = save_file_description(
            str(self.zip_file_path.parent), settings.COVID19_DOWNLOAD_README_FILE_PATH.split("/")[-1], file_description
        )
        self.filepaths_to_delete.append(Path(file_description_path))
        append_files_to_zip_file([file_description_path], str(self.zip_file_path))

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
        export_query = r"\COPY ({}) TO STDOUT {}".format(str(sql_filepath.read_text()).replace("\n", "  "), options)
        try:
            temp_file, temp_file_path = generate_export_query_temp_file(export_query, None)
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
            os.close(temp_file)
            os.remove(temp_file_path)
        return destination_path, count

    # def roll_data_file_into_zip(self, data_file, data_file_name):
    #     start_time = time.perf_counter()
    #     zip_process = multiprocessing.Process(
    #         target=split_and_zip_data_files,
    #         args=(str(self.zip_file_path), str(data_file), str(data_file_name), self.file_format, None),
    #     )
    #     zip_process.start()
    #     wait_for_process(zip_process, start_time, None)

    def upload_to_s3(self, zip_file_path):
        try:
            # push file to S3 bucket, if not local
            bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
            region = settings.USASPENDING_AWS_REGION
            start_uploading = time.perf_counter()
            multipart_upload(bucket, region, str(zip_file_path), zip_file_path.name)
            logger.info(f"Uploading took {time.perf_counter() - start_uploading:.2f}s")
        except Exception as e:
            raise Exception(e) from e
