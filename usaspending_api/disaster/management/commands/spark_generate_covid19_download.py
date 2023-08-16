from django.core.management.base import BaseCommand
import logging
from django.conf import settings
from pathlib import Path
from datetime import datetime, timezone

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

    @property
    def download_file_list(self):
        short_timestamp = self.full_timestamp[:-6]
        # TODO: Change when sql scripts are implemented
        sql_dir = Path("usaspending_api/disaster/management/sql/test_spark")
        return [
            (
                sql_dir / "test_sql.sql",
                self.working_dir_path / f"test_spark_download_{short_timestamp}",
            ),
        ]

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
            self.cleanup()

    def cleanup(self):
        # "best-effort" attempt to cleanup temp files. Isn't 100% effective
        for path in self.filepaths_to_delete:
            logger.info(f"Removing {path}")
            path.unlink()

    def prep_filesystem(self):
        if self.zip_file_path.exists():
            # Clean up a zip file that might exist from a prior attempt at this download
            self.zip_file_path.unlink()

        if not self.zip_file_path.parent.exists():
            self.zip_file_path.parent.mkdir()

    def process_data_copy_jobs(self):
        logger.info(f"Creating new COVID-19 download zip file: {self.zip_file_path}")
        self.filepaths_to_delete.append(self.zip_file_path)

        for sql_file, final_name in self.download_file_list:
            intermediate_data_file_path = final_name.parent / (final_name.name + "_temp")
            data_file, count = self.download_to_csv(sql_file, final_name, str(intermediate_data_file_path))
            if count <= 0:
                logger.warning(f"Empty data file generated: {final_name}!")

            # TODO: is this line neccessary?
            self.filepaths_to_delete.extend(self.working_dir_path.glob(f"{final_name.stem}*"))