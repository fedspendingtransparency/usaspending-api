import logging
import zipfile

from contextlib import contextmanager
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from pathlib import Path
from tempfile import TemporaryDirectory
from usaspending_api.common.etl.postgres import ETLTable, ETLTemporaryTable
from usaspending_api.common.etl.postgres import operations
from usaspending_api.common.helpers.sql_helpers import execute_dml_sql
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.common.zip import extract_single_file_zip
from usaspending_api.etl.operations.subaward.update_city_county import update_subaward_city_county
from usaspending_api.references.models import CityCountyStateCode


logger = logging.getLogger("script")


MAX_CHANGE_PERCENT = 20


class Command(BaseCommand):

    destination_table_name = CityCountyStateCode._meta.db_table
    staging_table_name = "temp_load_city_county_state_code_source"

    help = (
        f"Loads city, county, and state data from USGS/GNIS NationalFedCode pipe delimited text file "
        f"into the USAspending {destination_table_name} table."
    )

    file = None
    force = False
    working_file = None

    def add_arguments(self, parser):

        parser.add_argument(
            "file",
            metavar="FILE",
            help=(
                "Path or URI of NationalFedCode pipe delimited text file.  Can also be a ZIP file containing only "
                "one file that is the NationalFedCode pipe delimited text file."
            ),
        )

        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                f"Reloads FILE even if the max change threshold of {MAX_CHANGE_PERCENT}% is exceeded.  This "
                f"is a safety precaution to prevent accidentally updating every subaward in the system as part "
                f"of the nightly pipeline.  Will also force foreign key table links to be examined even if it "
                f"appears there were no changes."
            ),
        )

    def handle(self, *args, **options):

        self.file = options["file"]
        self.force = options["force"]

        logger.info(f"FILE: {self.file}")
        logger.info(f"FORCE SWITCH: {self.force}")
        logger.info(f"MAX CHANGE LIMIT: {'unlimited' if self.force else f'{MAX_CHANGE_PERCENT:}%'}")

        with Timer("Overall run"):
            # Ensure we have a local, unzipped copy of the file with which to work.
            with self._retrieve_file(self.file) as local_file_path:
                with self._unzip_file(local_file_path) as unzipped_file_path:
                    self.working_file = unzipped_file_path
                    logger.info(f"WORKING FILE: {self.working_file}")
                    self._validate_input_file_header()
                    self._process_file()

        # Unzipped file should be gone at this point.
        self.working_file = None

    def _process_file(self):
        try:
            with transaction.atomic():
                with Timer("Load file"):
                    change_count = self._perform_load()
                    logger.info(f"{change_count:,} rows affected overall")
                if change_count > 0 or self.force:
                    with Timer("Update subawards"):
                        subaward_count = update_subaward_city_county()
                        logger.info(f"{subaward_count:,} Subawards updated")
                t = Timer("Commit transaction")
                t.log_starting_message()
            t.log_success_message()
        except Exception:
            logger.error("ALL CHANGES ROLLED BACK DUE TO EXCEPTION")
            raise

        if change_count > 0 or self.force:
            try:
                self._vacuum_tables()
            except Exception:
                logger.error("CHANGES WERE SUCCESSFULLY COMMITTED EVEN THOUGH VACUUMS FAILED")
                raise

    @staticmethod
    @contextmanager
    def _unzip_file(file_path):
        """
        ZIP file context manager.  If the file pointed to by file_path is a ZIP file, extracts file to a
        temporary location, yields, and cleans up afterwards.  Otherwise, effectively does nothing.
        """
        if zipfile.is_zipfile(file_path):
            with TemporaryDirectory() as temp_dir:
                with Timer("Unzip file"):
                    unzipped_file_path = extract_single_file_zip(file_path, temp_dir)
                yield unzipped_file_path
        else:
            yield file_path

    @staticmethod
    @contextmanager
    def _retrieve_file(file_path):
        """
        Remote file context manager.  If file is not local, copies it to a local temporary location, yields, and
        cleans up afterwards.  Otherwise, effectively does nothing.
        """
        file = RetrieveFileFromUri(file_path)
        is_local = file.parsed_url_obj.scheme in ("file", "")  # Our best guess.

        if not is_local:
            with TemporaryDirectory() as temp_dir:
                temp_file_path = str(Path(temp_dir) / "local_file_copy")
                with Timer("Download file"):
                    file.copy(temp_file_path)
                yield temp_file_path
        else:
            yield file_path

    def _perform_load(self):
        self._create_staging_table()
        self._import_input_file()

        natural_key_columns = ["feature_id", "state_alpha", "county_sequence", "county_numeric"]
        destination_table = ETLTable(self.destination_table_name, key_overrides=natural_key_columns)
        source_table = ETLTemporaryTable(self.staging_table_name)

        with Timer("Delete obsolete rows"):
            deleted_count = operations.delete_obsolete_rows(source_table, destination_table)
            logger.info(f"{deleted_count:,} rows deleted")

        with Timer("Update changed rows"):
            updated_count = operations.update_changed_rows(source_table, destination_table)
            logger.info(f"{updated_count:,} rows updated")

        with Timer("Insert missing rows"):
            inserted_count = operations.insert_missing_rows(source_table, destination_table)
            logger.info(f"{inserted_count:,} rows inserted")

        with Timer("Remove staging table"):
            execute_dml_sql(f'drop table if exists "{self.staging_table_name}"')

        change_count = deleted_count + updated_count + inserted_count
        current_record_count = CityCountyStateCode.objects.count()
        max_change_threshold = int(current_record_count * MAX_CHANGE_PERCENT / 100.0)
        if current_record_count > 0 and change_count > max_change_threshold and not self.force:
            raise RuntimeError(
                f"Exceeded maximum number of allowed changes {max_change_threshold:,} ({MAX_CHANGE_PERCENT}%).  "
                f"Use --force switch if this was intentional."
            )

        return change_count

    def _validate_input_file_header(self):
        expected_header = (
            "FEATURE_ID|FEATURE_NAME|FEATURE_CLASS|CENSUS_CODE|CENSUS_CLASS_CODE|GSA_CODE|OPM_CODE|STATE_NUMERIC|"
            "STATE_ALPHA|COUNTY_SEQUENCE|COUNTY_NUMERIC|COUNTY_NAME|PRIMARY_LATITUDE|PRIMARY_LONGITUDE|"
            "DATE_CREATED|DATE_EDITED\n"
        )
        with Timer("Validating input file header"):
            with open(self.working_file, encoding="utf-8-sig") as csv_file:
                header = csv_file.readline()
            if header != expected_header:
                raise RuntimeError(f"Found header does not match expected header.  Invalid file format.")

    def _create_staging_table(self):
        with Timer("Create temporary staging table"):
            execute_dml_sql(f'drop table if exists "{self.staging_table_name}"')
            execute_dml_sql(
                f'create temporary table "{self.staging_table_name}" (like "{self.destination_table_name}" including all)'
            )
            execute_dml_sql(f'alter table "{self.staging_table_name}" drop column "id"')

    def _import_input_file(self):
        import_command = (
            f'copy "{self.staging_table_name}" (feature_id, feature_name, feature_class, census_code, '
            f"census_class_code, gsa_code, opm_code, state_numeric, state_alpha, county_sequence, county_numeric, "
            f"county_name, primary_latitude, primary_longitude, date_created, date_edited) from stdin with "
            f"(format csv, header, delimiter '|')"
        )
        with Timer("Importing file to staging table"):
            with connection.cursor() as cursor:
                with open(self.working_file, encoding="utf-8-sig") as csv_file:
                    cursor.cursor.copy_expert(import_command, csv_file, size=10485760)  # 10MB
                    logger.info(f"{cursor.cursor.rowcount:,} rows imported")

    def _vacuum_tables(self):
        with Timer(f"Vacuum {self.destination_table_name}"):
            execute_dml_sql(f'vacuum (full, analyze) "{self.destination_table_name}"')
