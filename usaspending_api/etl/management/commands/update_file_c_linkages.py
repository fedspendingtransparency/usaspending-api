import logging
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db import transaction, connection

from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages, read_sql_file
from usaspending_api.common.exceptions import InvalidParameterException


class Command(BaseCommand):

    LINKAGE_TYPES = ["contract", "assistance"]
    ETL_SQL_FILE_PATH = "usaspending_api/etl/management/sql/"
    logger = logging.getLogger("script")

    def add_arguments(self, parser):
        parser.add_argument(
            "--submission-ids", help=("One or more Broker submission_ids to be updated."), nargs="+", type=int
        )

    def handle(self, *args, **options):
        with transaction.atomic():
            if options.get("submission_ids"):
                for sub in options["submission_ids"]:
                    self.run_sql(sub)
            else:
                self.run_sql()
            self.faba_award_id_check()

    def run_sql(self, submission=None):
        for link_type in self.LINKAGE_TYPES:
            update_c_to_d_linkages(type=link_type, submission_id=submission)

    def faba_award_id_check(self):
        """Checks for any FABA records with an award_id associated with an award
        that no longer exists. If any records are found, the award_id is set to NULL.
        """
        self.logger.info("Checking for any FABA records that have an award ID of an award that no longer exists.")

        check_filename = "check_faba_award_ids.sql"
        update_filename = "update_faba_award_ids.sql"

        check_file_path = f"{self.ETL_SQL_FILE_PATH}c_file_linkage/{check_filename}"
        update_file_path = f"{self.ETL_SQL_FILE_PATH}c_file_linkage/{update_filename}"

        check_sql_command = read_sql_file(file_path=check_file_path)
        update_sql_command = read_sql_file(file_path=update_file_path)

        if len(check_sql_command) != 1 or len(update_sql_command) != 1:
            raise InvalidParameterException(
                "Invalid number of commands in SQL file. File should contain 1 SQL command."
            )

        sql_execution_start_time = datetime.now()

        # Count the number of FABA records with award_id values that need to be updated
        self.logger.info(f"Running {check_filename}")
        with connection.cursor() as cursor:
            cursor.execute(check_sql_command[0])
            before_change_count = cursor.fetchall()[0][0]
        self.logger.info(f"Count of FABA rows that need their award_id values updated: {before_change_count}")

        if before_change_count > 0:
            # Replace award_id values with NULL if the award doesn't exist
            self.logger.info(f"Running {update_filename}")
            with connection.cursor() as cursor:
                cursor.execute(update_sql_command[0])

        self.logger.info(f"Finished FABA award_id queries in {datetime.now() - sql_execution_start_time} seconds")
