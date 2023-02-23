import logging
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db import transaction, connection

from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages, read_sql_file


class Command(BaseCommand):

    help = (
        "By default, this command will use the `c_to_d_linkage_updates` table to determine which FABA",
        "records need to be updated. This table is populated by the `update_file_c_linkages_in_delta`",
        "command which should be run before this command during the nightly pipeline. It contains a",
        "mapping from FABA records to Awards. However, if the --recalculate-linkages flag is used, the",
        "necessary updates will be reculated using a series of SQL files",
    )

    UPDATE_LINKAGES_SQL = """
    UPDATE
        public.financial_accounts_by_awards AS faba
    SET
        award_id = updates.award_id
    FROM
        public.c_to_d_linkage_updates AS updates
    WHERE
        faba.financial_accounts_by_awards_id = updates.financial_accounts_by_awards_id;
    """

    LINKAGE_TYPES = ["contract", "assistance"]
    ETL_SQL_FILE_PATH = "usaspending_api/etl/management/sql/"
    logger = logging.getLogger("script")

    def add_arguments(self, parser):
        parser.add_argument(
            "--recalculate-linkages",
            action="store_true",
            required=False,
            help="Recalculate the necesarry linkages using a series of SQL files instead of using a precalculated list",
        )

        parser.add_argument(
            "--file-d-table",
            help="Name of File D table used to calculate linkages. Only applicable with `--recalculate-linkages` flag",
            type=str,
            required=True,
        )

        parser.add_argument(
            "--submission-ids",
            help="One or more submission_ids to be updated. Only applicable with the `--recalculate-linkages` flag",
            nargs="+",
            type=int,
        )

    def handle(self, *args, **options):

        recalculate_linkages = options["recalculate_linkages"]
        file_d_table = options["file_d_table"]
        submission_ids = options["submission_ids"]

        if recalculate_linkages:
            # If recalculate linkages argument is used, run through SQL files to link File C to provided File D table
            with transaction.atomic():
                self.unlink_from_removed_awards(file_d_table)
                if submission_ids:
                    for sub in submission_ids:
                        self.run_linkage_sql(file_d_table, sub)
                else:
                    self.run_linkage_sql(file_d_table)
        else:
            # Otherwise use the `c_to_d_linkage_updates` table to update the FABA table
            with connection.cursor() as cursor:
                self.logger.info("Updating FABA records using `c_to_d_linkage_updates` table.")
                cursor.execute(self.UPDATE_LINKAGES_SQL)

    def run_linkage_sql(self, file_d_table, submission=None):
        for link_type in self.LINKAGE_TYPES:
            update_c_to_d_linkages(type=link_type, file_d_table=file_d_table, submission_id=submission)

    def unlink_from_removed_awards(self, file_d_table):
        """Unlinks FABA records from Awards that no longer exist"""
        self.logger.info("Updating any FABA records that have an award ID of an award that no longer exists.")

        update_filename = "update_faba_award_ids.sql"
        update_file_path = f"{self.ETL_SQL_FILE_PATH}c_file_linkage/{update_filename}"
        update_sql_command = read_sql_file(file_path=update_file_path)
        update_sql_command = update_sql_command[0].format(file_d_table=file_d_table)

        sql_execution_start_time = datetime.now()

        # Replace award_id values with NULL if the award doesn't exist
        self.logger.info(f"Running {update_filename}")
        with connection.cursor() as cursor:
            cursor.execute(update_sql_command)

        self.logger.info(
            f"Finished the FABA award_id update query in {datetime.now() - sql_execution_start_time} seconds"
        )
