import logging

from datetime import datetime
from django.conf import settings
from typing import Optional

from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type

logger = logging.getLogger("script")


class AgnosticDeletes:
    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            dest="datetime",
            required=True,
            type=datetime_command_line_argument_type(naive=True),  # Broker and S3 date/times are naive.
            help="Load/Reload records from the provided datetime to the script execution start time.",
        )
        parser.add_argument(
            "--dry-run", action="store_true", help="Obtain the list of removed transactions, but skip the delete step."
        )
        parser.add_argument(
            "--skip-upload",
            action="store_true",
            help="Don't store the list of IDs for downline ETL. Automatically skipped if --dry-run is provided",
        )

    def handle(self, *args, **options):
        self.dry_run = options["dry_run"]
        self.skip_upload = self.dry_run or options["skip_upload"]

        if not self.skip_upload and not (
            settings.USASPENDING_AWS_REGION and settings.DELETED_TRANSACTION_JOURNAL_FILES
        ):
            raise Exception(
                "Missing one or more environment variables: 'USASPENDING_AWS_REGION', 'DELETED_TRANSACTION_JOURNAL_FILES'"
            )

        logger.info(f"Processing deletes from '{options['datetime']}' - present")
        try:
            self.execute_script(options)
        except Exception:
            logger.exception("Fatal error when processing deletes")
            raise SystemExit(1)

        return 0

    def execute_script(self, options: dict) -> None:
        removed_records = self.fetch_deleted_transactions(options["datetime"])
        if removed_records is None:
            logger.warning("Nothing to delete")
            return

        if self.dry_run and removed_records:
            logger.warning(f"--dry-run flag used, skipping delete operations. IDs: {removed_records}")
        else:
            self.delete_rows(removed_records)

        if removed_records and not self.skip_upload:
            self.store_delete_records(removed_records)

    def delete_rows(self, removed_records: dict) -> None:
        delete_template = "DELETE FROM {table} WHERE {key} IN {ids} AND updated_at < '{date}'::date"
        connection = get_connection(read_only=False)
        with connection.cursor() as cursor:
            for date, ids in removed_records.items():
                if len(ids) == 1:  # handle case with single-value Python tuples contain a trailing comma "(id,)"
                    ids += ids

                if len(ids) == 0:
                    logger.warning(f"No records to delete for '{date}'")
                else:
                    sql = delete_template.format(
                        table=self.destination_table_name, key=self.shared_pk, ids=tuple(ids), date=date
                    )
                    cursor.execute(sql)
                    logger.info(f"Removed {cursor.rowcount} rows previous to '{date}'")

    def fetch_deleted_transactions(self, date_time: datetime) -> Optional[dict]:
        raise NotImplementedError

    def store_delete_records(self, deleted_dict: dict) -> None:
        raise NotImplementedError
