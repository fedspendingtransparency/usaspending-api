import logging

from django.conf import settings

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
            "--dry-run",
            dest="skip_deletes",
            action="store_true",
            help="Obtain the list of removed transactions, but skip the delete step.",
        )

    def handle(self, *args, **options):
        if not (settings.USASPENDING_AWS_REGION and settings.FPDS_BUCKET_NAME):
            raise Exception("Missing one or more environment variables: 'USASPENDING_AWS_REGION', 'FPDS_BUCKET_NAME'")

        logger.info(f"Starting processing deletes from {options['datetime']}")
        try:
            self.execute_script(options)
        except Exception:
            logger.exception("Fatal error when processing deletes")
            return -1

        return 0

    def execute_script(self, options):
        removed_records = self.fetch_deleted_transactions(options["datetime"].date())
        if removed_records is None:
            return

        if options["skip_deletes"] and removed_records:
            logger.warn(f"--dry-run flag used, skipping delete operations. IDs: {removed_records}")
        elif removed_records:
            self.delete_rows(removed_records)
        else:
            logger.warn("Nothing to delete")

        self.store_delete_records(removed_records)

    def delete_rows(self, removed_records):
        delete_template = "DELETE FROM {table} WHERE {key} IN {ids} AND updated_at < '{date}'::date"
        connection = get_connection(read_only=False)
        with connection.cursor() as cursor:
            for date, ids in removed_records.items():
                if len(ids) == 1:  # handle case with single-value Python tuples contain a trailing comma "(id,)"
                    ids = ids + ids
                sql = delete_template.format(
                    table=self.destination_table_name, key=self.shared_pk, ids=tuple(ids), date=date
                )
                cursor.execute(sql)
                logger.info(f"Removed {cursor.rowcount} rows from {date} delete records")

    def fetch_deleted_transactions(self, date):
        raise NotImplementedError

    def store_delete_records(self, id_list):
        raise NotImplementedError
