import logging

from django.core.management import call_command
from django.core.management.base import BaseCommand
from pathlib import Path

from usaspending_api.common.helpers.date_helper import datetime_command_line_argument_type

logger = logging.getLogger("script")


class Command(BaseCommand):

    help = """TEMPORARY scaffolding for elasticsearch_indexer to allow graceful OPS switchover"""

    def add_arguments(self, parser):
        parser.add_argument(
            "fiscal_years",
            nargs="+",
            type=str,
            metavar="fiscal-years",
            help="Provide a list of fiscal years to process. For convenience, provide 'all' for FY2008 to current FY",
        )
        parser.add_argument(
            "--process-deletes",
            action="store_true",
            help="When this flag is set, the script will include the process to "
            "obtain records of deleted transactions from S3 and remove from the index",
        )
        parser.add_argument(
            "--dir",
            default=str(Path(__file__).resolve().parent),
            type=str,
            help="Set for a custom location of output files",
            dest="directory",
        )
        parser.add_argument(
            "--skip-counts",
            action="store_true",
            help="When this flag is set, the ETL process will skip the record counts to reduce operation time",
        )
        parser.add_argument(
            "--index-name",
            type=str,
            help="Provide name for new index about to be created. Only used when --create-new-index is provided",
        )
        parser.add_argument(
            "--create-new-index",
            action="store_true",
            help="It needs a new unique index name and set aliases used by API logic to the new index",
        )
        parser.add_argument(
            "--snapshot",
            action="store_true",
            help="Create a new Elasticsearch snapshot of the current index state which is stored in S3",
        )
        parser.add_argument(
            "--start-datetime",
            type=datetime_command_line_argument_type(naive=False),
            help="Processes transactions updated on or after the UTC date/time provided. yyyy-mm-dd hh:mm:ss is always "
            "a safe format. Wrap in quotes if date/time contains spaces.",
        )
        parser.add_argument(
            "--skip-delete-index",
            action="store_true",
            help="When creating a new index skip the step that deletes the old indexes and swaps the aliases. "
            "Only used when --create-new-index is provided.",
        )
        parser.add_argument(
            "--load-type",
            type=str,
            help="Select which type of load to perform, current options are transactions or awards.",
            choices=["transactions", "awards"],
            default="transactions",
        )
        parser.add_argument(
            "--idle-wait-time",
            type=int,
            help="Time in seconds the ES index process should wait before looking for a new CSV data file.",
            default=60,
        )

    def handle(self, *args, **options):
        # Chose a "whitelist" instead of poping unwanted keys to prevent
        #  unwanted key-value pairs from sneaking in
        desired_options = {
            "verbosity": options["verbosity"],
            "process_deletes": options["process_deletes"],
            "skip_counts": options["skip_counts"],
            "index_name": options["index_name"],
            "create_new_index": options["create_new_index"],
            "snapshot": options["snapshot"],
            "start_datetime": options["start_datetime"],
            "skip_delete_index": options["skip_delete_index"],
            "load_type": options["load_type"],
            "idle_wait_time": options["idle_wait_time"],
        }

        call_command("elasticsearch_indexer", *options["fiscal_years"], **desired_options)
