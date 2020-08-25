from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages


class Command(BaseCommand):

    LINKAGE_TYPES = ["contract", "assistance"]
    ETL_SQL_FILE_PATH = "usaspending_api/etl/management/sql/"

    def add_arguments(self, parser):
        parser.add_argument(
            "--submission-ids", help=("One or more Broker submission_ids to be updated."), nargs="+", type=int,
        )

    def handle(self, *args, **options):

        with transaction.atomic():
            if options.get("submission_ids"):
                for sub in options["submission_ids"]:
                    self.run_sql(sub)
            else:
                self.run_sql()

    def run_sql(self, submission=None):
        for link_type in self.LINKAGE_TYPES:
            update_c_to_d_linkages(type=link_type, submission_id=submission)
