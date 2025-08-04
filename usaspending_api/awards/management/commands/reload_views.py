import logging

from django.core.management.base import BaseCommand
from django.db import connection

from usaspending_api.awards.models.transaction_normalized import vw_transaction_normalized_sql
from usaspending_api.awards.models.transaction_fabs import vw_transaction_fabs_sql
from usaspending_api.awards.models.transaction_fpds import vw_transaction_fpds_sql
from usaspending_api.awards.models.award import vw_awards_sql


class Command(BaseCommand):

    help = "Drop and recreate the transaction and award views"
    logger = logging.getLogger("script")

    views = {
        "vw_awards": vw_awards_sql,
        "vw_transaction_normalized": vw_transaction_normalized_sql,
        "vw_transaction_fpds": vw_transaction_fpds_sql,
        "vw_transaction_fabs": vw_transaction_fabs_sql,
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--views",
            "-n",
            choices=list(self.views.keys()),
            default=list(self.views.keys()),
            nargs="+",
            help="Specify specific views to reload. Defaults to all.",
        )

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            for view_name, view_sql in self.views.items():
                if view_name in options.get("views"):
                    self.logger.info(f"Reloading {view_name}")
                    cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
                    cursor.execute(view_sql)
