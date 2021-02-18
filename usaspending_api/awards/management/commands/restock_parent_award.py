import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):

    help = "Empty and repopulate parent_award table with IDV aggregates and counts"
    logger = logging.getLogger("script")

    def add_arguments(self, parser):

        mutually_exclusive_group = parser.add_mutually_exclusive_group()

        command = "vacuum (full, analyze) parent_award"
        mutually_exclusive_group.add_argument(
            "--vacuum-full-analyze",
            action="store_const",
            const=command,
            dest="vacuum",
            help='Runs "%s" after restocking parent_award table' % command,
        )

        command = "vacuum (full) parent_award"
        mutually_exclusive_group.add_argument(
            "--vacuum-full",
            action="store_const",
            const=command,
            dest="vacuum",
            help='Runs "%s" after restocking parent_award table' % command,
        )

        command = "vacuum (analyze) parent_award"
        mutually_exclusive_group.add_argument(
            "--vacuum-analyze",
            action="store_const",
            const=command,
            dest="vacuum",
            help='Runs "%s" after restocking parent_award table' % command,
        )

    def handle(self, *args, **options):

        sql_path = str(settings.APP_DIR / "awards/management/sql/restock_parent_award.sql")

        with open(sql_path) as f:
            sql = f.read()

        with connection.cursor() as cursor:

            self.logger.info("Restocking parent_award")
            cursor.execute(sql)

            vacuum = options.get("vacuum")

            if vacuum:
                self.logger.info("Vacuuming parent_award")
                cursor.execute(vacuum)
