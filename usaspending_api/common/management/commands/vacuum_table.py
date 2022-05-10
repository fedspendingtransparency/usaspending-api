import logging

from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connection


logger = logging.getLogger("console")


class Command(BaseCommand):
    help = "Vacuum Analyze the specific list of tables"

    def add_arguments(self, parser):
        parser.add_argument(
            "-t",
            "--tables",
            dest="tables",
            action="append",
            nargs="+",
            default=[],
            help="List of space separated table names. Ex: python manage.py vacuum_table table1 table2",
        )

        parser.add_argument(
            "-a",
            "--all",
            dest="all",
            action="store_true",
            default=False,
            help="Flag to run VACUUM ANALYZE on all tables",
        )

    def handle(self, *args, **options):
        total_start = datetime.now()
        tables = options.get("tables")

        if options.get("all"):  # if parameter is not provided, run vacuum analyze on the entire database
            logger.info("Running VACUUM ANALZYE on entire database...")
            with connection.cursor() as cursor:
                cursor.execute("VACUUM VERBOSE ANALYZE;")
        else:
            tables = tables[0]
            for table in tables:
                logger.info("Running VACUUM ANALYZE on the %s table" % table)
                with connection.cursor() as cursor:
                    cursor.execute("VACUUM VERBOSE ANALYZE %s;" % table)
                logger.info(
                    "Finished running VACUUM ANALYZE on the %s table in %s seconds"
                    % (table, str(datetime.now() - total_start))
                )

        logger.info("Finished VACUUM ANALYZE-ing tables in %s seconds" % str(datetime.now() - total_start))
