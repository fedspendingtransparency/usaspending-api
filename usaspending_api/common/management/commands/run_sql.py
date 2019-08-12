from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connection
import logging

logger = logging.getLogger("console")


class Command(BaseCommand):
    help = "Run the SQL file(s) provided as an argument. Files must be provided with relative or absolute paths"

    @staticmethod
    def run_sql_file(file_path):
        with connection.cursor() as cursor:
            with open(file_path) as infile:
                for raw_sql in infile.read().split("\n\n\n"):
                    if raw_sql.strip():
                        cursor.execute(raw_sql)

    def add_arguments(self, parser):
        parser.add_argument(
            "-f",
            "--files",
            dest="files",
            action="append",
            nargs="+",
            default=[],
            help="List of space separated file names. Ex: python manage.py run_sql /path/to/file1.sql "
            "/path/to/file2.sql",
        )

    def handle(self, *args, **options):
        total_start = datetime.now()
        files = options.get("files")

        files = files[0]
        for file in files:
            if not file.endswith(".sql"):
                logger.info("Skipping %s due to incorrect file extension" % file)

            start = datetime.now()
            logger.info("Running %s" % file)
            self.run_sql_file(file)
            logger.info("Finished %s in %s seconds" % (file, str(datetime.now() - start)))

        logger.info("Finished all queries in %s seconds" % str(datetime.now() - total_start))
