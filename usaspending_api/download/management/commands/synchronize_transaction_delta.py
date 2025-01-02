"""
The entire purpose of this management command is to coordinate the transaction_delta table between
two databases:

    - a snapshot database from which monthly delta files are generated
    - a "live" database such as development, staging, or (most likely) production

Why do we need such coordination, you ask?  Well, while monthly delta files are being generated from
the snapshot database (which, as of this writing, takes longer than a day), transaction updates may
be performed in the "live" database.  Once delta files are successfully completed in the snapshot
database, we need to remove those records from the transaction_delta table in the "live" database.

So, in a nutshell, this function will delete transaction_delta records from the "live" database
with a created_at date <= the max(created_at) from the snapshot database.
"""

import logging
import os
import psycopg2

from django.core.management.base import BaseCommand
from usaspending_api.awards.models import TransactionDelta


logger = logging.getLogger(__name__)


class Command(BaseCommand):

    help = "Coordinates the removal of transaction_delta rows between two databases."

    @staticmethod
    def get_max_created_at(snapshot_db):
        with psycopg2.connect(dsn=snapshot_db) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select max(created_at) from transaction_delta")
                return cursor.fetchall()[0][0]

    def handle(self, *args, **options):
        snapshot_db = os.environ.get("DOWNLOAD_DATABASE_URL")
        if snapshot_db is None:
            raise EnvironmentError(
                "Missing required environment variable DOWNLOAD_DATABASE_URL which should contain "
                "the connection string for the monthly delta snapshot database."
            )

        max_created_at = self.get_max_created_at(snapshot_db)
        if max_created_at is not None:
            delete_count = TransactionDelta.objects.delete_by_created_at(max_created_at)
            logger.info("{:,} transaction_delta records successfully deleted".format(delete_count))
        else:
            logger.info("No transaction_delta records to delete")
