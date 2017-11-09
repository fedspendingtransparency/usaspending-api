from django.db import connection
from django.core.management.base import BaseCommand
import logging


class Command(BaseCommand):
    help = 'One-time fixer to fill missing location.zip5 from zip4'

    # Estimated runtime, based on simple linear extrap: 10 hours

    logger = logging.getLogger('console')

    update_qry = """
       UPDATE references_location
       SET    zip5 = SUBSTRING(zip4 FROM '^(\d{5})\-?(\d{4})?$')
       WHERE  zip5 IS NULL
       AND    zip4 IS NOT NULL
    """

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            cursor.execute(self.update_qry)

