from datetime import datetime
import logging
import os
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist

from usaspending_api.common.csv_helpers import s3_empty_bucket
from django.conf import settings


class Command(BaseCommand):
    """
    This command will remove all CSVdownloadableResponse entires from the database
    """
    help = "Removes all CSVdownloadableResponses from the database"
    logger = logging.getLogger('console')

    def handle(self, *args, **options):
        # This will throw an exception and exit the command if the id doesn't exist
        try:
            emptied = s3_empty_bucket()
            if emptied:
                self.logger.info("S3 Bucket {} has been emptied".format(settings.CSV_S3_BUCKET_NAME))
        except Exception as e:
            self.logger.exception(e)
