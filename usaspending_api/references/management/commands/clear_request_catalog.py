from datetime import datetime
import logging
import os
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist

from usaspending_api.common.models import RequestCatalog


class Command(BaseCommand):
    """
    This command will remove all RequestCatalog entires from the database
    """
    help = "Removes all CSVdownloadableResponses from the database"
    logger = logging.getLogger('console')

    def handle(self, *args, **options):
        # This will throw an exception and exit the command if the id doesn't exist
        deleted = RequestCatalog.objects.all().delete()

        statistics = "Statistics:\n  Total objects Removed: " + str(deleted[0])
        for model in deleted[1].keys():
            statistics = statistics + "\n  " + model + ": " + str(deleted[1][model])

        self.logger.info(statistics)
