from django.core.management.base import BaseCommand
from usaspending_api.references.models import NAICS
import os
import csv
import logging
import django


class Command(BaseCommand):
    help = "Loads program information obtained from csv file on census.gov"

    logger = logging.getLogger('console')

    DEFAULT_DIRECTORY = os.path.normpath('usaspending_api/references/management/commands/')
    DEFAULT_FILEPATH = os.path.join(DEFAULT_DIRECTORY,  'naics_codes.csv')

    def add_arguments(self, parser):
            parser.add_argument(
                '-f', '--file',
                default=self.DEFAULT_FILEPATH,
                help='path to CSV file to load',
            )

    def handle(self, *args, **options):

        filepath = options['file']
        fullpath = os.path.join(django.conf.settings.BASE_DIR, filepath)

        load_naics(fullpath)
        self.logger.log(20, "Loading NAICS complete.")


def load_naics(fullpath):
    """
    Create CFDA Program records from a CSV of historical data.
    """
    try:
        with open(fullpath, errors='backslashreplace', encoding='utf-8-sig') as csvfile:

            reader = csv.DictReader(csvfile, delimiter=',', quotechar='"', skipinitialspace='true')
            for row in reader:
                naics, created = NAICS.objects.get_or_create(
                                code=row['2017 NAICS Code'])
                naics.description = row['2017 NAICS Title'].strip()
                naics.save()
    except IOError:
        logger = logging.getLogger('console')
        logger.log("Could not open file to load from")
