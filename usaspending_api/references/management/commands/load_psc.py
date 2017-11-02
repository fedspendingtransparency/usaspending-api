from django.core.management.base import BaseCommand
from usaspending_api.references.models import PSC
import os
import csv
import logging


class Command(BaseCommand):
    help = 'Loads program information obtained from csv file on https://www.acquisition.gov/PSC_Manual'

    logger = logging.getLogger('console')

    def handle(self, *args, **options):
        DEFAULT_DIRECTORY = os.path.normpath('usaspending_api/references/management/commands/')
        DEFAULT_FILEPATH = os.path.join(DEFAULT_DIRECTORY, 'psc_codes.csv')

        load_psc(DEFAULT_FILEPATH)
        self.logger.log(20, 'Loaded PSC codes successfully.')


def load_psc(fullpath):
    """
    Create CFDA Program records from a CSV of historical data.
    """
    try:
        logger = logging.getLogger('console')

        with open(fullpath, errors='backslashreplace', encoding='utf-8-sig') as csvfile:

            reader = csv.DictReader(csvfile, delimiter=',', quotechar='"', skipinitialspace='true')

            for row in reader:
                psc_code = row['PSC CODE']
                psc_description = row['PRODUCT AND SERVICE CODE NAME'].strip()

                if len(psc_code) < 4:
                    logger.info('Skipping PSC code {}, not a 4-digit code.'.format(psc_code))
                    continue

                psc, created = PSC.objects.get_or_create(code=psc_code)
                psc.description = psc_description
                psc.save()

    except IOError:
        logger.error('Could not open file {}'.format(fullpath))
