import logging
import os
import csv
import boto

from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.recipient.models import StateData

from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger('console')

LOCAL_STATE_DATA_FILENAME = 'CensusStateData.csv'
LOCAL_STATE_DATA = os.path.join(settings.BASE_DIR, 'usaspending_api', 'data', LOCAL_STATE_DATA_FILENAME)


class Command(BaseCommand):
    help = "Loads state data from Census data"

    def add_arguments(self, parser):
        parser.add_argument('file', nargs='?', help='the file to load')

    def handle(self, *args, **options):
        state_data_field_map = {
            'fips': 'FIPS',
            'code': 'Code',
            'name': 'Name',
            'type': 'Type',
            'year': 'Year',
            'population': 'Population',
            'pop_source': 'Population Source',
            'median_household_income': 'Median Household Income',
            'mhi_source': 'Median Household Income Source'
        }

        csv_file = options['file']
        remote = False

        if csv_file:
            if not os.path.exists(csv_file):
                raise FileExistsError(csv_file)
            elif os.path.splitext(csv_file)[1] != '.csv':
                raise Exception('Wrong filetype provided, expecting csv')
            file_path = csv_file
        elif not settings.IS_LOCAL and os.environ.get('USASPENDING_AWS_REGION') and os.environ.get('STATE_DATA_BUCKET'):
            s3connection = boto.s3.connect_to_region(os.environ.get('USASPENDING_AWS_REGION'))
            s3bucket = s3connection.lookup(os.environ.get('STATE_DATA_BUCKET'))
            key = s3bucket.get_key(LOCAL_STATE_DATA_FILENAME)
            file_path = os.path.join('/', 'tmp', LOCAL_STATE_DATA_FILENAME)
            key.get_contents_to_filename(file_path)
            remote = True
        else:
            file_path = LOCAL_STATE_DATA

        with open(file_path) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Defaulting to None's instead of ''s
                row = {key: (value or None) for key, value in row.items()}
                load_data_into_model(
                    StateData(),
                    row,
                    field_map=state_data_field_map,
                    save=True
                )

        if remote:
            os.remove(file_path)
        logger.info('Loading StateData complete')
