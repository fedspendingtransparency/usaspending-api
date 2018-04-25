import logging
import boto
import os
import csv

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.etl.csv_data_reader import CsvDataReader
from usaspending_api.references.models import RefProgramActivity

BUCKET_NAME = 'gtas-sf133'
FILE_NAME = 'program_activity.csv'


class Command(BaseCommand):
    help = "Loads program activity codes."
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs='?', help='the file to load')

    def handle(self, *args, **options):

        # Create the csv reader
        csv_file = options['file']
        if not csv_file:
            # Get program activity csv from
            # moving it to self.bucket as it may be used in different cases
            region_name = settings.BULK_DOWNLOAD_AWS_REGION
            self.bucket = boto.s3.connect_to_region(region_name).get_bucket(BUCKET_NAME)
            keys = list(self.bucket.list(prefix=FILE_NAME))
            if len(keys) == 0:
                self.logger.error("Program activity file not found in bucket. Exiting.")
                return
            elif len(keys) > 1:
                self.logger.error("Found multiple program activity files. Exiting.")
                return
            else:
                self.logger.info('Retrieving program activity file.')
                csv_file = os.path.join('/', 'tmp', FILE_NAME)
                keys[0].get_contents_to_filename(csv_file)
                # lower headers
                with open(csv_file) as data:
                    data = csv.reader(data)
                    header = [row.lower() for row in next(data)]
                    updated_data = [header] + list(data)
                with open(csv_file, 'w') as data:
                    writer = csv.writer(data)
                    writer.writerows(updated_data)
        reader = CsvDataReader(csv_file)

        try:
            self.logger.info('Processing {}'.format(FILE_NAME))
            with transaction.atomic():
                # Load program activity file in a single transaction to ensure
                # integrity and to speed things up a bit
                for idx, row in enumerate(reader):
                    get_or_create_program_activity(row)
        except Exception as e:
            self.logger.exception(e)
        finally:
            if not options['file']:
                os.remove(csv_file)


def get_or_create_program_activity(row):
    """
    Create or update a program activity object.

    Args:
        row: a csv reader row

    Returns:
        True if a new program activity rows was created, False
        if an existing row was updated
    """

    obj, created = RefProgramActivity.objects.get_or_create(
        program_activity_code=row['pa_code'].strip().zfill(4),
        budget_year=row['year'],
        responsible_agency_id=row['agency_id'].strip().zfill(3),
        main_account_code=row['account'].strip().zfill(4),
        defaults={'program_activity_name': row['pa_name'].strip().upper()}
    )

    return created
