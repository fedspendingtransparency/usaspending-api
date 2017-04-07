import logging

from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.etl.csv_data_reader import CsvDataReader
from usaspending_api.references.models import RefProgramActivity


class Command(BaseCommand):
    help = "Loads program activity codes."
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):

        # Create the csv reader
        csv_file = options['file'][0]
        reader = CsvDataReader(csv_file)

        with transaction.atomic():
            # Load program activity file in a single transaction to ensure
            # integrity and to speed things up a bit
            for idx, row in enumerate(reader):
                get_or_create_program_activity(row)


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
        defaults={'program_activity_name': row['pa_name'].strip()}
    )

    return created
