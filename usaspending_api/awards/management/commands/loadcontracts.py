from django.core.management.base import BaseCommand, CommandError
from usaspending_api.awards.models import Award, Procurement
from usaspending_api.references.models import LegalEntity, Agency
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from datetime import datetime
import csv
import logging
import django


class Command(BaseCommand):
    help = "Loads contracts from a usaspending contracts download. \
            Usage: `python manage.py loadcontracts source_file_path`"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):
        field_map = {
            "federal_action_obligation": "dollarsobligated",
            "description": "descriptionofcontractrequirement",
            "modification_number": "modnumber"
        }

        value_map = {
            "data_source": "USA",
            "award": lambda row: Award.objects.get_or_create(piid=row['piid'], type='C')[0],
            "recipient": lambda row: LegalEntity.objects.get_or_create(recipient_name=row['dunsnumber'])[0],
            "awarding_agency": lambda row: Agency.objects.get(subtier_code=self.get_agency_code(row['maj_agency_cat'])),
            "action_date": lambda row: self.convert_date(row['signeddate']),
            "submission": SubmissionAttributes.objects.all().first()  # Probably want to change this?
        }

        loader = ThreadedDataLoader(Procurement, field_map=field_map, value_map=value_map, post_row_function=self.post_row_process_function)
        loader.load_from_file(options['file'][0])

    def convert_date(self, date):
        return datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

    def get_agency_code(self, maj_agency_cat):
        return maj_agency_cat.split(':')[0]

    def post_row_process_function(self, row, instance):
        instance.award.update_from_mod(instance)
