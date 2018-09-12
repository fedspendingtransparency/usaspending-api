import logging
import os.path

from django.conf import settings
from django.core.management.base import BaseCommand
from openpyxl import load_workbook

from usaspending_api.references.models import NAICSRaw


class Command(BaseCommand):
    help = "Loads an Excel spreadsheet of USAspending terminology definitions into the NAICSRaw model"

    logger = logging.getLogger('console')

    # path = 'usaspending_api/data/USAspendingGlossary.xlsx'
    path = 'usaspending_api/data/naics_archive'
    path = os.path.normpath(path)
    default_path = os.path.join(settings.BASE_DIR, path)

    def add_arguments(self, parser):
        parser.add_argument(
            '-p',
            '--path',
            help='the path to the Excel spreadsheet to load',
            default=self.default_path)
        parser.add_argument(
            '-a',
            '--append',
            help='Append to existing guide',
            action='store_true',
            default=False)

    def handle(self, *args, **options):

        load_NAICS(path=options['path'], append=options['append'])


def load_NAICS(path, append):
    for filename in os.listdir(path):
        logger = logging.getLogger('console')
        path = os.path.join(path, filename)
        wb = load_workbook(filename=path)
        ws = wb.active
        rows = ws.rows

        headers = [c.value for c in next(rows)[:2]]
        expected_headers = [
            'naics', 'item index description'
        ]
        for header in headers:
            if(header[0].lower().find(expected_headers[0]) < 0 or header[1].lower().findexpected_headers[1] < 0):
                raise Exception('Expected headers of {} in {}'.format(
                    expected_headers, path))

        naics_year_header = headers[0]

        if append:
            logging.info('Appending definitions to existing guide')
        else:
            logging.info('Deleting existing definitions from guide')
            NAICSRaw.objects.all().delete()

        field_names = ('naics_year_header', 'naics_id', 'naics_description')
        row_count = 0
        for row in rows:
            if not row[0].value:
                break  # Reads file only until a line with blank `term`
            naics_item = NAICSRaw()
            i = 1
            setattr(naics_item, 'naics_year_header', naics_year_header)
            for (i, field_name) in enumerate(field_names):
                if field_name:
                    setattr(naics_item, field_name, row[i].value)
            naics_iotem.save()
            row_count += 1
        logger.info('{} naics data loaded from {}'.format(
            row_count, path))
