import logging
import os.path

from django.conf import settings
from django.core.management.base import BaseCommand
from openpyxl import load_workbook
from usaspending_api.common.helpers.generic_helper import natural_sort
import re

# from usaspending_api.references.models import NAICSRaw
from usaspending_api.references.models import NAICS


class Command(BaseCommand):
    help = "Updates DB from Excel spreadsheets of USAspending terminology definitions into the naics model"

    logger = logging.getLogger('console')

    path = 'usaspending_api/data/naics_archive'
    path = os.path.normpath(path)
    default_path = os.path.join(settings.BASE_DIR, path)

    def add_arguments(self, parser):
        parser.add_argument('-p', '--path', help='the path to the Excel spreadsheet to load', default=self.default_path)
        parser.add_argument('-a', '--append', help='Append to existing guide', action='store_true', default=True)

    def handle(self, *args, **options):

        load_NAICS(path=options['path'], append=options['append'])


def load_NAICS(path, append):
    # year regex object
    p_year = re.compile("(20[0-9]{2})")

    # case-insensitive naics regex object (for verification)
    p_name = re.compile("naics", re.IGNORECASE)
    naics_list = NAICS.objects.all()
    dir_files = []

    for fname in os.listdir(path):
        if (fname.find("naics") > 0):
            dir_files.append(os.path.join(path, fname))

    # Sort by ints found

    dir_files = natural_sort(dir_files, reverse=True)

    for path in dir_files:
        logger = logging.getLogger('console')
        wb = load_workbook(filename=path)
        ws = wb.active
        rows = ws.rows

        headers = [c.value for c in next(rows)[:2]]
        expected_headers = ['naics', 'description']
        if ((headers[0] is not None) and (headers[1] is not None)):
            print(headers[1])
            year = p_year.search(headers[0]).group()
            contains_title = p_name.search(headers[1]).group()
            if ((year is None) or (contains_title is None)):
                raise Exception('Expected header containing "year; \'naics\'"')

            if append:
                logging.info('Appending definitions to existing guide')
            else:
                logging.info('Deleting existing definitions from guide')
                NAICS.objects.all().delete()

            row_count = 0
            for row in rows:
                if not row[0].value:
                    break  # Reads file only until a line with blank `term`

                naics_code = row[0].value
                naics_desc = row[1].value

                if (NAICS.objects.get(pk=naics_code) is None):
                    naics_item = NAICS(code=code, description=naics_desc)
                    naics_item.save()

            logger.info('{} naics data loaded from {}'.format(row_count, path))
