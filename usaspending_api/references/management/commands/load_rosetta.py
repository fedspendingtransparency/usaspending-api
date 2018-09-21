import logging
import os.path

from django.conf import settings
from django.core.management.base import BaseCommand
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from usaspending_api.references.models import Definition

logger = logging.getLogger('console')


class Command(BaseCommand):
    help = "Loads an Excel spreadsheet of DATA Act/USAspending data names across the various systems into <>"

    # path = 'usaspending_api/data/USAspendingGlossary.xlsx'
    bucket = "da-public-files"
    s3_filepath = "user_reference_docs/Data Transparency Rosetta Stone_Public_only.xlsx"

    def add_arguments(self, parser):
        parser.add_argument(
            '-p',
            '--path',
            help='filepath to a local Excel spreadsheet to load',
            default=None)

    def handle(self, *args, **options):
        rosetta_object = extract_data_from_source_file(path=options['path'])
        # load_xlsx_data_to_model(rosetta_object)


def extract_data_from_source_file(path=None):
    if path:
        filepath = path

    rosetta_object = {}
    wb = load_workbook(filename=filepath, read_only=True)
    sheet = wb["Public"]
    last_column = get_column_letter(sheet.max_column)
    last_row = sheet.max_row
    cell_range = "A2:{}2".format(last_column)
    # print(f"cell range {cell_range}")
    headers = [cell.value for cell in sheet[cell_range][0]]
    print(headers)
    # print(last_column)
    # print(last_row)

    return rosetta_object


def load_xlsx_data_to_model(rosetta_object):

    logger = logging.getLogger('console')

    wb = load_workbook(filename=path)
    ws = wb.active
    rows = ws.rows

    headers = [c.value for c in next(rows)[:6]]
    expected_headers = [
        'Term', 'Plain Language Descriptions', 'DATA Act Schema Term',
        'DATA Act Schema Definition', 'More Resources', 'Markdown for More Resources'
    ]
    if headers != expected_headers:
        raise Exception('Expected headers of {} in {}'.format(
            expected_headers, path))

    if append:
        logging.info('Appending definitions to existing guide')
    else:
        logging.info('Deleting existing definitions from guide')
        Definition.objects.all().delete()

    field_names = ('term', 'plain', 'data_act_term', 'official', None, 'resources')
    row_count = 0
    for row in rows:
        if not row[0].value:
            break  # Reads file only until a line with blank `term`
        definition = Definition()
        for (i, field_name) in enumerate(field_names):
            if field_name:
                setattr(definition, field_name, row[i].value)
        definition.save()
        row_count += 1
    logger.info('{} definitions loaded from {}'.format(
        row_count, path))
