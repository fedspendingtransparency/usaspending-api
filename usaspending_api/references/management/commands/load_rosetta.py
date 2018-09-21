import logging
import os.path
import json
from time import perf_counter
from collections import OrderedDict


from django.db import transaction
from django.conf import settings
from django.core.management.base import BaseCommand
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from usaspending_api.references.models import Rosetta

logger = logging.getLogger('console')

DB_TO_XLSX_MAPPING = OrderedDict([
    # DB contains a section field which is actually a XLSX header over that column
    ("element", "Element"),
    ("definition", "Definition"),
    ("fpds_element", "FPDS Element"),
    ("file_a_f", "File\nA-F"),
    ("award_file", "Award File"),
    ("award_element", "Award Element"),
    ("subaward_file", "Subaward File"),
    ("subaward_element", "Subaward Element"),
    ("account_file", "Account File"),
    ("account_element", "Account Element"),
    ("legacy_award_element", "Award Element"),
    ("legacy_subaward_element", "Subaward Element"),
])


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
        logger.info("Starting load_rosetta management command")
        script_start_time = perf_counter()
        rosetta_object = extract_data_from_source_file(path=options['path'])
        load_xlsx_data_to_model(rosetta_object)
        # print(json.dumps(rosetta_object))
        logger.info("Script completed in {:.2f}s".format(perf_counter() - script_start_time))


def extract_data_from_source_file(path: str=None) -> dict:
    if path:
        logger.info("Using local file: {}".format(path))
        filepath = path
    else:
        logger.info("Using S3 file: {}".format("TBD"))

    wb = load_workbook(filename=filepath)
    sheet = wb["Public"]
    last_column = get_column_letter(sheet.max_column)
    cell_range = "A2:{}2".format(last_column)
    # print(f"cell range {cell_range}")
    headers = [{"column": cell.column, "header": cell.value} for cell in sheet[cell_range][0]]

    current_section = None
    for header in headers:
        header["section"] = sheet["{}1".format(header["column"])].value
        if header["section"] is None:
            header["section"] = current_section
        else:
            current_section = header["section"]

    elements = {}
    for i, row in enumerate(sheet.values):
        if i < 2:
            continue
        elements[row[0]] = [r.strip() for r in row]

    field_names = list(header["header"] for header in headers)
    assert list(DB_TO_XLSX_MAPPING.values()) == field_names, "Column headers don't match the headers in the model"
    logger.info("Columns in file: {} with {} rows of elements".format(field_names, len(elements)))
    return {"headers": headers, "data": elements}


@transaction.atomic
def load_xlsx_data_to_model(rosetta_object: dict):
    Rosetta.objects.all().delete()

    for header, row in rosetta_object["data"].items():
        zipped_data = zip(DB_TO_XLSX_MAPPING.keys(), row)
        db_insert_params = {z[0]: z[1] for z in zipped_data}

        json_doc = {
            "section": DB_TO_XLSX_MAPPING[header],
        }
        for x in zip(DB_TO_XLSX_MAPPING.values(), row):
            json_doc[x[0]] = x[1]
        print(json.dumps(json_doc))
        break
        rosetta = Rosetta(full_doc=db_insert_params, **db_insert_params)
        rosetta.save()


    #     for (i, field_name) in enumerate(field_names):
    #         if field_name:
    #             setattr(rosetta, field_name, )
    #     rosetta.save()
    #     row_count += 1
    # logger.info('{} definitions loaded from {}'.format(row_count))
