import logging
import os.path
import boto3

from collections import OrderedDict
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from time import perf_counter

from usaspending_api.references.models import Rosetta

logger = logging.getLogger("console")

DB_TO_XLSX_MAPPING = OrderedDict(
    [
        # DB contains a section field which is actually a XLSX header over that column
        ("element", "Element"),
        ("definition", "Definition"),
        ("fpds_element", "FPDS Data Dictionary Element"),

        # "USA Spending Downloads"
        ("award_file", "Award File"),
        ("award_element", "Award Element"),
        ("subaward_file", "Subaward File"),
        ("subaward_element", "Subaward Element"),
        ("account_file", "Account File"),
        ("account_element", "Account Element"),

        # "Legacy USA Spending"
        ("legacy_award_file", "Award File"),
        ("legacy_award_element", "Award Element"),
        ("legacy_subaward_element", "Subaward Element"),
    ]
)


class Command(BaseCommand):
    help = "Loads an Excel spreadsheet of DATA Act/USAspending data names across the various systems into <>"

    s3_public_url = "https://files{}.usaspending.gov/docs/DATA+Transparency+Crosswalk.xlsx"

    def add_arguments(self, parser):
        parser.add_argument("-p", "--path", help="filepath to a local Excel spreadsheet to load", default=None)

    def handle(self, *args, **options):
        logger.info("Starting load_rosetta management command")

        script_start_time = perf_counter()
        rosetta_object = extract_data_from_source_file(path=options["path"])

        if options["path"] is None:
            prod_or_noprod = "-nonprod" if settings.DOWNLOAD_ENV != "production" else ""
            rosetta_object["metadata"]["download_location"] = self.s3_public_url.format(prod_or_noprod)
        load_xlsx_data_to_model(rosetta_object)

        logger.info("Script completed in {:.2f}s".format(perf_counter() - script_start_time))


def extract_data_from_source_file(path: str = None) -> dict:
    if path:
        logger.info("Using local file: {}".format(path))
        filepath = path
        download_path = None
    else:
        download_path = settings.ROSETTA_DICT_S3_PATH
        bucket_name = download_path.split("/")[0]
        filepath = "temp_data_act_crosswalk.xlsx"
        logger.info("Using S3 file: {} from {}".format(filepath, bucket_name))
        s3_client = boto3.client('s3', region_name=settings.USASPENDING_AWS_REGION)
        s3_client.download_file(bucket_name, "/".join(settings.ROSETTA_DICT_S3_PATH.split("/")[1:]), filepath)

    file_size = os.path.getsize(filepath)
    wb = load_workbook(filename=filepath)
    sheet = wb["Public"]
    last_column = get_column_letter(sheet.max_column)
    cell_range = "A2:{}2".format(last_column)
    headers = [{"column": cell.column, "header": cell.value} for cell in sheet[cell_range][0]]

    sections = []
    for header in headers:
        section = {"section": sheet["{}1".format(header["column"])].value, "colspan": 1}

        if section["section"] is None:
            sections[-1]["colspan"] += 1
        else:
            sections.append(section)

    elements = OrderedDict()
    for i, row in enumerate(sheet.values):
        if i < 2:
            continue
        elements[row[0]] = [r.strip() if r.strip() != "N/A" else None for r in row]

    field_names = list(header["header"] for header in headers)
    row_count = len(elements)
    assert list(DB_TO_XLSX_MAPPING.values()) == field_names, "Column headers don't match the headers in the model"
    logger.info("Columns in file: {} with {} rows of elements".format(field_names, row_count))

    if path is None:  # If path is None, it means the file was retrived from S3. Delete the temp file
        os.unlink(filepath)

    return {
        "metadata": {
            "total_rows": row_count,
            "total_columns": len(field_names),
            "download_location": download_path,
            "total_size": "{:.2f}KB".format(float(file_size) / 1024),
        },
        "headers": headers,
        "sections": sections,
        "data": elements,
    }


@transaction.atomic
def load_xlsx_data_to_model(rosetta_object: dict):
    Rosetta.objects.all().delete()
    json_doc = {
        "metadata": rosetta_object["metadata"],
        "sections": rosetta_object["sections"],
        "headers": list({"display": pretty, "raw": raw} for raw, pretty in DB_TO_XLSX_MAPPING.items()),
        "rows": list(row for row in rosetta_object["data"].values()),
    }
    # print(json.dumps(json_doc))
    rosetta = Rosetta(document_name="api_response", document=json_doc)
    rosetta.save()
