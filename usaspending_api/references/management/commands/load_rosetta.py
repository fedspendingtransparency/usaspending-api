import logging

from collections import OrderedDict
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from pathlib import Path
from time import perf_counter

from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
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

    s3_public_url = settings.DATA_DICTIONARY_DOWNLOAD_URL

    def add_arguments(self, parser):
        parser.add_argument("-p", "--path", help="filepath to an Excel spreadsheet to load", default=self.s3_public_url)

    def handle(self, *args, **options):
        script_start_time = perf_counter()
        logger.info("Starting load_rosetta management command")
        logger.info("Loading data from {}".format(options["path"]))

        try:
            local_filepath = Path("temp_local_data_dictionary.xlsx")
            RetrieveFileFromUri(options["path"]).copy(str(local_filepath))
            rosetta_object = extract_data_from_source_file(filepath=local_filepath)
        except Exception:
            logger.exception("Exception during file retrieval or parsing")
        finally:
            if local_filepath.exists():
                local_filepath.unlink()

        rosetta_object["metadata"]["download_location"] = options["path"]

        load_xlsx_data_to_model(rosetta_object)

        logger.info("Script completed in {:.2f}s".format(perf_counter() - script_start_time))


def extract_data_from_source_file(filepath: str) -> dict:
    file_size = filepath.stat().st_size
    wb = load_workbook(filename=str(filepath))
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
    if list(DB_TO_XLSX_MAPPING.values()) != field_names:
        raise Exception("Column headers don't match the headers in the model")
    logger.info("Columns in file: {} with {} rows of elements".format(field_names, row_count))

    return {
        "metadata": {
            "total_rows": row_count,
            "total_columns": len(field_names),
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

    rosetta = Rosetta(document_name="api_response", document=json_doc)
    rosetta.save()
