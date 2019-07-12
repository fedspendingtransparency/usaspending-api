import logging
import os.path
import re
import glob

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from openpyxl import load_workbook
from django.db import transaction

from usaspending_api.references.models import NAICS


class Command(BaseCommand):
    help = "Updates DB from Excel spreadsheets of USAspending terminology definitions into the naics model"

    logger = logging.getLogger("console")

    path = "usaspending_api/data/naics_archive"
    path = os.path.normpath(path)
    default_path = os.path.join(settings.BASE_DIR, path)

    def add_arguments(self, parser):
        parser.add_argument(
            "-p", "--path", help="the path to the Excel spreadsheets to load", default=self.default_path
        )
        parser.add_argument("-a", "--append", help="Append to existing guide", action="store_true")

    def handle(self, *args, **options):
        load_naics(path=options["path"], append=options["append"])


def populate_naics_fields(ws, naics_year, path):
    for current_row, row in enumerate(ws.rows):
        if current_row == 0:
            continue

        if not row[0].value:
            break  # Reads file only until a blank line

        try:
            naics_code = int(row[0].value)
        except ValueError:
            raise CommandError("Invalid NAICS code: {0}. Please review file {1}".format(naics_code, path))

        naics_desc = row[1].value.strip()

        obj, created = NAICS.objects.get_or_create(
            pk=naics_code, defaults={"description": naics_desc, "year": naics_year}
        )

        if not created:
            if int(naics_year) > int(obj.year):
                NAICS.objects.filter(pk=naics_code).update(description=naics_desc, year=naics_year)


@transaction.atomic
def load_naics(path, append):
    logger = logging.getLogger("console")

    if append:
        logger.info("Appending definitions to existing guide")
    else:
        logger.info("Deleting existing definitions from guide")
        NAICS.objects.all().delete()

    # year regex object precompile
    p_year = re.compile("(20[0-9]{2})")

    dir_files = glob.glob(path + "/*.xlsx")

    for path in sorted(dir_files, reverse=True):
        wb = load_workbook(filename=path)
        ws = wb.active

        naics_year = p_year.search(ws["A1"].value).group()
        populate_naics_fields(ws, naics_year, path)
