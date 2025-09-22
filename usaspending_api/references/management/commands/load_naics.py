import glob
import logging
import re

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db import models
from openpyxl import load_workbook

from usaspending_api.references.models import NAICS


class Command(BaseCommand):
    help = "Updates DB from Excel spreadsheets of USAspending terminology definitions into the naics model"

    logger = logging.getLogger("script")

    default_path = str(settings.APP_DIR / "data" / "naics_archive")

    def add_arguments(self, parser):
        parser.add_argument(
            "-p", "--path", help="the path to the Excel spreadsheets to load", default=self.default_path
        )
        parser.add_argument("-a", "--append", help="Append to existing guide", action="store_true")

    def handle(self, *args, **options):
        load_naics(path=options["path"], append=options["append"])


def populate_naics_fields(ws, naics_year, path):
    for current_row, row in enumerate(ws.rows):
        if not row[0].value:
            break  # Reads file only until a blank line

        naics_desc = row[1].value.strip()

        try:
            naics_code = int(row[0].value)
            load_single_naics(naics_code, naics_year, naics_desc)
        # Occasionally you will see more "creative" ways of listing naics. The following tries to account for common
        # patterns
        except ValueError:
            load_naics_range(row[0].value, naics_year, naics_desc, path)


def load_naics_range(naics_range_string, naics_year, naics_desc, path):
    if "-" in naics_range_string:
        try:
            minmax = naics_range_string.split("-")
            for naics_code in range(int(minmax[0].strip()), int(minmax[1].strip()) + 1):
                load_single_naics(naics_code, naics_year, naics_desc)
        except ValueError:
            raise CommandError(
                "Unparsable NAICS range value: {0}. Please review file {1}".format(naics_range_string, path)
            )
    else:
        raise CommandError("Unparsable NAICS range value: {0}. Please review file {1}".format(naics_range_string, path))


def load_single_naics(naics_code, naics_year, naics_desc):

    # crude way of ignoring naics of length 3 and 5
    if len(str(naics_code)) not in (2, 4, 6):
        return

    obj, created = NAICS.objects.get_or_create(pk=naics_code, defaults={"description": naics_desc, "year": naics_year})

    if not created:
        if int(naics_year) > int(obj.year):
            NAICS.objects.filter(pk=naics_code).update(description=naics_desc, year=naics_year)


def load_naics_year_retired():
    oldest_naics_year = NAICS.objects.aggregate(models.Max("year"))["year__max"]
    retired_naics = NAICS.objects.filter(year_retired=None, year__lt=oldest_naics_year)
    naics_years = list(NAICS.objects.all().values_list("year", flat=True).distinct().order_by("year"))

    for naics in retired_naics:
        previous_naics_year_index = naics_years.index(naics.year)
        naics.year_retired = naics_years[previous_naics_year_index + 1]

    NAICS.objects.bulk_update(retired_naics, ["year_retired"])


@transaction.atomic
def load_naics(path, append):
    logger = logging.getLogger("script")

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

        naics_year = p_year.search(path).group()
        populate_naics_fields(ws, naics_year, path)
    load_naics_year_retired()
