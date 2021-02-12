from django.core.management.base import BaseCommand
from usaspending_api.references.models import PSC
import os
import logging
from openpyxl import load_workbook


class Command(BaseCommand):
    help = "Loads program information obtained from Excel file on https://www.acquisition.gov/PSC_Manual"

    logger = logging.getLogger("script")
    default_directory = os.path.normpath("usaspending_api/references/management/commands/")
    default_filepath = os.path.join(default_directory, "PSC_Data_June_2019_Edition_FINAL_6-20-19+DRW.xlsx")

    def add_arguments(self, parser):
        parser.add_argument("-p", "--path", help="the path to the spreadsheets to load", default=self.default_filepath)
        parser.add_argument(
            "-u", "--update", help="Updates the lengths of any codes that were not in the file.", action="store_true"
        )

    def handle(self, *args, **options):

        load_psc(fullpath=options["path"], update=options["update"])
        self.logger.log(20, "Loaded PSC codes successfully.")


def load_psc(fullpath, update):
    """
    Create/Update Product or Service Code records from a Excel doc of historical data.
    """
    try:
        logger = logging.getLogger("script")
        wb = load_workbook(filename=fullpath, data_only=True)
        ws = wb.active
        for current_row, row in enumerate(ws.rows):
            if not row[0].value or row[0].value == "PSC CODE" or ws.row_dimensions[row[0].row].hidden:
                continue  # skip lines without codes and hidden rows
            psc_code = row[0].value
            psc_length = row[1].value
            psc_description = row[2].value

            psc_start_date = row[3].value
            psc_end_date = row[4].value
            psc_full_name = row[5].value
            psc_excludes = row[6].value
            psc_notes = row[7].value
            psc_includes = row[8].value

            psc, created = PSC.objects.get_or_create(code=psc_code)
            psc.description = psc_description
            psc.length = psc_length
            check_start_end_dates(psc, psc_start_date, psc_end_date)
            psc.full_name = psc_full_name
            psc.excludes = psc_excludes
            psc.notes = psc_notes
            psc.includes = psc_includes
            psc.save()
        if update:
            update_lengths()
            logger.log(20, "Updated PSC codes.")
    except IOError:
        logger.error("Could not open file {}".format(fullpath))


def check_start_end_dates(psc, start_date, end_date):
    if psc.start_date:
        if start_date:
            if psc.start_date < start_date.date():
                psc.start_date = start_date
    else:
        psc.start_date = start_date
    if psc.end_date:
        if end_date:
            if psc.end_date < end_date.date():
                psc.end_date = end_date

    else:
        psc.end_date = end_date


def update_lengths():
    unupdated_pscs = PSC.objects.filter(length=0)
    for psc in unupdated_pscs:
        length = len(psc.code)
        psc.length = length
        psc.save()
