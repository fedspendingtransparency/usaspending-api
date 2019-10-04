from django.core.management.base import BaseCommand
from usaspending_api.references.models import PSC
import os
import logging
import csv
from datetime import datetime

class Command(BaseCommand):
    help = "Loads program information obtained from Excel file on https://www.acquisition.gov/PSC_Manual"

    logger = logging.getLogger("console")
    default_directory = os.path.normpath("usaspending_api/references/management/commands/")
    default_filepath = os.path.join(default_directory, "filtered_csv_data.csv")

    def add_arguments(self, parser):
        parser.add_argument(
            "-p", "--path", help="the path to the spreadsheets to load", default=self.default_filepath
        )
        parser.add_argument("-u", "--update", help="Updates the lengths of any codes that were not in the file.",
                            action="store_true")

    def handle(self, *args, **options):

        load_psc(fullpath=options["path"], update=options["update"])
        self.logger.log(20, "Loaded PSC codes successfully.")



def load_psc(fullpath, update):
    """
    Create/Update Product or Service Code records from a Excel doc of historical data.
    """
    try:
        logger = logging.getLogger("console")
        with open(fullpath, errors="backslashreplace", encoding="utf-8-sig") as csvfile:

            reader = csv.DictReader(csvfile, delimiter=",", quotechar='"', skipinitialspace="true")

            for row in reader:
                psc_code = row["PSC CODE"]
                psc_description = row["PRODUCT AND SERVICE CODE NAME"]
                psc_length = len(psc_code)
                psc_start_date = row["START DATE"]
                psc_end_date = row["END DATE"]
                psc_full_name = row["PRODUCT AND SERVICE CODE FULL NAME"]
                psc_excludes = row["PRODUCT AND SERVICE CODE EXCLUDES"]
                psc_notes = row["PRODUCT AND SERVICE CODE NOTES"]
                psc_includes = row["PRODUCT AND SERVICE CODE INCLUDES"]

                psc, created = PSC.objects.get_or_create(code=psc_code)
                psc.description = psc_description
                psc.length = psc_length
                if psc_start_date:
                    if psc.start_date and datetime.strptime(psc_start_date, '%Y-%m-%d').date() >= psc.start_date:
                        psc.start_date = psc_start_date
                if psc_end_date:
                    if psc.end_date and datetime.strptime(psc_end_date, '%Y-%m-%d').date() >= psc.end_date:
                        psc.end_date = psc_end_date
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


def update_lengths():
    unupdated_pscs = PSC.objects.filter(length=0)
    for psc in unupdated_pscs:
        length = len(psc.code)
        psc.length = length
        psc.save()
