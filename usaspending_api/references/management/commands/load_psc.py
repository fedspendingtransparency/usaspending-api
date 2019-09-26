from django.core.management.base import BaseCommand
from usaspending_api.references.models import PSC
import os
import logging
from openpyxl import load_workbook

class Command(BaseCommand):
    help = "Loads program information obtained from Excel file on https://www.acquisition.gov/PSC_Manual"

    logger = logging.getLogger("console")

    def handle(self, *args, **options):
        default_directory = os.path.normpath("usaspending_api/references/management/commands/")
        default_filepath = os.path.join(default_directory, "PSC_Data_June_2019_Edition_FINAL_6-20-19+DRW.xlsx")

        load_psc(default_filepath)
        self.logger.log(20, "Loaded PSC codes successfully.")


def load_psc(fullpath):
    """
    Create CFDA Program records from a Excel doc of historical data.
    """
    try:
        logger = logging.getLogger("console")

        wb = load_workbook(filename=fullpath, data_only=True)
        ws = wb.active

        for current_row, row in enumerate(ws.rows):
            if not row[0].value or not row[2].value or row[0].value == "PSC CODE":
                continue  # skip lines without codes
            psc_code = str(row[0].value)
            psc_length = len(psc_code)
            psc_description = row[2].value.strip()
            psc_start_date = row[3].value
            psc_end_date = row[4].value
            psc_full_name = row[5].value
            psc_excludes = row[6].value
            psc_notes = row[7].value
            psc_includes = row[8].value
            if psc_end_date:
                try: # if we have an end date we are either 1) updated a code that already has an end date (unlikely)
                    psc = PSC.objects.get(code=psc_code, start_date=psc_start_date,
                                          end_date=psc_end_date)
                except PSC.DoesNotExist:
                    try: #or still unlikely, updating a code that has a start date but no end date
                        psc = PSC.objects.get(code=psc_code, start_date=psc_start_date, end_date__isnull=True)
                    except PSC.DoesNotExist: #but most likely we are adding an already expired code
                        psc, created = PSC.objects.get_or_create(code=psc_code, end_date=psc_end_date)
            elif psc_start_date:
                try: #if we have only a start date we don't want to overwrite data that has a different start date
                    psc = PSC.objects.get(code=psc_code, start_date=psc_start_date)
                except PSC.DoesNotExist:
                    try: # but we do want to overwrite data that has blank dates
                        psc = PSC.objects.get(code=psc_code, start_date__isnull=True, end_date__isnull=True)
                    except PSC.DoesNotExist:
                        psc, created = PSC.objects.get_or_create(code=psc_code)
            else:
                psc, created = PSC.objects.get_or_create(code=psc_code)

            psc.description = psc_description
            psc.length = psc_length
            psc.start_date = psc_start_date
            psc.end_date = psc_end_date
            psc.full_name = psc_full_name
            psc.excludes = psc_excludes
            psc.notes = psc_notes
            psc.includes = psc_includes
            psc.save()

    except IOError:
        logger.error("Could not open file {}".format(fullpath))
