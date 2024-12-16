import logging
from urllib.error import HTTPError

import pandas as pd
from django.core.management.base import BaseCommand

from usaspending_api.references.models import PSC


class Command(BaseCommand):
    help = "Loads program information obtained from Excel file on https://www.acquisition.gov/PSC_Manual"

    logger = logging.getLogger("script")
    default_filepath = "https://www.acquisition.gov/sites/default/files/manual/PSC%20April%202024.xlsx"

    def add_arguments(self, parser):
        parser.add_argument(
            "-p", "--path", help="The path to the spreadsheets to load. Can be a url.", default=self.default_filepath
        )
        parser.add_argument(
            "-u", "--update", help="Updates the lengths of any codes that were not in the file.", action="store_true"
        )

    def handle(self, *args, **options):

        load_psc(fullpath=options["path"], update=options["update"])
        self.logger.log(20, "Loaded PSC codes successfully.")


def load_psc(fullpath, update):
    """
    Create/Update Product or Service Code records from an Excel doc of historical data.
    """
    try:
        logger = logging.getLogger("script")
        new_pscs = (
            pd.read_excel(
                "https://www.acquisition.gov/sites/default/files/manual/PSC%20April%202024.xlsx",
                sheet_name="PSC for 042022",
                usecols=[
                    "PSC CODE",
                    "START DATE",
                    "END DATE",
                    "PRODUCT AND SERVICE CODE NAME",
                    "PRODUCT AND SERVICE CODE FULL NAME (DESCRIPTION)",
                    "PRODUCT AND SERVICE CODE INCLUDES",
                    "PRODUCT AND SERVICE CODE EXCLUDES",
                    "PRODUCT AND SERVICE CODE NOTES",
                ],
                parse_dates=["START DATE", "END DATE"],
            )
            # Rename columns to match PSC model fields
            .rename(
                columns={
                    "PSC CODE": "code",
                    "START DATE": "start_date",
                    "END DATE": "end_date",
                    "PRODUCT AND SERVICE CODE NAME": "full_name",
                    "PRODUCT AND SERVICE CODE FULL NAME (DESCRIPTION)": "description",
                    "PRODUCT AND SERVICE CODE INCLUDES": "includes",
                    "PRODUCT AND SERVICE CODE EXCLUDES": "excludes",
                    "PRODUCT AND SERVICE CODE NOTES": "notes",
                }
            )
            # Remove duplicate codes keeping most recent start_date
            .loc[lambda df: (~df.sort_values(by=["code", "start_date"]).duplicated(subset=["code"], keep="last"))]
            # Add length column
            .assign(length=lambda df: df["code"].astype(str).str.len()).to_dict(orient="records")
        )
        for new_psc in new_pscs:
            psc, created = PSC.objects.get_or_create(code=new_psc["code"])
            psc.length = new_psc["length"]
            psc.description = new_psc["description"]
            check_start_end_dates(psc, new_psc["start_date"], new_psc["end_date"])
            psc.full_name = new_psc["full_name"]
            psc.excludes = new_psc["excludes"]
            psc.notes = new_psc["notes"]
            psc.includes = new_psc["includes"]
            psc.save()
        if update:
            update_lengths()
            logger.log(20, "Updated PSC codes.")
    except IOError:
        logger.error("Could not open file {}".format(fullpath))
    except HTTPError:
        logger.error("Could not open url {}".format(fullpath))
    except Exception as e:
        logger.error(e)


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
