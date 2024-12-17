import datetime
import logging
from typing import Optional, Union, List, Tuple
from urllib.error import HTTPError

import pandas as pd
from django.core.management.base import BaseCommand

from usaspending_api.references.models import PSC


class Command(BaseCommand):
    help = "Loads program information obtained from Excel file on https://www.acquisition.gov/PSC_Manual"

    logger = logging.getLogger("script")
    default_filepath = "https://www.acquisition.gov/sites/default/files/manual/PSC%20April%202024.xlsx"
    default_sheet_name = "PSC for 042022"

    def add_arguments(self, parser):
        parser.add_argument(
            "-p", "--path", help="The path to the spreadsheets to load. Can be a url.", default=self.default_filepath
        )
        parser.add_argument(
            "-s",
            "--sheet_name",
            help="The name of the worksheet in the spreadsheet file.",
            default=self.default_sheet_name,
        )
        parser.add_argument(
            "-u", "--update", help="Updates the lengths of any codes that were not in the file.", action="store_true"
        )

    def handle(self, *args, **options):
        num_created, num_updated, errors = load_psc(
            fullpath=options["path"],
            sheet_name=options["sheet_name"],
            update=options["update"],
        )
        self.logger.log(
            20,
            f"Load PSC command created {num_created} PSCs and updated {num_updated} PSCs with {len(errors)} errors.",
        )


def load_psc(fullpath: str, sheet_name: str, update: bool) -> Tuple[int, int, List]:
    """
    Create/Update Product or Service Code records from an Excel doc of historical data.
    """
    errors = []
    try:
        new_pscs = (
            pd.read_excel(
                fullpath,
                sheet_name=sheet_name,
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
                    "PRODUCT AND SERVICE CODE FULL NAME (DESCRIPTION)": "full_name",
                    "PRODUCT AND SERVICE CODE NAME": "description",
                    "PRODUCT AND SERVICE CODE INCLUDES": "includes",
                    "PRODUCT AND SERVICE CODE EXCLUDES": "excludes",
                    "PRODUCT AND SERVICE CODE NOTES": "notes",
                }
            )
            .loc[
                lambda df: (
                    # Remove duplicate codes keeping most recent start_date
                    ~df.sort_values(by=["code", "start_date"]).duplicated(subset=["code"], keep="last")
                    # Remove rows with missing descriptions
                    & df["description"].notnull()
                )
            ]
            # Add length column
            .assign(length=lambda df: df["code"].astype(str).str.len())
            # Replace missing values with None
            .where(lambda df: df.notnull(), None)
            .to_dict(orient="records")
        )
        logger = logging.getLogger("script")
        num_created = 0
        num_updated = 0
        for new_psc in new_pscs:
            psc, created = PSC.objects.get_or_create(code=new_psc["code"])
            if created:
                num_created += 1
            else:
                num_updated += 1
            psc.length = new_psc["length"]
            psc.description = new_psc["description"]
            psc.start_date = check_dates(psc.start_date, new_psc["start_date"])
            psc.end_date = check_dates(psc.end_date, new_psc["end_date"])
            psc.full_name = new_psc["full_name"]
            psc.excludes = new_psc["excludes"]
            psc.notes = new_psc["notes"]
            psc.includes = new_psc["includes"]
            psc.save()
        if update:
            update_lengths()
            logger.log(20, "Updated PSC codes.")
    except IOError as e:
        logger.error("Could not open file {}".format(fullpath))
        errors.append(e)
    except HTTPError as e:
        logger.error("Could not open url {}".format(fullpath))
        errors.append(e)
    except Exception as e:
        logger.error(e)
        errors.append(e)
    return num_created, num_updated, errors


def check_dates(old_date: Union[datetime.date, None], new_date: pd.Timestamp) -> Optional[datetime.date]:
    if new_date is not pd.NaT and (old_date is None or old_date < new_date.date()):
        return new_date
    else:
        return old_date


def update_lengths():
    unupdated_pscs = PSC.objects.filter(length=0)
    for psc in unupdated_pscs:
        length = len(psc.code)
        psc.length = length
        psc.save()
