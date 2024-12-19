import logging
from typing import Any, Dict, Set, Union
from urllib.error import HTTPError

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models.query import QuerySet, ValuesListIterable

from usaspending_api.references.models import PSC


class Command(BaseCommand):
    help = "Loads program information obtained from Excel file on https://www.acquisition.gov/PSC_Manual"

    logger = logging.getLogger("script")
    default_filepath = f"{settings.FILES_SERVER_BASE_URL}/docs/PSC%20April%202024.xlsx"
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
        result = load_psc(
            fullpath=options["path"],
            sheet_name=options["sheet_name"],
            update=options["update"],
        )
        if result.get("error"):
            self.logger.error(result["error"])
        else:
            self.logger.info(result)
            self.logger.log(
                20,
                (
                    f"Load PSC command added {len(result['added'])} PSCs, updated {len(result['updated'])} PSCs, "
                    f"deleted {len(result['deleted'])} PSCs, and left {len(result['unchanged'])} PSCs unchanged."
                ),
            )


def compare_codes(old_pscs: Set[tuple], new_pscs: Set[tuple]) -> Dict[str, Any]:
    old_codes = set(old_psc[0] for old_psc in old_pscs)
    new_codes = set(new_psc[0] for new_psc in new_pscs)
    added_codes = new_codes - old_codes
    added = set(new_psc for new_psc in new_pscs if new_psc[0] in added_codes)
    deleted_codes = old_codes - new_codes
    deleted = set(old_psc for old_psc in old_pscs if old_psc[0] in deleted_codes)
    updated_new = set(new_psc for new_psc in new_pscs if new_psc not in added and new_psc not in old_pscs)
    updated_old = set(old_psc for old_psc in old_pscs if old_psc not in deleted and old_psc not in new_pscs)
    updated = [
        {
            "old": [old_psc for old_psc in updated_old if old_psc[0] == new_psc[0]][0],
            "new": new_psc,
        }
        for new_psc in updated_new
    ]
    unchanged = new_pscs - added - set(update["new"] for update in updated)
    return {
        "added": added,
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
    }


def load_psc(fullpath: str, sheet_name: str, update: bool) -> Dict[str, Union[ValuesListIterable, str]]:
    """
    Create/Update Product or Service Code records from an Excel doc of historical data.
    """
    logger = logging.getLogger("script")
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
            # Convert start date and end date to dates
            .assign(start_date=lambda df: df["start_date"].dt.date, end_date=lambda df: df["end_date"].dt.date)
            # Add length column
            .assign(length=lambda df: df["code"].astype(str).str.len())
            # Replace missing values with None
            .where(lambda df: df.notna(), None)
            .to_dict(orient="records")
        )
        with transaction.atomic():
            old = set(PSC.objects.all().values_list())
            PSC.objects.all().delete()
            PSC.objects.bulk_create(PSC(**new_psc) for new_psc in new_pscs)
            new = set(PSC.objects.all().values_list())
        result = compare_codes(old, new)
        if update:
            update_lengths()
            logger.log(20, "Updated PSC codes.")
    except IOError as e:
        result = {"error": e}
    except HTTPError as e:
        result = {"error": e}
    except Exception as e:
        result = {"error": e}
    return result


def update_lengths():
    unupdated_pscs = PSC.objects.filter(length=0)
    for psc in unupdated_pscs:
        length = len(psc.code)
        psc.length = length
        psc.save()
