import json
import logging
from typing import Any, Dict, Iterable

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

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

        def _serialize_pscs(models: Iterable[PSC]) -> str:
            return json.dumps([{k: str(v) for k, v in model.__dict__.items()} for model in models], indent=2)

        self.logger.info(f"Added: {_serialize_pscs(result['added'].values())}")
        self.logger.info(f"Updated: {_serialize_pscs([v.get('new') for v in result['updated'].values()])}")
        self.logger.info(f"Deleted: {_serialize_pscs(result['deleted'].values())}")
        self.logger.info(
            f"Load PSC command added {len(result['added'])} PSCs, updated {len(result['updated'])} PSCs, "
            f"deleted {len(result['deleted'])} PSCs, and left {len(result['unchanged'])} PSCs unchanged."
        )


def _is_psc_in(psc: PSC, pscs: Iterable[PSC]) -> bool:
    comparison_fields = [
        "code",
        "start_date",
        "end_date",
        "full_name",
        "description",
        "includes",
        "excludes",
        "notes",
    ]
    return any(
        all(getattr(psc, field) == getattr(compare_psc, field) for field in comparison_fields) for compare_psc in pscs
    )


def compare_codes(old_pscs: Dict[str, PSC], new_pscs: Dict[str, PSC]) -> Dict[str, Dict[str, Any]]:
    added = {code: new_psc for code, new_psc in new_pscs.items() if code not in old_pscs}
    deleted = {code: old_psc for code, old_psc in old_pscs.items() if code not in new_pscs}
    updated_new = {
        code: new_psc
        for code, new_psc in new_pscs.items()
        if code not in added and not _is_psc_in(new_psc, old_pscs.values())
    }
    updated_old = {
        code: old_psc
        for code, old_psc in old_pscs.items()
        if code not in deleted and not _is_psc_in(old_psc, new_pscs.values())
    }
    updated = {
        code: {
            "old": updated_old.get(code),
            "new": new_psc,
        }
        for code, new_psc in updated_new.items()
    }
    unchanged = {code: new_psc for code, new_psc in new_pscs.items() if code not in added and code not in updated}
    return {
        "added": added,
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
    }


def load_psc(fullpath: str, sheet_name: str, update: bool) -> Dict[str, Any]:
    """
    Create/Update Product or Service Code records from an Excel doc of historical data.
    """
    logger = logging.getLogger("script")
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
            dtype={"PSC CODE": str},
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
        .astype(object)
        .where(lambda df: df.notna(), None)
        .to_dict(orient="records")
    )
    with transaction.atomic():
        old = {psc.code: psc for psc in PSC.objects.all()}
        PSC.objects.all().delete()
        created = PSC.objects.bulk_create(PSC(**new_psc) for new_psc in new_pscs)
        new = {psc.code: psc for psc in created}
        result = compare_codes(old, new)
    if update:
        update_lengths()
        logger.log(20, "Updated PSC codes.")
    return result


def update_lengths():
    unupdated_pscs = PSC.objects.filter(length=0)
    for psc in unupdated_pscs:
        length = len(psc.code)
        psc.length = length
        psc.save()
