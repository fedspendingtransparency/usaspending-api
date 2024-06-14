import csv
import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.text import slugify

from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.references.models.bureau_title_lookup import BureauTitleLookup

logger = logging.getLogger("script")


class Command(BaseCommand):

    help = (
        "Loads a CSV from OMB and uses it to create a lookup table (bureau_title_lookup) from "
        "federal accounts to bureau titles"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-p",
            "--path",
            help="the path to the csv spreadsheet to load",
            default="https://max.omb.gov/maxportal/assets/public/treasury/FMS_GWA_EXPORT_APPN.csv",
        )

    @transaction.atomic
    def handle(self, *args, **options):
        path = options["path"]
        logger.info("Downloading Bureau Title Lookups")
        logger.info(f"Source: {path}")
        bureau_title_lookups = self.download_and_read_csv(path)

        logger.info("Deleting all existing Bureau Title Lookup records in website")
        deletes = BureauTitleLookup.objects.all().delete()
        logger.info(f"Deleted {deletes[0]:,} records")

        logger.info("Loading new Bureau Title Lookup records into database")
        lookup_count = len(BureauTitleLookup.objects.bulk_create(bureau_title_lookups))
        logger.info(f"Loaded: {lookup_count} unique Bureau Title Lookup records")

    def download_and_read_csv(self, path):
        unique_map = {}

        with RetrieveFileFromUri(path).get_file_object(text=True) as f:
            reader = csv.reader(f)
            for row in reader:

                # Removes extra white spaces and pad with zeroes
                aid = row[0].strip().zfill(3)
                main_acct = row[1].strip().zfill(4)
                bureau_title = row[23].strip()
                federal_account_code = f"{aid}-{main_acct}"
                type = row[44].strip().upper()

                # Rows with a type of "DUMMY" or an Agency id of "999" represent
                # test bureaus that should not be ingested.
                if type == "DUMMY" or aid == "999":
                    continue

                bureau_title_lookup = BureauTitleLookup(
                    **{
                        "federal_account_code": federal_account_code,
                        "bureau_title": bureau_title,
                        "bureau_slug": slugify(bureau_title),
                    }
                )

                # Use unique string and map to remove duplicates
                unique_string = f"{federal_account_code}|{bureau_title}"
                if unique_string not in unique_map:
                    unique_map[unique_string] = bureau_title_lookup

        return list(unique_map.values())
