"""
! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !

This is pre-work for DEV-2752 and will be folded into mainline code as part of that ticket.

If this warning is still hanging around in the year 2020, it's probably safe to drop this
model as the original developer probably won the lottery or something and now owns an island
in the Pacific and can't be bothered with such nonsense.

! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE ! DO NOT USE !
"""
import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from usaspending_api.agencies.models import CGAC, FREC, RawAgencyCodesCSV, SubtierAgency, ToptierAgency
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("console")


class Command(BaseCommand):

    help = (
        "Loads CGACs, FRECs, Subtier Agencies, and Toptier Agencies.  Load is all or nothing -- if "
        "anything fails, nothing gets saved."
    )

    def add_arguments(self, parser):

        parser.add_argument(
            "agency_file",
            metavar="AGENCY_FILE",
            help=(
                "Path (for local files) or URI (for http(s) files) of the raw agency CSV file to "
                "be loaded.  As of this writing, only local and http(s) files have been tested.  "
                "S3 will likely require some enhancements."
            ),
        )

        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                "Force a reload even if no changes are detected in the raw agency file.  This is "
                "primarily useful in situations where downstream data sets need to be updated even "
                "though there are no changes in the raw agency file, for example when adding a new "
                "column or to undo some manual changes."
            ),
        )

    def handle(self, *args, **options):

        agency_file = options["agency_file"]
        force = options["force"]
        logger.info("AGENCY FILE: {}".format(agency_file))
        logger.info("FORCE SWITCH: {}".format(force))

        try:
            with Timer("Overall import"):
                with transaction.atomic():
                    self._perform_imports(agency_file, force)

        except Exception:
            logger.error("ALL CHANGES WERE ROLLED BACK DUE TO EXCEPTION")
            raise

    @staticmethod
    def _perform_imports(agency_file, force):

        with Timer("Import {}".format(agency_file)):
            raw_record_count = RawAgencyCodesCSV.objects.perform_import(agency_file, force)
            logger.info("{:,} raw agency records imported".format(raw_record_count))

        if force is True or raw_record_count > 0:

            with Timer("Import CGACs"):
                logger.info("{:,} CGACs imported".format(CGAC.objects.perform_import()))

            with Timer("Import FRECs"):
                logger.info("{:,} FRECs imported".format(FREC.objects.perform_import()))

            with Timer("Import Toptiers"):
                logger.info("{:,} Toptiers imported".format(ToptierAgency.objects.perform_import()))

            with Timer("Import Subtiers"):
                logger.info("{:,} Subtiers imported".format(SubtierAgency.objects.perform_import()))

        else:
            logger.info("No raw records have changed.   No imports were performed.")
