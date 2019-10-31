import csv
import logging
import sys

from datetime import datetime

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.references.models import ToptierAgency
from usaspending_api.references.reference_helpers import (
    insert_federal_accounts,
    update_federal_accounts,
    remove_empty_federal_accounts,
)
from usaspending_api.common.helpers.timing_helpers import Timer

logger = logging.getLogger("console")

TAS_SQL_PATH = "usaspending_api/references/management/sql/restock_tas.sql"


class Command(BaseCommand):
    """
    Used to load TAS either from Broker or a local tas_list file. There is a tas_list file inside
    of the project that can be found at: "/usaspending_api/data/tas_list.csv"

    As of 10/30/19 the CSV functionality is used by the "load_reference_data" management
    command to load initial TAS data for local development.

    There are two ways to run the loader:
        - ./manage.py load_tas --location="/usaspending_api/data/tas_list.csv"
        - ./manage.py load_tas
    The second option requires that a dblink is setup between USAspending and Broker databases.
    """

    help = "Update TAS records using either DATA Broker or a local CARS file if provided."
    logger = logging.getLogger("console")

    def add_arguments(self, parser):
        parser.add_argument("-l", "--location", dest="location", help="(OPTIONAL) location of the CARS file to load")

    @transaction.atomic()
    def handle(self, *args, **options):
        try:
            with Timer("Loading TAS from {}".format(options["location"] or "Broker")):
                if options["location"]:
                    self.csv_tas_loader(options["location"])
                else:
                    call_command("run_sql", "-f", TAS_SQL_PATH)

            # Match funding toptiers by FREC if they didn't match by AID
            with Timer("Matching Funding Toptier Agency where AID didn't match"):
                unmapped_funding_agencies = TreasuryAppropriationAccount.objects.filter(funding_toptier_agency=None)
                match_count = 0
                msg_str = "=== Found {} unmatched funding agencies across all TAS objects. ==="
                logger.info(msg_str.format(unmapped_funding_agencies.count()))
                for next_tas in unmapped_funding_agencies:
                    frec_match = ToptierAgency.objects.filter(toptier_code=next_tas.fr_entity_code).first()
                    if frec_match:
                        match_count += 1
                        logger.info(
                            "   Matched unknown funding agency for TAS {} with FREC {}".format(
                                next_tas.tas_rendering_label, next_tas.fr_entity_code
                            )
                        )
                        next_tas.funding_toptier_agency = frec_match
                        next_tas.save()

                logger.info("=== Updated {} Funding Toptier Agencies with a FREC agency. ===".format(match_count))

            # update TAS fk relationships to federal accounts
            with Timer("Updated TAS FK relationships to Federal Accounts"):
                logger.info("=== Updating TAS FK relationships to Federal Accounts ===")
                updates = update_federal_accounts()
                logger.info("   Updated {} Federal Account Rows".format(updates))
                inserts = insert_federal_accounts()
                logger.info("   Created {} Federal Account Rows".format(inserts))
                deletes = remove_empty_federal_accounts()
                logger.info("   Removed {} Federal Account Rows".format(deletes))

            logger.info("=== TAS loader finished successfully! ===")

        except Exception as e:
            logger.error(e)
            logger.error("=== TAS loader failed ===")
            sys.exit(1)

    def csv_tas_loader(self, file_path):
        field_map = {
            "treasury_account_identifier": "ACCT_NUM",
            "account_title": "GWA_TAS_NAME",
            "reporting_agency_id": "Agency AID",
            "reporting_agency_name": "Agency Name",
            "budget_bureau_code": "ADMIN_ORG",
            "budget_bureau_name": "Admin Org Name",
            "fr_entity_code": "FR Entity Type",
            "fr_entity_description": "FR Entity Description",
            "budget_function_code": "Function Code",
            "budget_function_title": "Function Description",
            "budget_subfunction_code": "Sub Function Code",
            "budget_subfunction_title": "Sub Function Description",
        }

        value_map = {
            "data_source": "USA",
            "tas_rendering_label": self.generate_tas_rendering_label,
            "awarding_toptier_agency": lambda row: ToptierAgency.objects.filter(
                toptier_code=row["ATA"].strip()
            ).first(),
            "funding_toptier_agency": lambda row: ToptierAgency.objects.filter(toptier_code=row["AID"].strip()).first(),
            "internal_start_date": lambda row: datetime.strftime(
                datetime.strptime(row["DT_TM_ESTAB"], "%m/%d/%Y  %H:%M:%S"), "%Y-%m-%d"
            ),
            "internal_end_date": lambda row: datetime.strftime(
                datetime.strptime(row["DT_END"], "%m/%d/%Y  %H:%M:%S"), "%Y-%m-%d"
            )
            if row["DT_END"]
            else None,
        }

        with RetrieveFileFromUri(file_path).get_file_object(True) as tas_list_file_object:
            # Get a total count for print out
            tas_list_reader = csv.DictReader(tas_list_file_object)
            total_count = len(list(tas_list_reader)) + 1

            # Reset the reader back to the beginning of the file
            tas_list_file_object.seek(0)
            tas_list_reader = csv.DictReader(tas_list_file_object)

            for count, row in enumerate(tas_list_reader, 1):
                for key, value in row.items():
                    row[key] = value.strip() if value else None

                # Check to see if we need to update or create a TreasuryAppropriationAccount record
                current_record = TreasuryAppropriationAccount.objects.filter(
                    treasury_account_identifier=row["ACCT_NUM"]
                ).first()
                taa_instance = current_record or TreasuryAppropriationAccount()
                load_data_into_model(taa_instance, row, field_map=field_map, value_map=value_map, save=True)

                if count % 1000 == 0:
                    logger.info("   Loaded {} rows of {}".format(count, total_count))

    def generate_tas_rendering_label(self, row):
        return TreasuryAppropriationAccount.generate_tas_rendering_label(
            row["ATA"], row["Agency AID"], row["A"], row["BPOA"], row["EPOA"], row["MAIN"], row["SUB"]
        )
