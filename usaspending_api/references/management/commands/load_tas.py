import csv
import logging
import sys

from datetime import datetime

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import transaction

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.etl.operations.federal_account.update_agency import update_federal_account_agency
from usaspending_api.etl.operations.treasury_appropriation_account.update_agencies import (
    update_treasury_appropriation_account_agencies,
)
from usaspending_api.references.account_helpers import (
    insert_federal_accounts,
    link_treasury_accounts_to_federal_accounts,
    remove_empty_federal_accounts,
    update_federal_accounts,
)

logger = logging.getLogger("script")

TAS_SQL_PATH = "usaspending_api/references/management/sql/restock_tas.sql"


class Command(BaseCommand):
    """
    Used to load TAS either from Broker or a local tas_list file. There is a tas_list file inside
    of the project that can be found at: "usaspending_api/data/tas_list.csv"

    As of 10/30/19 the CSV functionality is used by the "load_reference_data" management
    command to load initial TAS data for local development.

    There are two ways to run the loader:
        - ./manage.py load_tas --location="usaspending_api/data/tas_list.csv"
        - ./manage.py load_tas
    The second option requires that a dblink is setup between USAspending and Broker databases.
    """

    help = "Update TAS records using either Data Broker or a TAS file if provided."

    def add_arguments(self, parser):
        parser.add_argument("-l", "--location", dest="location", help="(OPTIONAL) location of the TAS file to load")

    @transaction.atomic()
    def handle(self, *args, **options):
        try:
            with Timer("Loading TAS from {}".format(options["location"] or "Broker")):
                if options["location"]:
                    self.csv_tas_loader(options["location"])
                else:
                    call_command("run_sql", "-f", TAS_SQL_PATH)

            # Update TAS agency links.
            with Timer("Updating TAS agencies"):
                count = update_treasury_appropriation_account_agencies()
                logger.info(f"   Updated {count:,} TAS agency links")

            # Update Federal Accounts from TAS.
            with Timer("Updating Federal Accounts from TAS"):
                deletes = remove_empty_federal_accounts()
                logger.info(f"   Removed {deletes:,} Federal Account Rows")
                updates = update_federal_accounts()
                logger.info(f"   Updated {updates:,} Federal Account Rows")
                inserts = insert_federal_accounts()
                logger.info(f"   Created {inserts:,} Federal Account Rows")
                links = link_treasury_accounts_to_federal_accounts()
                logger.info(f"   Linked {links:,} Treasury Accounts to Federal Accounts")
                agencies = update_federal_account_agency()
                logger.info(f"   Updated {agencies:,} Federal Account agency links")

            logger.info("=== TAS loader finished successfully! ===")

        except Exception as e:
            logger.error(e)
            logger.error("=== TAS loader failed ===")
            sys.exit(1)

    def csv_tas_loader(self, file_path):
        field_map = {
            "treasury_account_identifier": "ACCT_NUM",
            "allocation_transfer_agency_id": "ATA",
            "agency_id": "AID",
            "beginning_period_of_availability": "BPOA",
            "ending_period_of_availability": "EPOA",
            "availability_type_code": "A",
            "main_account_code": "MAIN",
            "sub_account_code": "SUB",
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
            "awarding_toptier_agency": None,
            "funding_toptier_agency": None,
            "internal_start_date": lambda row: datetime.strftime(
                datetime.strptime(row["DT_TM_ESTAB"], "%m/%d/%Y  %H:%M:%S"), "%Y-%m-%d"
            ),
            "internal_end_date": lambda row: (
                datetime.strftime(datetime.strptime(row["DT_END"], "%m/%d/%Y  %H:%M:%S"), "%Y-%m-%d")
                if row["DT_END"]
                else None
            ),
        }

        with RetrieveFileFromUri(file_path).get_file_object(True) as tas_list_file_object:
            # Get a total count for print out
            tas_list_reader = csv.DictReader(tas_list_file_object)
            total_count = len(list(tas_list_reader))

            # Reset the reader back to the beginning of the file
            tas_list_file_object.seek(0)
            tas_list_reader = csv.DictReader(tas_list_file_object)

            for count, row in enumerate(tas_list_reader, 1):
                for key, value in row.items():
                    row[key] = value.strip() or None

                # Check to see if we need to update or create a TreasuryAppropriationAccount record
                current_record = TreasuryAppropriationAccount.objects.filter(
                    treasury_account_identifier=row["ACCT_NUM"]
                ).first()
                taa_instance = current_record or TreasuryAppropriationAccount()

                # Don't load Financing TAS
                if row["financial_indicator_type2"] == "F":
                    if taa_instance.treasury_account_identifier:
                        taa_instance.delete()
                    logger.info("   Row contains Financing TAS, Skipping...")
                    continue

                load_data_into_model(taa_instance, row, field_map=field_map, value_map=value_map, save=True)

                if count % 1000 == 0:
                    logger.info("   Loaded {} rows of {}".format(count, total_count))

    def generate_tas_rendering_label(self, row):
        return TreasuryAppropriationAccount.generate_tas_rendering_label(
            row["ATA"], row["Agency AID"], row["A"], row["BPOA"], row["EPOA"], row["MAIN"], row["SUB"]
        )
