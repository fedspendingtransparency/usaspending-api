import boto3
import logging

from django.core.management.base import BaseCommand
from django.conf import settings

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.models import ToptierAgency
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader, SkipRowException
from usaspending_api.references.reference_helpers import (
    insert_federal_accounts,
    update_federal_accounts,
    remove_empty_federal_accounts,
)


class Command(BaseCommand):
    help = "Loads tas and agencies info from CARS list in the folder of this management command."
    logger = logging.getLogger("console")

    def add_arguments(self, parser):
        parser.add_argument("location", nargs=1, help="The location of the file to load")

    def handle(self, *args, **options):
        is_remote_file = len(options["location"][0].split(".")) == 1
        if is_remote_file:
            s3connection = boto3.client("s3", region_name=settings.USASPENDING_AWS_REGION)
            file_path = s3connection.get_object(Bucket=options["location"][0], Key="cars_tas.csv")
        else:
            file_path = options["location"][0]

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
            "allocation_transfer_agency_id": lambda row: row["ATA"].strip(),
            "agency_id": lambda row: row["AID"].strip(),
            "beginning_period_of_availability": lambda row: row["BPOA"].strip(),
            "ending_period_of_availability": lambda row: row["EPOA"].strip(),
            "availability_type_code": lambda row: row["A"].strip(),
            "main_account_code": lambda row: row["MAIN"].strip(),
            "sub_account_code": lambda row: row["SUB"].strip(),
            "awarding_toptier_agency": lambda row: ToptierAgency.objects.filter(cgac_code=row["ATA"].strip())
            .order_by("fpds_code")
            .first(),
            "funding_toptier_agency": lambda row: ToptierAgency.objects.filter(cgac_code=row["AID"].strip())
            .order_by("fpds_code")
            .first(),
        }

        loader = ThreadedDataLoader(
            model_class=TreasuryAppropriationAccount,
            field_map=field_map,
            value_map=value_map,
            collision_field="treasury_account_identifier",
            collision_behavior="update",
            pre_row_function=self.skip_and_remove_financing_tas,
        )
        loader.load_from_file(filepath=file_path, remote_file=is_remote_file)

        # Match funding toptiers by FREC if they didn't match by AID
        unmapped_funding_agencies = TreasuryAppropriationAccount.objects.filter(funding_toptier_agency=None)
        match_count = 0
        self.logger.info(
            "Found {} unmatched funding agencies across all TAS objects. "
            "Attempting to match on FREC.".format(unmapped_funding_agencies.count())
        )
        for next_tas in unmapped_funding_agencies:
            # CGAC code is a combination of FRECs and CGACs. It will never be empty and it will always
            # be unique in ToptierAgencies; this should be safe to do.
            frec_match = ToptierAgency.objects.filter(cgac_code=next_tas.fr_entity_code).first()
            if frec_match:
                match_count += 1
                self.logger.info(
                    "Matched unknown funding agency for TAS {} with FREC {}".format(
                        next_tas.tas_rendering_label, next_tas.fr_entity_code
                    )
                )
                next_tas.funding_toptier_agency = frec_match
                next_tas.save()

        self.logger.info("Updated {} funding toptiers with a FREC agency.".format(match_count))

        # update TAS fk relationships to federal accounts
        remove_empty_federal_accounts()
        update_federal_accounts()
        insert_federal_accounts()

    def generate_tas_rendering_label(self, row):
        return TreasuryAppropriationAccount.generate_tas_rendering_label(
            row["ATA"], row["Agency AID"], row["A"], row["BPOA"], row["EPOA"], row["MAIN"], row["SUB"]
        )

    def skip_and_remove_financing_tas(self, row, instance):
        if row["financial_indicator_type2"] == "F":
            # If the instance already exists in the db, delete the instance.
            if instance.treasury_account_identifier:
                instance.delete()
            # If the row indicates the TAS is a financing account, skip that row by throwing an Exception.
            raise SkipRowException("Row contains Financing TAS, Skipping...")
