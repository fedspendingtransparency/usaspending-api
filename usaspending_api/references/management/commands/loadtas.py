import logging

from django.core.management.base import BaseCommand

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from usaspending_api.references.reference_helpers import insert_federal_accounts, update_federal_accounts


class Command(BaseCommand):
    help = "Loads tas and agencies info from CARS list in \
            the folder of this management command."
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):
        field_map = {
            "treasury_account_identifier": "ACCT_NUM",
            "account_title": "GWA_TAS NAME",
            "reporting_agency_id": "Agency AID",
            "reporting_agency_name": "Agency Name",
            "budget_bureau_code": "ADMIN_ORG",
            "budget_bureau_name": "Admin Org Name",
            "fr_entity_code": "FR Entity Type Code",
            "fr_entity_description": "FR Entity Description",
            "budget_function_code": "Function Code",
            "budget_function_title": "Function Description",
            "budget_subfunction_code": "Sub Function Code",
            "budget_subfunction_title": "Sub Function Description"
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
            "sub_account_code": lambda row: row["SUB"].strip()
        }

        loader = ThreadedDataLoader(model_class=TreasuryAppropriationAccount, field_map=field_map, value_map=value_map, collision_field='treasury_account_identifier', collision_behavior='update')
        loader.load_from_file(options['file'][0])

        # update TAS fk relationships to federal accounts
        update_federal_accounts()
        insert_federal_accounts()

    def generate_tas_rendering_label(self, row):
        return TreasuryAppropriationAccount.generate_tas_rendering_label(row["ATA"],
                                                                         row["Agency AID"],
                                                                         row["A"],
                                                                         row["BPOA"],
                                                                         row["EPOA"],
                                                                         row["MAIN"],
                                                                         row["SUB"])
