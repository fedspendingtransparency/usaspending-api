from django.core.management.base import BaseCommand, CommandError
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.threaded_data_loader import ThreadedDataLoader
from datetime import datetime
import os
import csv
import logging
import django


class Command(BaseCommand):
    help = "Loads tas and agencies info from CARS list in \
            the folder of this management command."
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('file', nargs=1, help='the file to load')

    def handle(self, *args, **options):
        field_map = {
            "treasury_account_identifier": "ACCT_NUM",
            "allocation_transfer_agency_id": "ATA",
            "agency_id": "AID",
            "beginning_period_of_availability": "BPOA",
            "ending_period_of_availability": "EPOA",
            "availability_type_code": "A",
            "main_account_code": "MAIN",
            "sub_account_code": "SUB",
            #"gwa_tas": "GWA_TAS",
            "account_title": "GWA_TAS NAME",
            "reporting_agency_id": "Agency AID",
            "reporting_agency_name": "Agency Name",
            "budget_bureau_code": "ADMIN_ORG",
            "budget_bureau_name": "Admin Org Name",
            "fr_entity_code": "FR Entity Type Code",
            "fr_entity_description": "FR Entity Description",
            #"fin_type_2": "Financial Indicator Type 2",
            #"fin_type_2_description": "FIN IND Type 2 Description",
            "budget_function_code": "Function Code",
            "budget_function_title": "Function Description",
            "budget_subfunction_code": "Sub Function Code",
            "budget_subfunction_title": "Sub Function Description"
        }

        value_map = {
            "data_source": "USA"
        }

        loader = ThreadedDataLoader(model_class=TreasuryAppropriationAccount, field_map=field_map, value_map=value_map, collision_field='treasury_account_identifier', collision_behavior='update')
        loader.load_from_file(options['file'][0])
# self.logger.log(20, "loaded account number %s %s %s ", treasury_appropriation_account.treasury_account_identifier, treasury_appropriation_account, treasury_appropriation_account.gwa_tas_name)
