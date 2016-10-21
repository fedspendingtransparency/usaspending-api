from django.core.management.base import BaseCommand, CommandError
from usaspending_api.accounts.models import TreasuryAppropriationAccount
import os
import csv
import logging
import django


class Command(BaseCommand):

    help = "Loads tas and agencies info from CARS list in \
            the folder of this management command."

    logger = logging.getLogger('console')

    def handle(self, *args, **options):

        try:
            with open(os.path.join(django.conf.settings.BASE_DIR,
                      'usaspending_api/references/management/commands/tas_list.csv')) \
                      as csvfile:

                reader = csv.DictReader(csvfile)
                for row in reader:
                    treasury_appropriation_account, created = TreasuryAppropriationAccount.objects.get_or_create(
                                    treasury_account_identifier=row['ACCT_NUM'])

                    treasury_appropriation_account.allocation_transfer_agency_id = row['ATA']
                    treasury_appropriation_account.agency_id = row['AID']
                    treasury_appropriation_account.beginning_period_of_availability = row['BPOA']
                    treasury_appropriation_account.ending_period_of_availability = row['EPOA']
                    treasury_appropriation_account.availability_type_code = row['A']
                    treasury_appropriation_account.main_account_code = row['MAIN']
                    treasury_appropriation_account.sub_account_code = row['SUB']
                    treasury_appropriation_account.gwa_tas = row['GWA_TAS']
                    treasury_appropriation_account.gwa_tas_name = row['GWA_TAS NAME']
                    treasury_appropriation_account.agency_aid = row['Agency AID']
                    treasury_appropriation_account.agency_name = row['Agency Name']
                    treasury_appropriation_account.admin_org = row['ADMIN_ORG']
                    treasury_appropriation_account.admin_org_name = row['Admin Org Name']
                    treasury_appropriation_account.fr_entity_type_code = row['FR Entity Type Code']
                    treasury_appropriation_account.fr_entity_description = row['FR Entity Description']
                    treasury_appropriation_account.fin_type_2 = row['Financial Indicator Type 2']
                    treasury_appropriation_account.fin_type_2_description = row['FIN IND Type 2 Description']
                    treasury_appropriation_account.function_code = row['Function Code']
                    treasury_appropriation_account.function_description = row['Function Description']
                    treasury_appropriation_account.sub_function_code = row['Sub Function Code']
                    treasury_appropriation_account.sub_function_description = row['Sub Function Description']

                    treasury_appropriation_account.save()
                    self.logger.log(20, "loaded %s" % treasury_appropriation_account.gwa_tas_name)

        except IOError:
            self.logger.log("Could not open file to load from")
