from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import ObjectDoesNotExist
from django.db import connections
from django.utils import timezone
import logging
import django

from usaspending_api.submissions.models import *
from usaspending_api.accounts.models import *


# This command will load a single submission from the data broker database into
# the data store using SQL commands to pull the raw data from the broker and
# by creating new django model instances for each object
class Command(BaseCommand):
    help = "Loads a single submission from the configured data broker database"
    logger = logging.getLogger('console')

    def add_arguments(self, parser):
        parser.add_argument('submission_id', nargs=1, help='the submission id to load', type=int)

        parser.add_argument(
            '--delete',
            action='store_true',
            dest='delete',
            default=False,
            help='Delete the submission if it exists instead of updating it',
        )

    def handle(self, *args, **options):
        # Grab the data broker database connections
        try:
            db_conn = connections['data_broker']
            db_cursor = db_conn.cursor()
        except Exception as err:
            self.logger.critical('Could not connect to database. Is DATA_BROKER_DATABASE_URL set?')
            self.logger.critical(print(err))
            return

        # Grab the submission id
        submission_id = options['submission_id'][0]

        # Verify the ID exists in the database, and grab the data
        db_cursor.execute('SELECT * FROM submission WHERE submission_id = %s', [submission_id])
        submission_data = dictfetchall(db_cursor)

        if len(submission_data) == 0:
            self.logger.error('Could not find submission with id ' + str(submission_id))
            return
        elif len(submission_data) > 1:
            self.logger.error('Found multiple submissions with id ' + str(submission_id))
            return

        # We have a single submission, which is what we want
        submission_data = submission_data[0]

        # Create the submission data instances
        # We load in currently available data, but the model references will
        # stick around so we can update and save as we get more data from
        # other tables

        # First, check if we already have entries for this submission id
        submission_attributes = None
        update = False
        try:
            submission_attributes = SubmissionAttributes.objects.get(pk=submission_id)
            if options['delete']:
                self.logger.info('Submission id ' + str(submission_id) + ' already exists. It will be deleted.')
                submission_attributes.delete()
                submission_attributes = SubmissionAttributes()
            else:
                self.logger.info('Submission id ' + str(submission_id) + ' already exists. Records will be updated.')
                update = True
        except ObjectDoesNotExist:
            submission_attributes = SubmissionAttributes()

        submission_attributes.submission_id = submission_data['submission_id']
        # User id is deprecated - need to move to Django Auth
        submission_attributes.user_id = 1
        submission_attributes.cgac_code = submission_data['cgac_code']
        submission_attributes.reporting_period_start = submission_data['reporting_start_date']
        submission_attributes.reporting_period_end = submission_data['reporting_end_date']
        if update:
            submission_attributes.update_date = timezone.now()
        else:
            submission_attributes.create_date = timezone.now()

        submission_attributes.save()

        if update:
            submission_process = SubmissionProcess.objects.get(submission=submission_attributes)
        else:
            submission_process = SubmissionProcess()
        submission_process.submission = submission_attributes
        if update:
            submission_process.update_date = timezone.now()
        else:
            submission_process.create_date = timezone.now()

        submission_process.save()

        # Move on, and grab file data
        db_cursor.execute('SELECT * FROM appropriation WHERE submission_id = %s', [submission_id])
        appropriation_data = dictfetchall(db_cursor)
        self.logger.info('Acquired appropriation data for ' + str(submission_id) + ', there are ' + str(len(appropriation_data)) + ' rows.')

        # Create account objects
        for row in appropriation_data:
            # Check and see if there is an entry for this TAS
            treasury_account = None
            try:
                treasury_account = TreasuryAppropriationAccount.objects.get(tas_rendering_label=row['tas'])
            except ObjectDoesNotExist:
                treasury_account = TreasuryAppropriationAccount()
                treasury_account.tas_rendering_label = row['tas']
                treasury_account.allocation_transfer_agency_id = row['allocation_transfer_agency']
                treasury_account.responsible_agency_id = row['agency_identifier']
                treasury_account.beginning_period_of_availa = row['beginning_period_of_availa']
                treasury_account.ending_period_of_availabil = row['ending_period_of_availabil']
                treasury_account.availability_type_code = row['availability_type_code']
                treasury_account.main_account_code = row['main_account_code']
                treasury_account.sub_account_code = row['sub_account_code']
                treasury_account.create_date = timezone.now()

                treasury_account.save()

            # Now that we have the account, we can load the appropriation balances
            # TODO: Figure out how we want to determine what row is overriden by what row
            # If we want to correlate, the following attributes are available in the
            # data broker data that might be useful: appropriation_id, row_number
            # appropriation_balances = somethingsomething get appropriation balances...
            appropriation_balances = AppropriationAccountBalances()
            appropriation_balances.treasury_account_identifier = treasury_account
            appropriation_balances.submission_process = submission_process
            appropriation_balances.budget_authority_unobligat_fyb = row['budget_authority_unobligat_fyb']
            appropriation_balances.adjustments_to_unobligated_cpe = row['adjustments_to_unobligated_cpe']
            appropriation_balances.budget_authority_appropria_cpe = row['budget_authority_appropria_cpe']
            appropriation_balances.borrowing_authority_amount_cpe = row['borrowing_authority_amount_cpe']
            appropriation_balances.contract_authority_amount_cpe = row['contract_authority_amount_cpe']
            appropriation_balances.spending_authority_from_of_cpe = row['spending_authority_from_of_cpe']
            appropriation_balances.other_budgetary_resources_cpe = row['other_budgetary_resources_cpe']
            appropriation_balances.budget_authority_available_cpe = row['budget_authority_available_cpe']
            appropriation_balances.gross_outlay_amount_by_tas_cpe = row['gross_outlay_amount_by_tas_cpe']
            appropriation_balances.deobligations_recoveries_r_cpe = row['deobligations_recoveries_r_cpe']
            appropriation_balances.unobligated_balance_cpe = row['unobligated_balance_cpe']
            appropriation_balances.status_of_budgetary_resour_cpe = row['status_of_budgetary_resour_cpe']
            appropriation_balances.obligations_incurred_total_cpe = row['obligations_incurred_total_cpe']
            appropriation_balances.tas_rendering_label = row['tas']
            appropriation_balances.create_date = timezone.now()

            appropriation_balances.save()


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]
