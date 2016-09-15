from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import ObjectDoesNotExist
from django.db import connections
from django.utils import timezone
import logging
import django

from usaspending_api.submissions.models import *


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
        try:
            submission_attributes = SubmissionAttributes.objects.get(pk=submission_id)
            if options['delete']:
                self.logger.info('Submission id ' + str(submission_id) + ' already exists. It will be deleted.')
                submission_attributes.delete()
                submission_attributes = SubmissionAttributes()
            else:
                self.logger.info('Submission id ' + str(submission_id) + ' already exists. Records will be updated.')
                # TODO
        except ObjectDoesNotExist:
            submission_attributes = SubmissionAttributes()

        submission_attributes.submission_id = submission_data['submission_id']
        # User id is deprecated - need to move to Django Auth
        submission_attributes.user_id = 1
        submission_attributes.cgac_code = submission_data['cgac_code']
        submission_attributes.reporting_period_start = submission_data['reporting_start_date']
        submission_attributes.reporting_period_end = submission_data['reporting_end_date']
        submission_attributes.create_date = timezone.now()

        submission_attributes.save()

        submission_process = SubmissionProcess()
        submission_process.submission = submission_attributes

        submission_process.save()


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]
