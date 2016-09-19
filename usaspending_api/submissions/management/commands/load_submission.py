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

        # Update and save submission attributes
        # Create our value map - specific data to load
        value_map = {
            'user_id': 1,
            'update_date': timezone.now()
        }

        # If we're not updating, we're creating - set the date
        if not update:
            value_map['create_date'] = timezone.now()
        load_data_into_model(submission_attributes, submission_data, value_map=value_map, save=True)

        # Update and save submission process
        value_map = {
            'update_date': timezone.now(),
            'submission': submission_attributes
        }

        if not update:
            value_map['create_date'] = timezone.now()

        submission_process = SubmissionProcess()
        if update:
            submission_process = SubmissionProcess.objects.get(submission=submission_attributes)

        load_data_into_model(submission_process, [], value_map=value_map, save=True)

        # Move on, and grab file A data
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

                field_map = {
                    'tas_rendering_label': 'tas',
                    'allocation_transfer_agency_id': 'allocation_transfer_agency',
                    'responsible_agency_id': 'agency_identifier'
                }

                value_map = {
                    'create_date': timezone.now(),
                    'update_date': timezone.now()
                }

                load_data_into_model(treasury_account, row, field_map=field_map, value_map=value_map, save=True)

            # Now that we have the account, we can load the appropriation balances
            # TODO: Figure out how we want to determine what row is overriden by what row
            # If we want to correlate, the following attributes are available in the
            # data broker data that might be useful: appropriation_id, row_number
            # appropriation_balances = somethingsomething get appropriation balances...
            appropriation_balances = AppropriationAccountBalances()

            value_map = {
                'treasury_account_identifier': treasury_account,
                'submission_process': submission_process,
                'create_date': timezone.now(),
                'update_date': timezone.now()
            }

            field_map = {
                'tas_rendering_label': 'tas'
            }

            load_data_into_model(appropriation_balances, row, field_map=field_map, value_map=value_map, save=True)

        # Let's get File B information
        db_cursor.execute('SELECT * FROM object_class_program_activity WHERE submission_id = %s', [submission_id])
        prg_act_obj_cls_data = dictfetchall(db_cursor)
        self.logger.info('Acquired program activity object class data for ' + str(submission_id) + ', there are ' + str(len(prg_act_obj_cls_data)) + ' rows.')


# Loads data into a model instance
# Data should be a row, a dict of field -> value pairs
# Keyword args are:
#  field_map - A map of field columns to data columns. This is so you can map
#               a field in the data to a different field in the model. For instance,
#               model.tas_rendering_label = data['tas'] could be set up like this:
#               field_map = {'tas_rendering_label': 'tas'}
#               The algorithm checks for a value map before a field map, so if the
#               column is present in both value_map and field_map, value map takes
#               precedence over the other
#  value_map - Want to force or override a value? Specify the field name for the
#               instance and the data you want to load. Example:
#               {'update_date': timezone.now()}
#               The algorithm checks for a value map before a field map, so if the
#               column is present in both value_map and field_map, value map takes
#               precedence over the other
#  save - Defaults to False, but when set to true will save the model at the end
def load_data_into_model(model_instance, data, **kwargs):
    field_map = None
    value_map = None
    save = False

    if 'field_map' in kwargs:
        field_map = kwargs['field_map']
    if 'value_map' in kwargs:
        value_map = kwargs['value_map']
    if 'save' in kwargs:
        save = kwargs['save']

    # Grab all the field names from the meta class of the model instance
    fields = [field.name for field in model_instance._meta.get_fields()]
    for field in fields:
        if field in data:
            setattr(model_instance, field, data[field])
        elif value_map:
            if field in value_map:
                setattr(model_instance, field, value_map[field])
        elif field_map:
            if field in field_map:
                setattr(model_instance, field, data[field_map[field]])

    if save:
        model_instance.save()


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]
