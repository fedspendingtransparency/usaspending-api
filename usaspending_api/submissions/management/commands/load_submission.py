from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import ObjectDoesNotExist
from django.core.serializers.json import json, DjangoJSONEncoder
from django.db import connections
from django.utils import timezone
from django.conf import settings
from datetime import datetime
import logging
import django
import os

from usaspending_api.submissions.models import *
from usaspending_api.accounts.models import *
from usaspending_api.financial_activities.models import *
from usaspending_api.awards.models import *
from usaspending_api.references.models import *


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

        parser.add_argument(
            '--test',
            action='store_true',
            dest='test',
            default=False,
            help='Runs the submission loader in test mode, sets the delete flag enabled, and uses stored data rather than pulling from a database'
        )

    def handle(self, *args, **options):
        # Grab the data broker database connections
        if not options['test']:
            try:
                db_conn = connections['data_broker']
                db_cursor = db_conn.cursor()
            except Exception as err:
                # self.logger.critical('Could not connect to database. Is DATA_BROKER_DATABASE_URL set?')
                # self.logger.critical(print(err))
                return
        else:
            options['delete'] = True
            db_cursor = PhonyCursor()

        # Grab the submission id
        submission_id = options['submission_id'][0]

        # Verify the ID exists in the database, and grab the data
        db_cursor.execute('SELECT * FROM submission WHERE submission_id = %s', [submission_id])
        submission_data = dictfetchall(db_cursor)

        if len(submission_data) == 0:
            # self.logger.error('Could not find submission with id ' + str(submission_id))
            return
        elif len(submission_data) > 1:
            # self.logger.error('Found multiple submissions with id ' + str(submission_id))
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
                # self.logger.info('Submission id ' + str(submission_id) + ' already exists. It will be deleted.')
                submission_attributes.delete()
                submission_attributes = SubmissionAttributes()
            else:
                # self.logger.info('Submission id ' + str(submission_id) + ' already exists. Records will be updated.')
                update = True
        except ObjectDoesNotExist:
            submission_attributes = SubmissionAttributes()

        # Update and save submission attributes
        # Create our value map - specific data to load
        value_map = {
            'user_id': 1,
        }

        load_data_into_model(submission_attributes, submission_data, value_map=value_map, save=True)

        # Update and save submission process
        value_map = {
            'submission': submission_attributes
        }

        submission_process = SubmissionProcess()
        if update:
            submission_process = SubmissionProcess.objects.get(submission=submission_attributes)

        load_data_into_model(submission_process, [], value_map=value_map, save=True)

        # Move on, and grab file A data
        db_cursor.execute('SELECT * FROM appropriation WHERE submission_id = %s', [submission_id])
        appropriation_data = dictfetchall(db_cursor)
        # self.logger.info('Acquired appropriation data for ' + str(submission_id) + ', there are ' + str(len(appropriation_data)) + ' rows.')

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
                    'submission_id': submission_attributes.submission_id,
                    'allocation_transfer_agency__cgac_code': 'allocation_transfer_agency',
                    'responsible_agency__cgac_code': 'agency_identifier'
                }

                load_data_into_model(treasury_account, row, field_map=field_map, save=True)

            # Now that we have the account, we can load the appropriation balances
            # TODO: Figure out how we want to determine what row is overriden by what row
            # If we want to correlate, the following attributes are available in the
            # data broker data that might be useful: appropriation_id, row_number
            # appropriation_balances = somethingsomething get appropriation balances...
            appropriation_balances = AppropriationAccountBalances()

            value_map = {
                'treasury_account_identifier': treasury_account,
                'submission_process': submission_process,
                'submission': submission_attributes
            }

            field_map = {
                'tas_rendering_label': 'tas'
            }

            load_data_into_model(appropriation_balances, row, field_map=field_map, value_map=value_map, save=True)

        # Let's get File B information
        db_cursor.execute('SELECT * FROM object_class_program_activity WHERE submission_id = %s', [submission_id])
        prg_act_obj_cls_data = dictfetchall(db_cursor)
        # self.logger.info('Acquired program activity object class data for ' + str(submission_id) + ', there are ' + str(len(prg_act_obj_cls_data)) + ' rows.')

        for row in prg_act_obj_cls_data:
            account_balances = None
            try:
                account_balances = AppropriationAccountBalances.objects.get(tas_rendering_label=row['tas'])
            except:
                continue

            financial_by_prg_act_obj_cls = FinancialAccountsByProgramActivityObjectClass()

            value_map = {
                'submission': submission_attributes,
                'appropriation_account_balances': account_balances,
                'object_class': RefObjectClassCode.objects.filter(pk=row['object_class']).first(),
                'program_activity_code': RefProgramActivity.objects.filter(pk=row['program_activity_code']).first(),
            }

            load_data_into_model(financial_by_prg_act_obj_cls, row, value_map=value_map, save=True)

        # Let's get File C information
        db_cursor.execute('SELECT * FROM award_financial WHERE submission_id = %s', [submission_id])
        award_financial_data = dictfetchall(db_cursor)
        # self.logger.info('Acquired award financial data for ' + str(submission_id) + ', there are ' + str(len(award_financial_data)) + ' rows.')

        for row in award_financial_data:
            account_balances = None
            try:
                account_balances = AppropriationAccountBalances.objects.get(tas_rendering_label=row['tas'])
            except:
                continue

            award_financial_data = FinancialAccountsByAwards()

            value_map = {
                'submission': submission_attributes,
                'appropriation_account_balances': account_balances,
                'object_class': RefObjectClassCode.objects.filter(pk=row['object_class']).first(),
                'program_activity_code': RefProgramActivity.objects.filter(pk=row['program_activity_code']).first(),
            }

            load_data_into_model(award_financial_data, row, value_map=value_map, save=True)

            afd_trans = FinancialAccountsByAwardsTransactionObligations()

            value_map = {
                'financial_accounts_by_awards': award_financial_data,
                'submission': submission_attributes
            }

            load_data_into_model(afd_trans, row, value_map=value_map, save=True)

        # File D2
        db_cursor.execute('SELECT * FROM award_financial_assistance WHERE submission_id = %s', [submission_id])
        award_financial_assistance_data = dictfetchall(db_cursor)
        # self.logger.info('Acquired award financial assistance data for ' + str(submission_id) + ', there are ' + str(len(award_financial_assistance_data)) + ' rows.')

        # Create LegalEntity
        legal_entity_location_field_map = {
            "location_address_line1": "legal_entity_address_line1",
            "location_address_line2": "legal_entity_address_line2",
            "location_address_line3": "legal_entity_address_line3",
            "location_city_code": "legal_entity_city_code",
            "location_city_name": "legal_entity_city_name",
            "location_congressional_code": "legal_entity_congressional",
            # Since the country code is actually the id, we add _id to set up FK
            "location_country_code_id": "legal_entity_country_code",
            "location_county_code": "legal_entity_county_code",
            "location_county_name": "legal_entity_county_name",
            "location_foreign_city_name": "legal_entity_foreign_city",
            "location_foreign_postal_code": "legal_entity_foreign_posta",
            "location_foreign_province": "legal_entity_foreign_provi",
            "location_state_code": "legal_entity_state_code",
            "location_state_name": "legal_entity_state_name",
            "location_zip5": "legal_entity_zip5",
            "location_zip_last4": "legal_entity_zip_last4",
        }

        # Create the place of performance location
        place_of_performance_field_map = {
            "location_city_name": "place_of_performance_city",
            "location_performance_code": "place_of_performance_code",
            "location_congressional_code": "place_of_performance_congr",
            # Tagging ID on the following to set the FK
            "location_country_code_id": "place_of_perform_country_c",
            "location_county_name": "place_of_perform_county_na",
            "location_foreign_location_description": "place_of_performance_forei",
            "location_state_name": "place_of_perform_state_nam",
            "location_zip4": "place_of_performance_zip4a",
        }

        for row in award_financial_assistance_data:
            location = load_data_into_model(Location(), row, field_map=legal_entity_location_field_map, as_dict=True)
            legal_entity_location, created = Location.objects.get_or_create(**location)

            # Create the legal entity if it doesn't exist
            legal_entity = None
            try:
                legal_entity = LegalEntity.objects.get(row['awardee_or_recipient_uniqu'])
            except:
                legal_entity = LegalEntity()

                legal_entity_value_map = {
                    "location": legal_entity_location,
                    "legal_entity_id": row['awardee_or_recipient_uniqu']
                }

                load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

            location = load_data_into_model(Location(), row, field_map=place_of_performance_field_map, as_dict=True)
            pop_location, created = Location.objects.get_or_create(**location)

            # Create the base award, create actions, then tally the totals for the award
            award_field_map = {
                "description": "award_description",
                "awarding_agency__cgac_code": "awarding_agency_code",
                "funding_agency__cgac_code": "functing_agency_code",
                "type": "assistance_type"
            }

            award_value_map = {
                "period_of_performance_start_date": format_date(row['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
                "place_of_performance": pop_location,
                "date_signed": format_date(row['action_date']),
                "latest_submission": submission_attributes,
                "recipient": legal_entity,
            }

            award = load_data_into_model(Award(), row, field_map=award_field_map, value_map=award_value_map, as_dict=True)
            award, created = Award.objects.get_or_create(**award)

            fad_value_map = {
                "award": award,
                "submission": submission_attributes

            }

            financial_assistance_data = load_data_into_model(FinancialAssistanceAward(), row, value_map=fad_value_map, as_dict=True)
            fad, created = FinancialAssistanceAward.objects.get_or_create(**financial_assistance_data)

        # File D1
        db_cursor.execute('SELECT * FROM award_procurement WHERE submission_id = %s', [submission_id])
        procurement_data = dictfetchall(db_cursor)
        # self.logger.info('Acquired award procurement data for ' + str(submission_id) + ', there are ' + str(len(procurement_data)) + ' rows.')

        for row in procurement_data:
            # Yes, I could use Q objects here but for maintainability I broke it out
            award = Award.objects.filter(piid=row['parent_award_id']).first()
            if award is None:
                award = Award.objects.filter(fain=row['parent_award_id']).first()
            if award is None:
                award = Award.objects.filter(uri=row['parent_award_id']).first()
            if award is None:
                # self.logger.error('Could not find an award object with a matching identifier')
                continue

            procurement_value_map = {
                "award": award,
                'submission': submission_attributes
            }

            procurement = load_data_into_model(Procurement(), row, value_map=procurement_value_map, save=True)


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
#  as_dict - If true, returns the model as a dict instead of saving or altering
def load_data_into_model(model_instance, data, **kwargs):
    field_map = None
    value_map = None
    save = False
    as_dict = False

    if 'field_map' in kwargs:
        field_map = kwargs['field_map']
    if 'value_map' in kwargs:
        value_map = kwargs['value_map']
    if 'save' in kwargs:
        save = kwargs['save']
    if 'as_dict' in kwargs:
        as_dict = kwargs['as_dict']

    # Grab all the field names from the meta class of the model instance
    fields = [field.name for field in model_instance._meta.get_fields()]
    mod = model_instance

    if as_dict:
        mod = {}

    for field in fields:
        broker_field = field
        # If our field is the 'long form' field, we need to get what it maps to
        # in the broker so we can map the data properly
        if broker_field in settings.LONG_TO_TERSE_LABELS:
            broker_field = settings.LONG_TO_TERSE_LABELS[broker_field]
        sts = False
        if value_map:
            if broker_field in value_map:
                store_value(mod, field, value_map[broker_field])
                sts = True
            elif field in value_map:
                store_value(mod, field, value_map[field])
                sts = True
        if field_map and not sts:
            if broker_field in field_map:
                store_value(mod, field, data[field_map[broker_field]])
                sts = True
            elif field in field_map:
                store_value(mod, field, data[field_map[field]])
                sts = True
        if broker_field in data and not sts:
            store_value(mod, field, data[broker_field])
        elif field in data and not sts:
            store_value(mod, field, data[field])

    if save:
        model_instance.save()
    if as_dict:
        return mod


def format_date(date):
    return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')


def store_value(model_instance_or_dict, field, value):
    if value is None:
        return
    # print('Loading ' + str(field) + ' with ' + str(value))
    if isinstance(model_instance_or_dict, dict):
        model_instance_or_dict[field] = value
    else:
        setattr(model_instance_or_dict, field, value)


def dictfetchall(cursor):
    if isinstance(cursor, PhonyCursor):
        return cursor.results
    else:
        "Return all rows from a cursor as a dict"
        columns = [col[0] for col in cursor.description]
        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]


# Spoofs the db cursor responses
class PhonyCursor:

    def __init__(self):
        json_data = open(os.path.join(os.path.dirname(__file__), '../../test_data/etl_test_data.json'))
        self.db_responses = json.load(json_data)
        json_data.close()

        self.results = None

    def execute(self, statement, parameters):
        self.results = self.db_responses[statement]
