from datetime import datetime
import logging
import os

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.core.management.base import BaseCommand
from django.core.serializers.json import json
from django.db import connections
from django.db.models import Q
from django.core.cache import caches

from usaspending_api.accounts.models import AppropriationAccountBalances, TreasuryAppropriationAccount
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards,
    TransactionAssistance, TransactionContract, Transaction, AWARD_TYPES, CONTRACT_PRICING_TYPES)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import (
    Agency, CFDAProgram, LegalEntity, Location, ObjectClass, RefCountryCode, RefProgramActivity)
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards
import usaspending_api.etl.helpers as h

# This dictionary will hold a map of tas_id -> treasury_account to ensure we don't
# keep hitting the databroker DB for account data
TAS_ID_TO_ACCOUNT = {}

# Store some additional support data needed for the type description matching
award_type_dict = {a[0]: a[1] for a in AWARD_TYPES}
contract_pricing_dict = {c[0]: c[1] for c in CONTRACT_PRICING_TYPES}

# Lists to store for update_awards and update_contract_awards
AWARD_UPDATE_ID_LIST = []
AWARD_CONTRACT_UPDATE_ID_LIST = []

awards_cache = caches['awards']


class Command(BaseCommand):
    """
    This command will load a single submission from the data broker database into
    the data store using SQL commands to pull the raw data from the broker and
    by creating new django model instances for each object
    """
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

        awards_cache.clear()

        # Grab the data broker database connections
        if not options['test']:
            try:
                db_conn = connections['data_broker']
                db_cursor = db_conn.cursor()
            except Exception as err:
                self.logger.critical('Could not connect to database. Is DATA_BROKER_DATABASE_URL set?')
                self.logger.critical(print(err))
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
            submission_attributes = SubmissionAttributes.objects.get(broker_submission_id=submission_id)
            if options['delete']:
                self.logger.info('Submission id ' + str(submission_id) + ' already exists. It will be deleted.')
                submission_attributes.delete()
                submission_attributes = SubmissionAttributes()
            else:
                self.logger.info('Submission id ' + str(submission_id) + ' already exists. Records will be updated.')
        except ObjectDoesNotExist:
            submission_attributes = SubmissionAttributes()

        # Update and save submission attributes
        field_map = {
            'reporting_period_start': 'reporting_start_date',
            'reporting_period_end': 'reporting_end_date'
        }

        # Create our value map - specific data to load
        value_map = {
            'broker_submission_id': submission_id
        }

        del submission_data["submission_id"]  # To avoid collisions with the newer PK system

        load_data_into_model(submission_attributes, submission_data, field_map=field_map, value_map=value_map, save=True)

        # Move on, and grab file A data
        db_cursor.execute('SELECT * FROM appropriation WHERE submission_id = %s', [submission_id])
        appropriation_data = dictfetchall(db_cursor)
        self.logger.info('Acquired appropriation data for ' + str(submission_id) + ', there are ' + str(len(appropriation_data)) + ' rows.')

        # Create account objects
        for row in appropriation_data:
            # Check and see if there is an entry for this TAS
            treasury_account = get_treasury_appropriation_account_tas_lookup(row.get('tas_id'), db_cursor)
            if treasury_account is None:
                raise Exception('Could not find appropriation account for TAS: ' + row['tas'])

            # Now that we have the account, we can load the appropriation balances
            # TODO: Figure out how we want to determine what row is overriden by what row
            # If we want to correlate, the following attributes are available in the
            # data broker data that might be useful: appropriation_id, row_number
            # appropriation_balances = somethingsomething get appropriation balances...
            appropriation_balances = AppropriationAccountBalances()

            value_map = {
                'treasury_account_identifier': treasury_account,
                'submission': submission_attributes,
                'reporting_period_start': submission_attributes.reporting_period_start,
                'reporting_period_end': submission_attributes.reporting_period_end
            }

            field_map = {}

            load_data_into_model(appropriation_balances, row, field_map=field_map, value_map=value_map, save=True)

        AppropriationAccountBalances.populate_final_of_fy()

        # Let's get File B information
        db_cursor.execute('SELECT * FROM object_class_program_activity WHERE submission_id = %s', [submission_id])
        prg_act_obj_cls_data = dictfetchall(db_cursor)
        self.logger.info('Acquired program activity object class data for ' + str(submission_id) + ', there are ' + str(len(prg_act_obj_cls_data)) + ' rows.')

        for row in prg_act_obj_cls_data:
            account_balances = None
            try:
                # Check and see if there is an entry for this TAS
                treasury_account = get_treasury_appropriation_account_tas_lookup(row.get('tas_id'), db_cursor)
                if treasury_account is None:
                    raise Exception('Could not find appropriation account for TAS: ' + row['tas'])
                account_balances = AppropriationAccountBalances.objects.get(treasury_account_identifier=treasury_account)
            except:
                continue

            financial_by_prg_act_obj_cls = FinancialAccountsByProgramActivityObjectClass()

            value_map = {
                'submission': submission_attributes,
                'reporting_period_start': submission_attributes.reporting_period_start,
                'reporting_period_end': submission_attributes.reporting_period_end,
                'treasury_account': treasury_account,
                'appropriation_account_balances': account_balances,
                'object_class': get_or_create_object_class(row['object_class'], row['by_direct_reimbursable_fun'], self.logger),
                'program_activity_id': get_or_create_program_activity(row['program_activity_code'])
            }

            load_data_into_model(financial_by_prg_act_obj_cls, row, value_map=value_map, save=True)

        # Let's get File C information
        db_cursor.execute('SELECT * FROM award_financial WHERE submission_id = %s', [submission_id])
        award_financial_data = dictfetchall(db_cursor)
        self.logger.info('Acquired award financial data for ' + str(submission_id) + ', there are ' + str(len(award_financial_data)) + ' rows.')

        award_queue = {}
        afd_queue = []

        for row in award_financial_data:
            account_balances = None
            try:
                # Check and see if there is an entry for this TAS
                treasury_account = get_treasury_appropriation_account_tas_lookup(row.get('tas_id'), db_cursor)
                if treasury_account is None:
                    raise Exception('Could not find appropriation account for TAS: ' + row['tas'])
                # Find the award that this award transaction belongs to. If it doesn't exist, create it.
                award = Award.get_or_create_summary_award(
                    piid=row.get('piid'),
                    fain=row.get('fain'),
                    uri=row.get('uri'),
                    parent_award_id=row.get('parent_award_id'),
                    use_cache=True)
                award.latest_submission = submission_attributes
                award_queue[award.manual_hash()] = award
            except:   # TODO: silently swallowing a bare exception is bad mojo
                continue

            award_financial_data = FinancialAccountsByAwards()

            value_map = {
                'award': award,
                'submission': submission_attributes,
                'reporting_period_start': submission_attributes.reporting_period_start,
                'reporting_period_end': submission_attributes.reporting_period_end,
                'treasury_account': treasury_account,
                'object_class': get_or_create_object_class(row['object_class'], row['by_direct_reimbursable_fun'], self.logger),
                'program_activity_id': get_or_create_program_activity(row['program_activity_code'])
            }

            afd = load_data_into_model(award_financial_data, row, value_map=value_map, save=False)
            afd_queue.append(afd)

        Award.objects.bulk_create(award_queue.values())
        FinancialAccountsByAwards.objects.bulk_create(afd_queue)
        awards_cache.clear()

        # File D2
        db_cursor.execute('SELECT * FROM award_financial_assistance WHERE submission_id = %s', [submission_id])
        award_financial_assistance_data = dictfetchall(db_cursor)
        self.logger.info('Acquired award financial assistance data for ' + str(submission_id) + ', there are ' + str(len(award_financial_assistance_data)) + ' rows.')

        legal_entity_location_field_map = {
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "city_code": "legal_entity_city_code",
            "city_name": "legal_entity_city_name",
            "congressional_code": "legal_entity_congressional",
            "county_code": "legal_entity_county_code",
            "county_name": "legal_entity_county_name",
            "foreign_city_name": "legal_entity_foreign_city",
            "foreign_postal_code": "legal_entity_foreign_posta",
            "foreign_province": "legal_entity_foreign_provi",
            "state_code": "legal_entity_state_code",
            "state_name": "legal_entity_state_name",
            "zip5": "legal_entity_zip5",
            "zip_last4": "legal_entity_zip_last4",
            "location_country_code": "legal_entity_country_code"
        }

        place_of_performance_field_map = {
            "city_name": "place_of_performance_city",
            "performance_code": "place_of_performance_code",
            "congressional_code": "place_of_performance_congr",
            "county_name": "place_of_perform_county_na",
            "foreign_location_description": "place_of_performance_forei",
            "state_name": "place_of_perform_state_nam",
            "zip4": "place_of_performance_zip4a",
            "location_country_code": "place_of_perform_country_c"

        }

        legal_entity_location_value_map = {
            "recipient_flag": True
        }

        place_of_performance_value_map = {
            "place_of_performance_flag": True
        }

        fad_field_map = {
            "type": "assistance_type",
            "description": "award_description",
        }

        for row in award_financial_assistance_data:

            legal_entity_location, created = get_or_create_location(legal_entity_location_field_map, row, legal_entity_location_value_map)

            # Create the legal entity if it doesn't exist
            try:
                legal_entity = LegalEntity.objects.get(recipient_unique_id=row['awardee_or_recipient_uniqu'])
            except ObjectDoesNotExist:
                legal_entity_value_map = {
                    "location": legal_entity_location,
                    "legal_entity_id": row['awardee_or_recipient_uniqu']
                }
                legal_entity = load_data_into_model(LegalEntity(), row, value_map=legal_entity_value_map, save=True)

            # Create the place of performance location
            pop_location, created = get_or_create_location(place_of_performance_field_map, row, place_of_performance_value_map)

            # Find the award that this award transaction belongs to. If it doesn't exist, create it.
            award = Award.get_or_create_summary_award(
                piid=row.get('piid'),
                fain=row.get('fain'),
                uri=row.get('uri'),
                parent_award_id=row.get('parent_award_id'))
            award.save()

            AWARD_UPDATE_ID_LIST.append(award.id)

            parent_txn_value_map = {
                "award": award,
                "awarding_agency": Agency.objects.filter(toptier_agency__cgac_code=row['awarding_agency_code'],
                                                         subtier_agency__subtier_code=row["awarding_sub_tier_agency_c"]).first(),
                "funding_agency": Agency.objects.filter(toptier_agency__cgac_code=row['funding_agency_code'],
                                                        subtier_agency__subtier_code=row["funding_sub_tier_agency_co"]).first(),
                "recipient": legal_entity,
                "place_of_performance": pop_location,
                'submission': submission_attributes,
                'type_description': award_type_dict.get(row['assistance_type']),
                "period_of_performance_start_date": format_date(row['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
                "action_date": format_date(row['action_date']),
            }

            transaction_instance = load_data_into_model(
                Transaction(), row,
                field_map=fad_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            transaction_instance, created = Transaction.objects.get_or_create(**transaction_instance)

            fad_value_map = {
                "transaction": transaction_instance,
                "submission": submission_attributes,
                "cfda": CFDAProgram.objects.filter(program_number=row['cfda_number']).first(),
                'reporting_period_start': submission_attributes.reporting_period_start,
                'reporting_period_end': submission_attributes.reporting_period_end,
                "period_of_performance_start_date": format_date(row['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
            }

            financial_assistance_data = load_data_into_model(
                TransactionAssistance(), row,
                field_map=fad_field_map,
                value_map=fad_value_map,
                save=True)

        # File D1
        db_cursor.execute('SELECT * FROM award_procurement WHERE submission_id = %s', [submission_id])
        procurement_data = dictfetchall(db_cursor)
        self.logger.info('Acquired award procurement data for ' + str(submission_id) + ', there are ' + str(len(procurement_data)) + ' rows.')

        legal_entity_location_field_map = {
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "location_country_code": "legal_entity_country_code",
            "city_name": "legal_entity_city_name",
            "congressional_code": "legal_entity_congressional",
            "state_code": "legal_entity_state_code",
            "zip4": "legal_entity_zip4"
        }

        place_of_performance_field_map = {
            # not sure place_of_performance_locat maps exactly to city name
            "city_name": "place_of_performance_locat",
            "congressional_code": "place_of_performance_congr",
            "state_code": "place_of_performance_state",
            "zip4": "place_of_performance_zip4a",
            "location_country_code": "place_of_perform_country_c"
        }

        contract_field_map = {
            "type": "contract_award_type",
            "description": "award_description"
        }

        for row in procurement_data:
            legal_entity_location, created = get_or_create_location(legal_entity_location_field_map, row, legal_entity_location_value_map)

            # Create the legal entity if it doesn't exist
            try:
                legal_entity = LegalEntity.objects.get(recipient_unique_id=row['awardee_or_recipient_uniqu'])
            except ObjectDoesNotExist:
                legal_entity_value_map = {
                    "location": legal_entity_location,
                    "legal_entity_id": row['awardee_or_recipient_uniqu'],
                }
                legal_entity = load_data_into_model(LegalEntity(), row, value_map=legal_entity_value_map, save=True)

            # Create the place of performance location
            pop_location, created = get_or_create_location(place_of_performance_field_map, row, place_of_performance_value_map)

            # Find the award that this award transaction belongs to. If it doesn't exist, create it.
            award = Award.get_or_create_summary_award(
                piid=row.get('piid'),
                fain=row.get('fain'),
                uri=row.get('uri'),
                parent_award_id=row.get('parent_award_id'))
            award.save()

            AWARD_UPDATE_ID_LIST.append(award.id)
            AWARD_CONTRACT_UPDATE_ID_LIST.append(award.id)

            parent_txn_value_map = {
                "award": award,
                "awarding_agency": Agency.objects.filter(toptier_agency__cgac_code=row['awarding_agency_code'],
                                                         subtier_agency__subtier_code=row["awarding_sub_tier_agency_c"]).first(),
                "funding_agency": Agency.objects.filter(toptier_agency__cgac_code=row['funding_agency_code'],
                                                        subtier_agency__subtier_code=row["funding_sub_tier_agency_co"]).first(),
                "recipient": legal_entity,
                'type_description': award_type_dict.get(row['contract_award_type']),
                "place_of_performance": pop_location,
                'submission': submission_attributes,
                "period_of_performance_start_date": format_date(row['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
                "action_date": format_date(row['action_date']),
            }

            transaction_instance = load_data_into_model(
                Transaction(), row,
                field_map=contract_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            transaction_instance, created = Transaction.objects.get_or_create(**transaction_instance)

            contract_value_map = {
                'transaction': transaction_instance,
                'submission': submission_attributes,
                'type_of_contract_pricing_description': contract_pricing_dict.get(row['type_of_contract_pricing']),
                'reporting_period_start': submission_attributes.reporting_period_start,
                'reporting_period_end': submission_attributes.reporting_period_end,
                "period_of_performance_potential_end_date": format_date(row['period_of_perf_potential_e'])
            }

            contract_instance = load_data_into_model(
                TransactionContract(), row,
                field_map=contract_field_map,
                value_map=contract_value_map,
                save=True)

        # Update awards for new linkages
        update_awards(tuple(AWARD_UPDATE_ID_LIST))
        update_contract_awards(tuple(AWARD_CONTRACT_UPDATE_ID_LIST))


def format_date(date_string, pattern='%Y%m%d'):
    try:
        return datetime.strptime(date_string, pattern)
    except TypeError:
        return None


def get_or_create_object_class(row_object_class, row_direct_reimbursable, logger):
    """Lookup an object class record.

        Args:
            row_object_class: object class from the broker
            row_direct_reimbursable: direct/reimbursable flag from the broker
                (used only when the object_class is 3 digits instead of 4)
    """
    if len(row_object_class) == 4:
        # this is a 4 digit object class, 1st digit = direct/reimbursable information
        direct_reimbursable = row_object_class[:1]
        object_class = row_object_class[1:]
    else:
        # the object class field is the 3 digit version, so grab direct/reimbursable
        # information from a separate field
        if row_direct_reimbursable is None:
            direct_reimbursable = None
        elif row_direct_reimbursable.lower() == 'd':
            direct_reimbursable = 1
        elif row_direct_reimbursable.lower() == 'r':
            direct_reimbursable = 2
        else:
            direct_reimbursable = None
        object_class = row_object_class

    # set major object class; note that we shouldn't have to do this
    # once we have a complete list of object classes loaded to ObjectClass
    # (we only fill it in now should it be needed by the subsequent get_or_create)
    major_object_class = '{}0'.format(object_class[:1])
    if major_object_class == '10':
        major_object_class_name = 'Personnel compensation and benefits'
    elif major_object_class == '20':
        major_object_class_name = 'Contractual services and supplies'
    elif major_object_class == '30':
        major_object_class_name = 'Acquisition of assets'
    elif major_object_class == '40':
        major_object_class_name = 'Grants and fixed charges'
    else:
        major_object_class_name = 'Other'

    # we couldn't find a matching object class record, so create one
    # (note: is this really what we want to do? should we map to an 'unknown' instead?)
    # should
    obj_class, created = ObjectClass.objects.get_or_create(
        major_object_class=major_object_class,
        major_object_class_name=major_object_class_name,
        object_class=object_class,
        direct_reimbursable=direct_reimbursable)
    if created:
        logger.warning('Created missing object_class record for {}'.format(object_class))

    return obj_class


def get_or_create_program_activity(program_activity_code):
    # We do it this way rather than .get_or_create because we do not want to
    # duplicate existing pk's with null values
    prg_activity = RefProgramActivity.objects.filter(program_activity_code=program_activity_code).first()
    if prg_activity is None and program_activity_code is not None:
        prg_activity = RefProgramActivity.objects.create(program_activity_code=program_activity_code)
    return prg_activity


def get_treasury_appropriation_account_tas_lookup(tas_lookup_id, db_cursor):
    """Get the matching TAS object from the broker database and save it to our running list."""
    if tas_lookup_id in TAS_ID_TO_ACCOUNT:
        return TAS_ID_TO_ACCOUNT[tas_lookup_id]
    # Checks the broker DB tas_lookup table for the tas_id and returns the matching TAS object in the datastore
    db_cursor.execute('SELECT * FROM tas_lookup WHERE account_num = %s', [tas_lookup_id])
    tas_data = dictfetchall(db_cursor)

    # These or "" convert from none to a blank string, which is how the TAS table stores nulls
    q_kwargs = {
        "allocation_transfer_agency_id": tas_data[0]["allocation_transfer_agency"] or "",
        "agency_id": tas_data[0]["agency_identifier"] or "",
        "beginning_period_of_availability": tas_data[0]["beginning_period_of_availa"] or "",
        "ending_period_of_availability": tas_data[0]["ending_period_of_availabil"] or "",
        "availability_type_code": tas_data[0]["availability_type_code"] or "",
        "main_account_code": tas_data[0]["main_account_code"] or "",
        "sub_account_code": tas_data[0]["sub_account_code"] or ""
    }

    TAS_ID_TO_ACCOUNT[tas_lookup_id] = TreasuryAppropriationAccount.objects.filter(Q(**q_kwargs)).first()
    return TAS_ID_TO_ACCOUNT[tas_lookup_id]


def load_data_into_model(model_instance, data, **kwargs):
    """
    Loads data into a model instance
    Data should be a row, a dict of field -> value pairs
    Keyword args:
        field_map - A map of field columns to data columns. This is so you can map
                    a field in the data to a different field in the model. For instance,
                    model.tas_rendering_label = data['tas'] could be set up like this:
                    field_map = {'tas_rendering_label': 'tas'}
                    The algorithm checks for a value map before a field map, so if the
                    column is present in both value_map and field_map, value map takes
                    precedence over the other
        value_map - Want to force or override a value? Specify the field name for the
                    instance and the data you want to load. Example:
                    {'update_date': timezone.now()}
                    The algorithm checks for a value map before a field map, so if the
                    column is present in both value_map and field_map, value map takes
                    precedence over the other
        save - Defaults to False, but when set to true will save the model at the end
        as_dict - If true, returns the model as a dict instead of saving or altering
    """
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
        # Let's handle the data source field here for all objects
        if field is 'data_source':
            store_value(mod, field, 'DBR')
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
    else:
        return model_instance


def get_or_create_location(location_map, row, location_value_map={}):
    """
    Retrieve or create a location object

    Input parameters:
        - location_map: a dictionary with key = field name on the location model
            and value = corresponding field name on the current row of data
        - row: the row of data currently being loaded
    """
    location_country = RefCountryCode.objects.filter(
        country_code=row[location_map.get('location_country_code')]).first()

    # temporary fix until broker is patched: remove later
    state_code = row.get(location_map.get('state_code'))
    if state_code is not None:
        # Fix for procurement data foreign provinces stored as state_code
        if location_country and location_country.country_code != "USA":
            location_value_map.update({'foreign_province': state_code})
            location_value_map.update({'state_code': None})
        else:
            location_value_map.update({'state_code': state_code.replace('.', '')})
    # end of temporary fix

    if location_country:
        location_value_map.update({
            'location_country_code': location_country,
            'country_name': location_country.country_name
        })
    else:
        # no country found for this code
        location_value_map.update({
            'location_country_code': None,
            'country_name': None
        })

    location_data = load_data_into_model(
        Location(), row, value_map=location_value_map, field_map=location_map, as_dict=True)

    del location_data['data_source']  # hacky way to ensure we don't create a series of empty location records
    if len(location_data):
        try:
            location_object, created = Location.objects.get_or_create(**location_data, defaults={'data_source': 'DBR'})
        except MultipleObjectsReturned:
            # incoming location data is so sparse that comparing it to existing locations
            # yielded multiple records. create a new location with this limited info.
            # note: this will need fixed up to prevent duplicate location records with the
            # same sparse data
            location_object = Location.objects.create(**location_data)
            created = True
        return location_object, created
    else:
        # record had no location information at all
        return None, None


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


class PhonyCursor:
    """Spoofs the db cursor responses."""

    def __init__(self):
        json_data = open(os.path.join(os.path.dirname(__file__), '../../tests/etl_test_data.json'))
        self.db_responses = json.load(json_data)
        json_data.close()

        self.results = None

    def execute(self, statement, parameters):
        self.results = self.db_responses[statement]
