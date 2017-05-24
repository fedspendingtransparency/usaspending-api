from datetime import datetime
from decimal import Decimal
import logging
import os
import re

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.core.serializers.json import json
from django.db import connections
from django.db.models import F, Q
from django.core.cache import caches
import pandas as pd
import numpy as np

from usaspending_api.accounts.models import (
    AppropriationAccountBalances, AppropriationAccountBalancesQuarterly,
    TreasuryAppropriationAccount)
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards,
    TransactionAssistance, TransactionContract, Transaction)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass, TasProgramActivityObjectClassQuarterly)
from usaspending_api.references.models import (
    Agency, LegalEntity, Location, ObjectClass, RefCountryCode, Cfda, RefProgramActivity)
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.etl.award_helpers import (
    get_award_financial_transaction, update_awards, update_contract_awards,
    update_award_categories, get_awarding_agency)
from usaspending_api.etl.helpers import get_fiscal_quarter, get_previous_submission
from usaspending_api.etl.broker_etl_helpers import dictfetchall, PhonyCursor
from usaspending_api.etl.subaward_etl import load_subawards
from usaspending_api.references.helpers import canonicalize_location_dict

from usaspending_api.etl.helpers import update_model_description_fields

# This dictionary will hold a map of tas_id -> treasury_account to ensure we don't
# keep hitting the databroker DB for account data
TAS_ID_TO_ACCOUNT = {}

# Lists to store for update_awards and update_contract_awards
AWARD_UPDATE_ID_LIST = []
AWARD_CONTRACT_UPDATE_ID_LIST = []

awards_cache = caches['awards']
logger = logging.getLogger('console')


class Command(BaseCommand):
    """
    This command will load a single submission from the DATA Act broker. If
    we've already loaded the specified broker submisison, this command
    will remove the existing records before loading them again.
    """
    help = "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable must set so we can pull submission data from their db."

    def add_arguments(self, parser):
        parser.add_argument('submission_id', nargs=1, help='the data broker submission id to load', type=int)

        parser.add_argument(
            '--test',
            action='store_true',
            dest='test',
            default=False,
            help='Runs the submission loader in test mode and uses stored data rather than pulling from a database'
        )

    def handle(self, *args, **options):

        awards_cache.clear()

        # Grab the data broker database connections
        if not options['test']:
            try:
                db_conn = connections['data_broker']
                db_cursor = db_conn.cursor()
            except Exception as err:
                logger.critical('Could not connect to database. Is DATA_BROKER_DATABASE_URL set?')
                logger.critical(print(err))
                return
        else:
            db_cursor = PhonyCursor()

        # Grab the submission id
        submission_id = options['submission_id'][0]

        # Verify the ID exists in the database, and grab the data
        db_cursor.execute('SELECT * FROM submission WHERE submission_id = %s', [submission_id])
        submission_data = dictfetchall(db_cursor)

        if len(submission_data) == 0:
            logger.error('Could not find submission with id ' + str(submission_id))
            return
        elif len(submission_data) > 1:
            logger.error('Found multiple submissions with id ' + str(submission_id))
            return

        # We have a single submission, which is what we want
        submission_data = submission_data[0]
        broker_submission_id = submission_data['submission_id']
        del submission_data['submission_id']  # To avoid collisions with the newer PK system
        submission_attributes = get_submission_attributes(broker_submission_id, submission_data)

        # Move on, and grab file A data
        db_cursor.execute('SELECT * FROM appropriation WHERE submission_id = %s', [submission_id])
        appropriation_data = dictfetchall(db_cursor)
        logger.info('Acquired appropriation data for ' + str(submission_id) + ', there are ' + str(len(appropriation_data)) + ' rows.')
        load_file_a(submission_attributes, appropriation_data, db_cursor)

        # Let's get File B information
        prg_act_obj_cls_data = get_file_b(submission_attributes, db_cursor)
        logger.info('Acquired program activity object class data for ' + str(submission_id) + ', there are ' + str(len(prg_act_obj_cls_data)) + ' rows.')
        load_file_b(submission_attributes, prg_act_obj_cls_data, db_cursor)

        # File D2
        db_cursor.execute('SELECT * FROM award_financial_assistance WHERE submission_id = %s', [submission_id])
        award_financial_assistance_data = dictfetchall(db_cursor)
        logger.info('Acquired award financial assistance data for ' + str(submission_id) + ', there are ' + str(len(award_financial_assistance_data)) + ' rows.')
        load_file_d2(submission_attributes, award_financial_assistance_data, db_cursor)

        # File D1
        db_cursor.execute('SELECT * FROM award_procurement WHERE submission_id = %s', [submission_id])
        procurement_data = dictfetchall(db_cursor)
        logger.info('Acquired award procurement data for ' + str(submission_id) + ', there are ' + str(len(procurement_data)) + ' rows.')
        load_file_d1(submission_attributes, procurement_data, db_cursor)

        # Let's get File C information
        # Note: we load File C last, because the D1 and D2 files have the awarding
        # agency top tier (CGAC) and sub tier data needed to look up/create
        # the most specific possible corresponding award. When looking up/
        # creating awards for File C, we dont have sub-tier agency info, so
        # we'll do our best to match them to the more specific award records
        # already created by the D file load

        award_financial_query = 'SELECT * FROM award_financial WHERE submission_id = %s'
        if isinstance(db_cursor, PhonyCursor):  # spoofed data for test
            award_financial_frame = pd.DataFrame(db_cursor.db_responses[award_financial_query])
        else:  # real data
            award_financial_frame = pd.read_sql(award_financial_query % submission_id,
                                                connections['data_broker'])
        logger.info('Acquired award financial data for {}, there are {} rows.'
                    .format(submission_id, award_financial_frame.shape[0]))
        load_file_c(submission_attributes, db_cursor, award_financial_frame)

        # Once all the files have been processed, run any global
        # cleanup/post-load tasks.
        # 1. Load subawards
        try:
            load_subawards(submission_attributes, db_cursor)
        except:
            logger.warn("Error loading subawards for this submission")
        # 2. Update the descriptions TODO: If this is slow, add ID limiting as above
        update_model_description_fields()
        # 3. Update awards to reflect their latest associated txn info
        update_awards(tuple(AWARD_UPDATE_ID_LIST))
        # 4. Update contract-specific award fields to reflect latest txn info
        update_contract_awards(tuple(AWARD_CONTRACT_UPDATE_ID_LIST))
        # 5. Update the category variable
        update_award_categories(tuple(AWARD_UPDATE_ID_LIST))


def format_date(date_string, pattern='%Y%m%d'):
    try:
        return datetime.strptime(date_string, pattern)
    except TypeError:
        return None


def get_or_create_object_class(row_object_class, row_direct_reimbursable, logger):
    """Lookup an object class record.

        Args:
            row_object_class: object class from the broker
            row_by_direct_reimbursable_fun: direct/reimbursable flag from the broker
                (used only when the object_class is 3 digits instead of 4)
    """

    row = Bunch(object_class=row_object_class, by_direct_reimbursable_fun=row_direct_reimbursable)
    return get_or_create_object_class_rw(row, logger)


class Bunch:
    'Generic class to hold a group of attributes.'

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


def get_or_create_object_class_rw(row, logger):
    """Lookup an object class record.

       (As ``get_or_create_object_class``, but arguments are bunched into a ``row`` object.)

        Args:
            row.object_class: object class from the broker
            row.by_direct_reimbursable_fun: direct/reimbursable flag from the broker
                (used only when the object_class is 3 digits instead of 4)
    """

    if len(row.object_class) == 4:
        # this is a 4 digit object class, 1st digit = direct/reimbursable information
        direct_reimbursable = row.object_class[:1]
        object_class = row.object_class[1:]
    else:
        # the object class field is the 3 digit version, so grab direct/reimbursable
        # information from a separate field
        if row.by_direct_reimbursable_fun is None:
            direct_reimbursable = None
        elif row.by_direct_reimbursable_fun.lower() == 'd':
            direct_reimbursable = 1
        elif row.by_direct_reimbursable_fun.lower() == 'r':
            direct_reimbursable = 2
        else:
            direct_reimbursable = None
        object_class = row.object_class

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


def get_or_create_program_activity(row, submission_attributes):
    # We do it this way rather than .get_or_create because we do not want to
    # duplicate existing pk's with null values
    filters = {'program_activity_code': row['program_activity_code'],
               'budget_year': submission_attributes.reporting_fiscal_year,
               'responsible_agency_id': row['agency_identifier'],
               'main_account_code': row['main_account_code'], }
    prg_activity = RefProgramActivity.objects.filter(**filters).first()
    if prg_activity is None and row['program_activity_code'] is not None:
        # If the PA has a blank name, create it with the value in the row.
        # PA loader should overwrite the names for the unique PAs from the official
        # domain values list if the title needs updating, but for now grab it from the submission
        prg_activity = RefProgramActivity.objects.create(**filters, program_activity_name=row['program_activity_name'])
        logger.warning('Created missing program activity record for {}'.format(str(filters)))

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
        reverse -   Field names matching this regex should be reversed
                    (multiplied by -1) before saving.
        as_dict - If true, returns the model as a dict instead of saving or altering
    """
    field_map = kwargs.get('field_map')
    value_map = kwargs.get('value_map')
    save = kwargs.get('save')
    as_dict = kwargs.get('as_dict', False)
    reverse = kwargs.get('reverse')

    # Grab all the field names from the meta class of the model instance
    fields = [field.name for field in model_instance._meta.get_fields()]
    mod = model_instance

    if as_dict:
        mod = {}

    for field in fields:
        # Let's handle the data source field here for all objects
        if field is 'data_source':
            store_value(mod, field, 'DBR', reverse)
        broker_field = field
        # If our field is the 'long form' field, we need to get what it maps to
        # in the broker so we can map the data properly
        if broker_field in settings.LONG_TO_TERSE_LABELS:
            broker_field = settings.LONG_TO_TERSE_LABELS[broker_field]
        sts = False
        if value_map:
            if broker_field in value_map:
                store_value(mod, field, value_map[broker_field], reverse)
                sts = True
            elif field in value_map:
                store_value(mod, field, value_map[field], reverse)
                sts = True
        if field_map and not sts:
            if broker_field in field_map:
                store_value(mod, field, data[field_map[broker_field]], reverse)
                sts = True
            elif field in field_map:
                store_value(mod, field, data[field_map[field]], reverse)
                sts = True
        if broker_field in data and not sts:
            store_value(mod, field, data[broker_field], reverse)
        elif field in data and not sts:
            store_value(mod, field, data[field], reverse)

    if save:
        model_instance.save()
    if as_dict:
        return mod
    else:
        return model_instance


def get_submission_attributes(broker_submission_id, submission_data):
    """
    For a specified broker submission, return the existing corresponding usaspending
    submission record or create and return a new one.
    """
    # check if we already have an entry for this broker submission id; if not, create one
    submission_attributes, created = SubmissionAttributes.objects.get_or_create(
        broker_submission_id=broker_submission_id)

    if created:
        # this is the first time we're loading this broker submission
        logger.info('Creating broker submission id {}'.format(broker_submission_id))

    else:
        # we've already loaded this broker submission, so delete it before reloading
        # if there's another submission that references this one as a "previous submission"
        # do not proceed.
        # TODO: now that we're chaining submisisons together, get clarification on
        # what should happen when a submission in the middle of the chain is deleted
        downstream_submission = SubmissionAttributes.objects.filter(previous_submission=submission_attributes).first()
        if downstream_submission is not None:
            message = (
                'Broker submission {} (API submission id = {}) has a downstream submission (id={}) and '
                'cannot be deleted'.format(
                    broker_submission_id,
                    submission_attributes.submission_id,
                    downstream_submission.submission_id)
            )
            raise ValueError(message)

        logger.info('Broker bubmission id {} already exists. It will be deleted.'.format(broker_submission_id))
        call_command('rm_submission', broker_submission_id)

    # Find the previous submission for this CGAC and fiscal year (if there is one)
    previous_submission = get_previous_submission(
        submission_data['cgac_code'],
        submission_data['reporting_fiscal_year'],
        submission_data['reporting_fiscal_period'])

    # Update and save submission attributes
    field_map = {
        'reporting_period_start': 'reporting_start_date',
        'reporting_period_end': 'reporting_end_date',
        'quarter_format_flag': 'is_quarter_format',
    }

    # Create our value map - specific data to load
    value_map = {
        'broker_submission_id': broker_submission_id,
        'reporting_fiscal_quarter': get_fiscal_quarter(
            submission_data['reporting_fiscal_period']),
        'previous_submission': None if previous_submission is None else previous_submission,
        # pull in broker's last update date to use as certified date
        'certified_date': submission_data['updated_at'].date() if type(submission_data['updated_at']) == datetime else None,
    }

    return load_data_into_model(
        submission_attributes, submission_data,
        field_map=field_map, value_map=value_map, save=True)


def get_or_create_location(location_map, row, location_value_map={}):
    """
    Retrieve or create a location object

    Input parameters:
        - location_map: a dictionary with key = field name on the location model
            and value = corresponding field name on the current row of data
        - row: the row of data currently being loaded
    """
    row = canonicalize_location_dict(row)

    location_country = RefCountryCode.objects.filter(
        country_code=row[location_map.get('location_country_code')]).first()

    state_code = row.get(location_map.get('state_code'))
    if state_code is not None:
        # Remove . in state names (i.e. D.C.)
        location_value_map.update({'state_code': state_code.replace('.', '')})

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


def store_value(model_instance_or_dict, field, value, reverse=None):
    if value is None:
        return
    if reverse and reverse.search(field):
        value = -1 * Decimal(value)
    if isinstance(model_instance_or_dict, dict):
        model_instance_or_dict[field] = value
    else:
        setattr(model_instance_or_dict, field, value)


def load_file_a(submission_attributes, appropriation_data, db_cursor):
    """
    Process and load file A broker data (aka TAS balances,
    aka appropriation account balances).
    """
    reverse = re.compile('gross_outlay_amount_by_tas_cpe')
    # Create account objects
    for row in appropriation_data:

        # Check and see if there is an entry for this TAS
        treasury_account = get_treasury_appropriation_account_tas_lookup(
            row.get('tas_id'), db_cursor)
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

        load_data_into_model(appropriation_balances, row, field_map=field_map, value_map=value_map, save=True, reverse=reverse)

    AppropriationAccountBalances.populate_final_of_fy()

    # Insert File A quarterly numbers for this submission
    AppropriationAccountBalancesQuarterly.insert_quarterly_numbers(
        submission_attributes.submission_id)


def get_file_b(submission_attributes, db_cursor):
    """
    Get broker File B data for a specific submission.
    This function was added as a workaround for the fact that a few agencies
    (two, as of April, 2017: DOI and ACHP) submit multiple File B records
    for the same object class. These "dupes", come in as the same 4 digit object
    class code but with one of the direct reimbursable flags set to NULL.

    From our perspective, this is a duplicate, because we get our D/R info from
    the 1st digit of the object class when it's four digits.

    Thus, this function examines the File B data for a given submission. If
    it has the issue of "duplicate" object classes, it will squash the
    offending records together so that all financial totals are reporting
    as a single object class/program activity/TAS record as expected.

    If the broker validations change to prohibit this pattern in the data,
    this intervening function will no longer be necessary, we can go back to
    selecting * from the broker's File B data.

    Args:
        submission_attrbitues: submission object currently being loaded
        db_cursor: db connection info
    """
    submission_id = submission_attributes.broker_submission_id

    # does this file B have the dupe object class edge case?
    check_dupe_oc = (
        'SELECT count(*) '
        'FROM object_class_program_activity '
        'WHERE submission_id = %s '
        'AND length(object_class) = 4 '
        'GROUP BY tas_id, program_activity_code, object_class '
        'HAVING COUNT(*) > 1'
    )
    db_cursor.execute(check_dupe_oc, [submission_id])
    dupe_oc_count = len(dictfetchall(db_cursor))

    if dupe_oc_count == 0:
        # there are no object class duplicates, so proceed as usual
        db_cursor.execute('SELECT * FROM object_class_program_activity WHERE submission_id = %s', [submission_id])
    else:
        # file b contains at least one case of duplicate 4 digit object classes
        # for the same program activity/tas, so combine the records in question
        combine_dupe_oc = (
            'SELECT  '
            'submission_id, '
            'job_id, '
            'agency_identifier, '
            'allocation_transfer_agency, '
            'availability_type_code, '
            'beginning_period_of_availa, '
            'ending_period_of_availabil, '
            'main_account_code, '
            'RIGHT(object_class, 3) AS object_class, '
            'CASE WHEN length(object_class) = 4 AND LEFT(object_class, 1) = \'1\' THEN \'d\' WHEN length(object_class) = 4 AND LEFT(object_class, 1) = \'2\' THEN \'r\' ELSE by_direct_reimbursable_fun END AS by_direct_reimbursable_fun, '
            'tas, '
            'tas_id, '
            'program_activity_code, '
            'program_activity_name, '
            'sub_account_code, '
            'SUM(deobligations_recov_by_pro_cpe) AS deobligations_recov_by_pro_cpe, '
            'SUM(gross_outlay_amount_by_pro_cpe) AS gross_outlay_amount_by_pro_cpe, '
            'SUM(gross_outlay_amount_by_pro_fyb) AS gross_outlay_amount_by_pro_fyb, '
            'SUM(gross_outlays_delivered_or_cpe) AS gross_outlays_delivered_or_cpe, '
            'SUM(gross_outlays_delivered_or_fyb) AS gross_outlays_delivered_or_fyb, '
            'SUM(gross_outlays_undelivered_cpe) AS gross_outlays_undelivered_cpe, '
            'SUM(gross_outlays_undelivered_fyb) AS gross_outlays_undelivered_fyb, '
            'SUM(obligations_delivered_orde_cpe) AS obligations_delivered_orde_cpe, '
            'SUM(obligations_delivered_orde_fyb) AS obligations_delivered_orde_fyb, '
            'SUM(obligations_incurred_by_pr_cpe) AS obligations_incurred_by_pr_cpe, '
            'SUM(obligations_undelivered_or_cpe) AS obligations_undelivered_or_cpe, '
            'SUM(obligations_undelivered_or_fyb) AS obligations_undelivered_or_fyb, '
            'SUM(ussgl480100_undelivered_or_cpe) AS ussgl480100_undelivered_or_cpe, '
            'SUM(ussgl480100_undelivered_or_fyb) AS ussgl480100_undelivered_or_fyb, '
            'SUM(ussgl480200_undelivered_or_cpe) AS ussgl480200_undelivered_or_cpe, '
            'SUM(ussgl480200_undelivered_or_fyb) AS ussgl480200_undelivered_or_fyb, '
            'SUM(ussgl483100_undelivered_or_cpe) AS ussgl483100_undelivered_or_cpe, '
            'SUM(ussgl483200_undelivered_or_cpe) AS ussgl483200_undelivered_or_cpe, '
            'SUM(ussgl487100_downward_adjus_cpe) AS ussgl487100_downward_adjus_cpe, '
            'SUM(ussgl487200_downward_adjus_cpe) AS ussgl487200_downward_adjus_cpe, '
            'SUM(ussgl488100_upward_adjustm_cpe) AS ussgl488100_upward_adjustm_cpe, '
            'SUM(ussgl488200_upward_adjustm_cpe) AS ussgl488200_upward_adjustm_cpe, '
            'SUM(ussgl490100_delivered_orde_cpe) AS ussgl490100_delivered_orde_cpe, '
            'SUM(ussgl490100_delivered_orde_fyb) AS ussgl490100_delivered_orde_fyb, '
            'SUM(ussgl490200_delivered_orde_cpe) AS ussgl490200_delivered_orde_cpe, '
            'SUM(ussgl490800_authority_outl_cpe) AS ussgl490800_authority_outl_cpe, '
            'SUM(ussgl490800_authority_outl_fyb) AS ussgl490800_authority_outl_fyb, '
            'SUM(ussgl493100_delivered_orde_cpe) AS ussgl493100_delivered_orde_cpe, '
            'SUM(ussgl497100_downward_adjus_cpe) AS ussgl497100_downward_adjus_cpe, '
            'SUM(ussgl497200_downward_adjus_cpe) AS ussgl497200_downward_adjus_cpe, '
            'SUM(ussgl498100_upward_adjustm_cpe) AS ussgl498100_upward_adjustm_cpe, '
            'SUM(ussgl498200_upward_adjustm_cpe) AS ussgl498200_upward_adjustm_cpe '
            'FROM object_class_program_activity '
            'WHERE submission_id = %s '
            'GROUP BY  '
            'submission_id, '
            'job_id, '
            'agency_identifier, '
            'allocation_transfer_agency, '
            'availability_type_code, '
            'beginning_period_of_availa, '
            'ending_period_of_availabil, '
            'main_account_code, '
            'RIGHT(object_class, 3), '
            'CASE WHEN length(object_class) = 4 AND LEFT(object_class, 1) = \'1\' THEN \'d\' WHEN length(object_class) = 4 AND LEFT(object_class, 1) = \'2\' THEN \'r\' ELSE by_direct_reimbursable_fun END, '
            'program_activity_code, '
            'program_activity_name, '
            'sub_account_code, '
            'tas, '
            'tas_id'
        )
        logger.info(
            'Found {} duplicated File B 4 digit object codes in submission {}. '
            'Aggregating financial values.'.format(dupe_oc_count, submission_id))
        # we have at least one instance of duplicated 4 digit object classes so
        # aggregate the financial values togther
        db_cursor.execute(combine_dupe_oc, [submission_id])

    data = dictfetchall(db_cursor)
    return data


def load_file_b(submission_attributes, prg_act_obj_cls_data, db_cursor):
    """
    Process and load file B broker data (aka TAS balances by program
    activity and object class).
    """
    reverse = re.compile(r'(_(cpe|fyb)$)|^transaction_obligated_amount$')
    test_counter = 0
    for row in prg_act_obj_cls_data:
        test_counter += 1
        account_balances = None
        try:
            # Check and see if there is an entry for this TAS
            treasury_account = get_treasury_appropriation_account_tas_lookup(row.get('tas_id'), db_cursor)
            if treasury_account is None:
                raise Exception('Could not find appropriation account for TAS: ' + row['tas'])
        except:
            continue

        # get the corresponding account balances row (aka "File A" record)
        account_balances = AppropriationAccountBalances.objects.get(
            treasury_account_identifier=treasury_account,
            submission_id=submission_attributes.submission_id
        )

        financial_by_prg_act_obj_cls = FinancialAccountsByProgramActivityObjectClass()

        value_map = {
            'submission': submission_attributes,
            'reporting_period_start': submission_attributes.reporting_period_start,
            'reporting_period_end': submission_attributes.reporting_period_end,
            'treasury_account': treasury_account,
            'appropriation_account_balances': account_balances,
            'object_class': get_or_create_object_class(row['object_class'], row['by_direct_reimbursable_fun'], logger),
            'program_activity': get_or_create_program_activity(row, submission_attributes)
        }

        load_data_into_model(financial_by_prg_act_obj_cls, row, value_map=value_map, save=True, reverse=reverse)

    # Insert File B quarterly numbers for this submission
    TasProgramActivityObjectClassQuarterly.insert_quarterly_numbers(
        submission_attributes.submission_id)

    FinancialAccountsByProgramActivityObjectClass.populate_final_of_fy()


def load_file_c(submission_attributes, db_cursor, award_financial_frame):
    """
    Process and load file C broker data.
    Note: this should run AFTER the D1 and D2 files are loaded because we try
    to join to those records to retrieve some additional information
    about the awarding sub-tier agency.
    """
    # this matches the file b reverse directive, but am repeating it here
    # to ensure that we don't overwrite it as we change up the order of
    # file loading
    reverse = re.compile(r'(_(cpe|fyb)$)|^transaction_obligated_amount$')

    award_financial_frame['txn'] = award_financial_frame.apply(get_award_financial_transaction, axis=1)
    award_financial_frame['awarding_agency'] = award_financial_frame.apply(get_awarding_agency, axis=1)
    award_financial_frame['object_class'] = award_financial_frame.apply(get_or_create_object_class_rw, axis=1, logger=logger)
    award_financial_frame['program_activity'] = award_financial_frame.apply(get_or_create_program_activity, axis=1, submission_attributes=submission_attributes)

    # for row in award_financial_data:
    for row in award_financial_frame.replace({np.nan: None}).to_dict(orient='records'):
        # Check and see if there is an entry for this TAS
        treasury_account = get_treasury_appropriation_account_tas_lookup(
            row.get('tas_id'), db_cursor)
        if treasury_account is None:
            raise Exception('Could not find appropriation account for TAS: ' + row['tas'])

        # Find a matching transaction record, so we can use its
        # subtier agency information to match to (or create) an Award record

        # Find the award that this award transaction belongs to. If it doesn't exist, create it.
        created, award = Award.get_or_create_summary_award(
            awarding_agency=row['awarding_agency'],
            piid=row.get('piid'),
            fain=row.get('fain'),
            uri=row.get('uri'),
            parent_award_id=row.get('parent_award_id'),
            use_cache=False)

        award_financial_data = FinancialAccountsByAwards()

        value_map = {
            'award': award,
            'submission': submission_attributes,
            'reporting_period_start': submission_attributes.reporting_period_start,
            'reporting_period_end': submission_attributes.reporting_period_end,
            'treasury_account': treasury_account,
            'object_class': row.get('object_class'),
            'program_activity': row.get('program_activity'),
        }

        # Still using the cpe|fyb regex compiled above for reverse
        afd = load_data_into_model(award_financial_data, row, value_map=value_map, save=True, reverse=reverse)

    awards_cache.clear()


def load_file_d1(submission_attributes, procurement_data, db_cursor):
    """
    Process and load file D1 broker data (contract award txns).
    """
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

    place_of_performance_value_map = {
        "place_of_performance_flag": True
    }

    legal_entity_location_value_map = {
        "recipient_flag": True
    }

    contract_field_map = {
        "type": "contract_award_type",
        "description": "award_description"
    }

    for row in procurement_data:
        legal_entity_location, created = get_or_create_location(legal_entity_location_field_map, row, legal_entity_location_value_map)

        # Create the legal entity if it doesn't exist
        legal_entity, created = LegalEntity.get_or_create_by_duns(duns=row['awardee_or_recipient_uniqu'])
        if created:
            legal_entity_value_map = {
                "location": legal_entity_location,
            }
            legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

        # Create the place of performance location
        pop_location, created = get_or_create_location(
            place_of_performance_field_map, row, place_of_performance_value_map)

        # If awarding toptier agency code (aka CGAC) is not supplied on the D1 record,
        # use the sub tier code to look it up. This code assumes that all incoming
        # records will supply an awarding subtier agency code
        if row['awarding_agency_code'] is None or len(row['awarding_agency_code'].strip()) < 1:
            row['awarding_agency_code'] = Agency.get_by_subtier(
                row["awarding_sub_tier_agency_c"]).toptier_agency.cgac_code
        # If funding toptier agency code (aka CGAC) is empty, try using the sub
        # tier funding code to look it up. Unlike the awarding agency, we can't
        # assume that the funding agency subtier code will always be present.
        if row['funding_agency_code'] is None or len(row['funding_agency_code'].strip()) < 1:
            funding_agency = Agency.get_by_subtier(row["funding_sub_tier_agency_co"])
            row['funding_agency_code'] = (
                funding_agency.toptier_agency.cgac_code if funding_agency is not None
                else None)

        # Find the award that this award transaction belongs to. If it doesn't exist, create it.
        awarding_agency = Agency.get_by_toptier_subtier(
            row['awarding_agency_code'],
            row["awarding_sub_tier_agency_c"]
        )
        created, award = Award.get_or_create_summary_award(
            awarding_agency=awarding_agency,
            piid=row.get('piid'),
            fain=row.get('fain'),
            uri=row.get('uri'),
            parent_award_id=row.get('parent_award_id'))
        award.save()

        AWARD_UPDATE_ID_LIST.append(award.id)
        AWARD_CONTRACT_UPDATE_ID_LIST.append(award.id)

        parent_txn_value_map = {
            "award": award,
            "awarding_agency": awarding_agency,
            "funding_agency": Agency.get_by_toptier_subtier(row['funding_agency_code'],
                                                            row["funding_sub_tier_agency_co"]),
            "recipient": legal_entity,
            "place_of_performance": pop_location,
            'submission': submission_attributes,
            "period_of_performance_start_date": format_date(row['period_of_performance_star']),
            "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
            "action_date": format_date(row['action_date']),
        }

        transaction_dict = load_data_into_model(
            Transaction(),  # thrown away
            row,
            field_map=contract_field_map,
            value_map=parent_txn_value_map,
            as_dict=True)

        transaction = Transaction.get_or_create_transaction(**transaction_dict)
        transaction.save()

        contract_value_map = {
            'submission': submission_attributes,
            'reporting_period_start': submission_attributes.reporting_period_start,
            'reporting_period_end': submission_attributes.reporting_period_end,
            "period_of_performance_potential_end_date": format_date(row['period_of_perf_potential_e'])
        }

        contract_instance = load_data_into_model(
            TransactionContract(),  # thrown away
            row,
            field_map=contract_field_map,
            value_map=contract_value_map,
            as_dict=True)

        transaction_contract = TransactionContract(transaction=transaction, **contract_instance)
        transaction_contract.save()


def load_file_d2(submission_attributes, award_financial_assistance_data, db_cursor):
    """
    Process and load file D2 broker data (financial assistance award txns).
    """
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
        legal_entity, created = LegalEntity.get_or_create_by_duns(duns=row['awardee_or_recipient_uniqu'])
        if created:
            legal_entity_value_map = {
                "location": legal_entity_location,
            }
            legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

        # Create the place of performance location
        pop_location, created = get_or_create_location(place_of_performance_field_map, row, place_of_performance_value_map)

        # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
        # use the sub tier code to look it up. This code assumes that all incoming
        # records will supply an awarding subtier agency code
        if row['awarding_agency_code'] is None or len(row['awarding_agency_code'].strip()) < 1:
            row['awarding_agency_code'] = Agency.get_by_subtier(
                row["awarding_sub_tier_agency_c"]).toptier_agency.cgac_code
        # If funding toptier agency code (aka CGAC) is empty, try using the sub
        # tier funding code to look it up. Unlike the awarding agency, we can't
        # assume that the funding agency subtier code will always be present.
        if row['funding_agency_code'] is None or len(row['funding_agency_code'].strip()) < 1:
            funding_agency = Agency.get_by_subtier(row["funding_sub_tier_agency_co"])
            row['funding_agency_code'] = (
                funding_agency.toptier_agency.cgac_code if funding_agency is not None
                else None)

        # Find the award that this award transaction belongs to. If it doesn't exist, create it.
        awarding_agency = Agency.get_by_toptier_subtier(
            row['awarding_agency_code'],
            row["awarding_sub_tier_agency_c"]
        )
        created, award = Award.get_or_create_summary_award(
            awarding_agency=awarding_agency,
            piid=row.get('piid'),
            fain=row.get('fain'),
            uri=row.get('uri'),
            parent_award_id=row.get('parent_award_id'))
        award.save()

        AWARD_UPDATE_ID_LIST.append(award.id)

        parent_txn_value_map = {
            "award": award,
            "awarding_agency": awarding_agency,
            "funding_agency": Agency.get_by_toptier_subtier(row['funding_agency_code'],
                                                            row["funding_sub_tier_agency_co"]),
            "recipient": legal_entity,
            "place_of_performance": pop_location,
            'submission': submission_attributes,
            "period_of_performance_start_date": format_date(row['period_of_performance_star']),
            "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
            "action_date": format_date(row['action_date']),
        }

        transaction_dict = load_data_into_model(
            Transaction(),  # thrown away
            row,
            field_map=fad_field_map,
            value_map=parent_txn_value_map,
            as_dict=True)

        transaction = Transaction.get_or_create_transaction(**transaction_dict)
        transaction.save()

        fad_value_map = {
            "submission": submission_attributes,
            "cfda": Cfda.objects.filter(program_number=row['cfda_number']).first(),
            'reporting_period_start': submission_attributes.reporting_period_start,
            'reporting_period_end': submission_attributes.reporting_period_end,
            "period_of_performance_start_date": format_date(row['period_of_performance_star']),
            "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
        }

        financial_assistance_data = load_data_into_model(
            TransactionAssistance(),  # thrown away
            row,
            field_map=fad_field_map,
            value_map=fad_value_map,
            as_dict=True)

        transaction_assistance = TransactionAssistance.get_or_create(transaction=transaction, **financial_assistance_data)
        transaction_assistance.save()
