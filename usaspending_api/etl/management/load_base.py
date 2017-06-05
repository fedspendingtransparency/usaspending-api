from datetime import datetime
from decimal import Decimal
import logging

from django.conf import settings
from django.core.exceptions import MultipleObjectsReturned
from django.core.management.base import BaseCommand
from django.db import connections
from django.core.cache import caches

from usaspending_api.awards.models import (
    Award,
    TransactionContract, Transaction)
from usaspending_api.references.models import (
    Agency, LegalEntity, Location, RefCountryCode, )
from usaspending_api.etl.award_helpers import (
    update_awards, update_contract_awards,
    update_award_categories, )
from usaspending_api.etl.broker_etl_helpers import PhonyCursor
from usaspending_api.references.helpers import canonicalize_location_dict

from usaspending_api.etl.helpers import update_model_description_fields

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

        self.handle_loading(db_cursor=db_cursor, *args, **options)
        self.post_load_cleanup()

    def post_load_cleanup(self):
        "Global cleanup/post-load tasks not specific to a submission"

        # 1. Update the descriptions TODO: If this is slow, add ID limiting as above
        update_model_description_fields()
        # 2. Update awards to reflect their latest associated txn info
        update_awards(tuple(AWARD_UPDATE_ID_LIST))
        # 3. Update contract-specific award fields to reflect latest txn info
        update_contract_awards(tuple(AWARD_CONTRACT_UPDATE_ID_LIST))
        # 4. Update the category variable
        update_award_categories(tuple(AWARD_UPDATE_ID_LIST))


def load_file_d1(submission_attributes, procurement_data, db_cursor, date_pattern='%Y%m%d'):
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
            "period_of_performance_start_date": format_date(row['period_of_performance_star'], date_pattern),
            "period_of_performance_current_end_date": format_date(row['period_of_performance_curr'], date_pattern),
            "action_date": format_date(row['action_date'], date_pattern),
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
            "period_of_performance_potential_end_date": format_date(row['period_of_perf_potential_e'], date_pattern)
        }

        contract_instance = load_data_into_model(
            TransactionContract(),  # thrown away
            row,
            field_map=contract_field_map,
            value_map=contract_value_map,
            as_dict=True)

        transaction_contract = TransactionContract(transaction=transaction, **contract_instance)
        transaction_contract.save()


def format_date(date_string, pattern='%Y%m%d'):
    try:
        return datetime.strptime(date_string, pattern)
    except TypeError:
        return None


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
