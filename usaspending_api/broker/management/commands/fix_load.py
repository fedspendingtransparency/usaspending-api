import logging
import timeit
from functools import wraps
from datetime import datetime
import psycopg2.extras
from django.core.management.base import BaseCommand
from django.db import connections, connection, transaction as db_transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFPDS, TransactionNormalized
from usaspending_api.etl.management.load_base import load_data_into_model, format_date
from usaspending_api.references.helpers import canonicalize_location_dict
from usaspending_api.references.models import RefCountryCode, Location

BATCH_SIZE = 100000

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

pop_field_map = {
    # not sure place_of_performance_locat maps exactly to city name
    # "city_name": "place_of_performance_locat", # location id doesn't mean it's a city. Can't use this mapping
    "congressional_code": "place_of_performance_congr",
    "state_code": "place_of_performance_state",
    "zip4": "place_of_performance_zip4a",
    "location_country_code": "place_of_perform_country_c",
    "city_name": "place_of_perform_city_name",  # NEW
    "county_name": "place_of_perform_county_na",  # NEW
}

le_field_map = {
    "address_line1": "legal_entity_address_line1",
    "address_line2": "legal_entity_address_line2",
    "address_line3": "legal_entity_address_line3",
    "location_country_code": "legal_entity_country_code",
    "city_name": "legal_entity_city_name",
    "congressional_code": "legal_entity_congressional",
    "state_code": "legal_entity_state_code",
    "zip4": "legal_entity_zip4"
}


def log_time(func):
    """Decorator to log elapsed time for a function"""

    @wraps(func) 
    def log_time_wrapper(*args, **kwargs):
        logger.info('Begin {}'.format(func.__name__))
        start = timeit.default_timer()
        result = func(*args, **kwargs)
        end = timeit.default_timer()
        logger.info('Finished {} in {} seconds'.format(func.__name__, end - start))
        return result

    return log_time_wrapper


class Command(BaseCommand):
    help = "Update historical transaction data for a fiscal year from the Broker."

    country_code_map = {}
    subtier_agency_map = {}
    subtier_to_agency_map = {}
    toptier_agency_map = {}
    agency_no_sub_map = {}
    agency_sub_only_map = {}
    agency_toptier_map = {}
    award_map = {}
    parent_award_map = {}
    le_map = {}

    def set_lookup_maps(self):
        self.country_code_map = {country.country_code: country
                                 for country in RefCountryCode.objects.all()}

    def get_fpds_data(self, db_cursor, piid):
        """For a PIID, get FPDS data from broker"""

        query = 'SELECT * FROM detached_award_procurement WHERE piid = %s'
        db_cursor.execute(query, [piid, ])
        return dictfetchall(db_cursor)

    @log_time
    def piids_for_blank_pops(self, limit):

        return TransactionFPDS.objects.only('piid'). \
            filter(transaction__place_of_performance__state_code__isnull=True). \
            filter(transaction__place_of_performance__location_country_code__country_code='USA') \
            [:limit]

    @log_time
    def piids_for_blank_recipients(self, limit):

        return TransactionFPDS.objects.only('piid'). \
            filter(transaction__recipient__location__state_code__isnull=True). \
            filter(transaction__recipient__location__location_country_code__country_code='USA') \
            [:limit]

    def location_from_fpds_row(self, fpds_broker_row, field_map, value_map):
        row = canonicalize_location_dict(fpds_broker_row)

        # THIS ASSUMPTION DOES NOT HOLD FOR FPDS SINCE IT DOES NOT HAVE A PLACE OF PERFORMANCE CODE
        # We can assume that if the country code is blank and the place of performance code is NOT '00FORGN', then
        # the country code is USA
        # if pop_flag and not country_code and pop_code != '00FORGN':
        #     row[field_map.get('location_country_code')] = 'USA'

        # Get country code obj
        location_country_code = self.country_code_map.get(row[field_map.get(
            'location_country_code')])

        # Fix state code periods
        state_code = row.get(field_map.get('state_code'))
        if state_code is not None:
            value_map.update({'state_code': state_code.replace('.', '')})

        if location_country_code:
            value_map.update({
                'location_country_code': location_country_code,
                'country_name': location_country_code.country_name
            })

            if location_country_code.country_code != 'USA':  # The missing country_code may be the problem
                value_map.update({'state_code': None, 'state_name': None})
        else:
            # no country found for this code
            value_map.update({
                'location_country_code': None,
                'country_name': None
            })

        location_instance_data = load_data_into_model(Location(),
                                                      row,
                                                      value_map=value_map,
                                                      field_map=field_map,
                                                      as_dict=True)

        # get_or_create can't be used b/c we have many duplicates
        loc = Location.objects.filter(**location_instance_data).order_by('create_date').first()
        if loc:
            created = False 
        else:
            loc = Location(**location_instance_data)
            loc.save()
            created = True
        return (loc, created)

    @log_time
    def fix_places_of_performance(self, fpds_broker_data):

        start_time = datetime.now()
        value_map = {"place_of_performance_flag": True}
        create_count = change_count = 0
        for index, row in enumerate(fpds_broker_data, 1):
            (loc, created) = self.location_from_fpds_row(row,
                                              field_map=pop_field_map,
                                              value_map=dict(value_map))
            create_count += int(created)
            for txn in TransactionFPDS.objects.filter(
                    detached_award_procurement_id=row[
                        'detached_award_procurement_id']).all():
                if txn.transaction.place_of_performance != loc:
                    change_count += 1
                    txn.transaction.place_of_performance = loc
                    txn.transaction.save()
                if txn.transaction_id == txn.transaction.award.latest_transaction_id:
                    txn.transaction.award.place_of_performance = loc
                    txn.transaction.award.save()
        return (change_count, create_count)

    @log_time
    def fix_recipient_locations(self, fpds_broker_data):

        value_map = {"recipient_flag": True}
        create_count = change_count = 0
        for index, row in enumerate(fpds_broker_data, 1):
            for txn in TransactionFPDS.objects.filter(
                    detached_award_procurement_id=row[
                        'detached_award_procurement_id']).all():
                recip = txn.transaction.recipient
                last_txn = recip.transactionnormalized_set.order_by(
                    '-action_date')[0]
                if txn.transaction == last_txn:
                    (loc, created) = self.location_from_fpds_row(
                        row,
                        field_map=le_field_map,
                        value_map=dict(value_map))
                    create_count += int(created)
                    if recip.location != loc:
                        change_count += 1
                        recip.location = loc
                        recip.save()
        return (change_count, create_count)

    def log_and_execute(self, description, func, *args, **kwargs):
        logger.info('Begin {}'.format(description))
        start = timeit.default_timer()
        result = func(*args, **kwargs)
        end = timeit.default_timer()
        logger.info('Finished {} in {} seconds'.format(description, (end - start)))

    
    def fix_locations(self, db_cursor, piid):

        logger.info('Fixing locations for PIID {}'.format(piid))
        overall_start = timeit.default_timer()

        fpds_broker_data = self.get_fpds_data(db_cursor=db_cursor, piid=piid)

        logger.info('Fixing POP Location data...')
        start = timeit.default_timer()
        self.fix_places_of_performance(fpds_broker_data=fpds_broker_data)
        end = timeit.default_timer()
        logger.info('Fixed POP locations in {} seconds'.format(end - start))

        logger.info('Fixing LE Location data...')
        start = timeit.default_timer()
        self.fix_recipient_locations(fpds_broker_data=fpds_broker_data)
        end = timeit.default_timer()
        logger.info('Fixed LE location in {} seconds'.format(end - start))

        end = timeit.default_timer()
        logger.info('PIID {} fixed in {} seconds'.format(piid, end - overall_start))


    def add_arguments(self, parser):

        parser.add_argument('--piid',
                            help="PIID of award to reload")
        parser.add_argument('--limit', type=int, default=1000, 
                            help="Max # of awards to fix")

    @db_transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting FPDS data fix...')
        start = timeit.default_timer()

        self.set_lookup_maps()
        db_cursor = connections['data_broker'].cursor()
        piid = options.get('piid')
        limit = options.get('limit')
        if piid:
            fpds_broker_data = self.get_fpds_data(db_cursor=db_cursor, piid=piid)
            (changed, created) = self.fix_places_of_performance(fpds_broker_data=fpds_broker_data)
            logger.info('Created {} locations, changed POP for {} transactions'.format(created, changed))
            (changed, created) = self.fix_recipient_locations(fpds_broker_data=fpds_broker_data)
            logger.info('Created {} locations, changed location for {} recipients'.format(created, changed))
            created = pop_created + recip_created
        else:
            for (n, txn) in enumerate(self.piids_for_blank_pops(limit=limit)):
                logger.info('Begin piid {}'.format(txn.piid))
                fpds_broker_data = self.get_fpds_data(db_cursor=db_cursor, piid=txn.piid)
                (changed, created) = self.fix_places_of_performance(fpds_broker_data=fpds_broker_data)
                logger.info('Created {} locations, changed POP for {} transactions'.format(created, changed))
                logger.info('Checked {} POPs in {} seconds'.format(n, timeit.default_timer() - start))
            for (n, txn) in enumerate(self.piids_for_blank_recips(limit=limit)):
                logger.info('Begin piid {}'.format(txn.piid))
                fpds_broker_data = self.get_fpds_data(db_cursor=db_cursor, piid=txn.piid)
                (changed, created) = self.fix_recipient_locations(fpds_broker_data=fpds_broker_data)
                logger.info('Created {} locations, changed location for {} recipients'.format(created, changed))
                logger.info('Checked {} recipient locations in {} seconds'.format(n, timeit.default_timer() - start))
        
