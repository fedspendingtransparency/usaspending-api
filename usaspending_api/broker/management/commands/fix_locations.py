"""
Idempotently re-runs location creation / matching code from broker FPDS data
"""

import logging
import time
from datetime import datetime
from functools import wraps
from itertools import count, groupby

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction as db_transaction
from django.db import connection, connections

from usaspending_api.awards.models import (TransactionFABS, TransactionFPDS,
                                           TransactionNormalized)
from usaspending_api.broker.management.commands import (fabs_nightly_loader,
                                                        fpds_nightly_loader)
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.load_base import (format_date,
                                                      load_data_into_model)
from usaspending_api.references.helpers import canonicalize_location_dict
from usaspending_api.references.models import Location, RefCountryCode

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

BATCH_DOWNLOAD_SIZE = 10000


def log_time(func):
    """Decorator to log elapsed time for a function"""

    @wraps(func)
    def log_time_wrapper(*args, **kwargs):
        logger.info('Begin {}'.format(func.__name__))
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.info('Finished {} in {} seconds'.format(func.__name__, end - start))
        return result

    return log_time_wrapper


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument('-c', '--contracts', action='store_true', help="Fix locations for contracts")
        parser.add_argument('-a', '--assistance', action='store_true', help="Fix locations for assistance")
        parser.add_argument('--limit', type=int, default=100000000, help="Max # of awards to fix")
        parser.add_argument('--fiscal-year', type=int, help="Fix only this FY")
        parser.add_argument(
            '--only-final-transactions',
            action='store_true',
            help="For speed, fix only final transaction of each award")

    @log_time
    def handle(self, *args, **options):

        if options.get('contracts'):
            fixer = FPDSLocationFixer(options)
        elif options.get('assistance'):
            fixer = FABSLocationFixer(options)
        else:
            raise CommandError('Please specify either --contracts or --assistance')

        fixer.fix_all_rows()


class LocationFixer:

    def __init__(self, options):
        pass

    def loc_from_txn(self, data):

        data['data_source'] = 'DBR'

        if ((not data['location_country_code']) and (data['performance_code'] != '00FORGN')):
            data['location_country_code'] = 'USA'

        # if present, country code determines name
        country_code_obj = RefCountryCode.objects.filter(country_code=data['location_country_code']).first()
        if country_code_obj:
            data['location_country_code'] = country_code_obj
            data['country_name'] = country_code_obj.country_name
        else:
            # no country found
            data['location_country_code'] = None
            data['country_name'] = None

        if data.get('state_code'):
            data['state_code'] = data['state_code'].replace('.', '')
        if data['location_country_code'].country_code != 'USA':
            data['state_code'] = None
            data['state_name'] = None

        loc_instance = Location(**data)
        loc_instance.load_country_data()
        loc_instance.load_city_county_data()
        loc_instance.fill_missing_state_data()
        # fix zip code!

        return loc_instance




class FABSLocationFixer(LocationFixer):

    @log_time
    def fix_all_rows(self):

        overall_start = time.time()
        to_create = []

        logger.info('Fixing Location.state, FABS, places of performance')

        for (i, inst) in enumerate(TransactionFABS.objects \
            .filter(transaction__place_of_performance__state_code__isnull=True) \
            .filter(transaction__place_of_performance__location_country_code='USA')):

            row = {k: getattr(inst, v) for (k, v) in self.pop_field_map.items()}
            row['place_of_performance_flag'] = True
            to_create.append(self.loc_from_txn(row))

            if not i % 1000:
                Location.objects.bulk_create(to_create)
                to_create = []
                elapsed = time.time()
                logger.info('Total: {} rows, {} s'.format(i, elapsed - overall_start))

        elapsed = time.time()
        logger.info('Total rows: {}, {} s'.format(i, elapsed - overall_start))
        logger.info('Fixing state info, FABS, recipients')

        to_create = []
        import ipdb; ipdb.set_trace()
        for (i, inst) in enumerate(TransactionFABS.objects \
            .filter(transaction__recipient__location__state_code__isnull=True) \
            .filter(transaction__recipient__location__location_country_code='USA')):

            row = {k: getattr(inst, v) for (k, v) in self.le_field_map.items()}
            row['recipient_flag'] = True
            to_create.append(self.loc_from_txn(row))

            if not i % 1000:
                Location.objects.bulk_create(to_create)
                to_create = []
                elapsed = time.time()
                logger.info('Total: {} rows, {} s'.format(i, elapsed - overall_start))


    pop_field_map = {
        "city_name": "place_of_performance_city",
        "performance_code": "place_of_performance_code",
        "congressional_code": "place_of_performance_congr",
        "county_name": "place_of_perform_county_na",
        # "county_code": "place_of_perform_county_co",
        "foreign_location_description": "place_of_performance_forei",
        "state_name": "place_of_perform_state_nam",
        "zip4": "place_of_performance_zip4a",
        "location_country_code": "place_of_perform_country_c",
        "country_name": "place_of_perform_country_n"
    }

    le_field_map = {
        "address_line1": "legal_entity_address_line1",
        "address_line2": "legal_entity_address_line2",
        "address_line3": "legal_entity_address_line3",
        "city_name": "legal_entity_city_name",
        "city_code": "legal_entity_city_code",
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
        "location_country_code": "legal_entity_country_code",
        "country_name": "legal_entity_country_name"
    }


class FPDSLocationFixer(LocationFixer):
    def broker_ids_for_bad_pops(self):
        """Get detached_award_procurement_ids for places of performance with blank state code"""

        result = TransactionFPDS.objects.only('detached_award_procurement_id'). \
            filter(transaction__place_of_performance__state_code__isnull=True). \
            filter(transaction__place_of_performance__location_country_code__country_code='USA')

        return result

    def get_broker_data(self, broker_ids):

        id_list = ','.join(b_id.detached_award_procurement_id for b_id in broker_ids)
        query = """SELECT {}
            FROM detached_award_procurement
            WHERE detached_award_procurement_id IN ({})""". \
            format(self.BROKER_COLUMNS, id_list)

        self.broker_cursor.execute(query)
        return dictfetchall(self.broker_cursor)

    def txns_from_broker_row(self, row):
        return TransactionFPDS.objects.filter(detached_award_procurement_id=row['detached_award_procurement_id'])

    def broker_ids_for_bad_recipients(self):
        """Get detached_award_procurement_ids for recipient locations with blank state code"""

        result = TransactionFPDS.objects.only('detached_award_procurement_id'). \
            filter(transaction__recipient__location__state_code__isnull=True). \
            filter(transaction__recipient__location__location_country_code__country_code='USA')

        return result

    BROKER_COLUMNS = """detached_award_procurement_id, place_of_perform_country_c,
      place_of_perform_city_name, place_of_performance_zip4a,
      place_of_performance_congr, place_of_perform_county_na,
      place_of_performance_state,
      legal_entity_state_code,
      legal_entity_city_name ,
      legal_entity_address_line1 ,
      legal_entity_address_line2 ,
      legal_entity_address_line3 ,
      legal_entity_zip4 ,
      legal_entity_congressional ,
      legal_entity_country_code
      """

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
