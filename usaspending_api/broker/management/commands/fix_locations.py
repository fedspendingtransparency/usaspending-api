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
        parser.add_argument('--piid', help="Reload single award, this PIID")
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


def chunks(source_iterable, size=BATCH_DOWNLOAD_SIZE):
    """Given an iterable, yield smaller iterables of size `size`"""
    c = count()
    for _, g in groupby(source_iterable, lambda _: next(c) // size):
        yield g


class LocationFixer:
    def __init__(self, options):

        self.broker_cursor = connections['data_broker'].cursor()
        self.options = options
        self.set_lookup_maps()

    def set_lookup_maps(self):
        self.country_code_map = {country.country_code: country for country in RefCountryCode.objects.all()}

    @log_time
    def fix_all_rows(self):

        broker_ids = self.broker_ids_for_bad_pops()
        broker_ids = self.apply_option_filters(broker_ids, self.options)
        for some_broker_ids in chunks(broker_ids):
            logger.info('loading (up to) {} broker rows'.format(BATCH_DOWNLOAD_SIZE))
            broker_rows = self.get_broker_data(some_broker_ids)
            self.fix_batch_of_places_of_performance(broker_rows)

        broker_ids = self.broker_ids_for_bad_recipients()
        broker_ids = self.apply_option_filters(broker_ids, self.options)
        for some_broker_ids in chunks(broker_ids):
            logger.info('loading (up to) {} broker rows'.format(BATCH_DOWNLOAD_SIZE))
            broker_rows = self.get_broker_data(some_broker_ids)
            self.fix_batch_of_recipients(broker_rows)

    def apply_option_filters(self, query, options):

        fiscal_year = options.get('fiscal_year')
        if fiscal_year:
            query = query.filter(transaction__fiscal_year=fiscal_year)

        only_final_transactions = options.get('only_final_transactions')
        if only_final_transactions:
            # for speed, only fix final transaction of each award,
            # which are the only ones now surfaced on website
            query = query.filter(transaction__latest_for_award__isnull=False)

        return query.all()[:options['limit']]

    @log_time
    def fix_batch_of_places_of_performance(self, broker_data):

        value_map = {"place_of_performance_flag": True}
        create_count = change_count = 0
        for row in broker_data:
            (loc, created) = self.location_from_broker_row(row, field_map=self.pop_field_map, value_map=dict(value_map))
            create_count += int(created)
            for txn in self.txns_from_broker_row(row).all():
                if txn.transaction.place_of_performance != loc:
                    change_count += 1
                    txn.transaction.place_of_performance = loc
                    txn.transaction.save()
                if txn.transaction_id == txn.transaction.award.latest_transaction_id:
                    txn.transaction.award.place_of_performance = loc
                    txn.transaction.award.save()
        return (change_count, create_count)

    @log_time
    def fix_batch_of_recipients(self, broker_data):

        value_map = {"recipient_flag": True}
        create_count = change_count = 0
        for row in broker_data:
            for txn in self.txns_from_broker_row(row).all():
                recip = txn.transaction.recipient
                last_txn = recip.transactionnormalized_set.order_by('-action_date')[0]
                if txn.transaction == last_txn:
                    (loc, created) = self.location_from_broker_row(
                        row, field_map=self.le_field_map, value_map=dict(value_map))
                    create_count += int(created)
                    if recip.location != loc:
                        change_count += 1
                        recip.location = loc
                        recip.save()
        return (change_count, create_count)

    def location_from_broker_row(self, broker_row, field_map, value_map):
        """Find or create Location from a row of broker data"""

        row = canonicalize_location_dict(broker_row)

        # THIS ASSUMPTION DOES NOT HOLD FOR FPDS SINCE IT DOES NOT HAVE A PLACE OF PERFORMANCE CODE
        # We can assume that if the country code is blank and the place of performance code is NOT '00FORGN', then
        # the country code is USA
        # if pop_flag and not country_code and pop_code != '00FORGN':
        #     row[field_map.get('location_country_code')] = 'USA'

        # Get country code obj
        location_country_code = self.country_code_map.get(row[field_map.get('location_country_code')])

        # Fix state code periods
        state_code = row.get(field_map.get('state_code'))
        if state_code is not None:
            value_map.update({'state_code': state_code.replace('.', '')})

        if location_country_code:
            value_map.update({
                'location_country_code': location_country_code,
                'country_name': location_country_code.country_name
            })

            if location_country_code.country_code != 'USA':
                value_map.update({'state_code': None, 'state_name': None})
        else:
            # no country found for this code
            value_map.update({'location_country_code': None, 'country_name': None})

        location_instance_data = load_data_into_model(
            Location(), row, value_map=value_map, field_map=field_map, as_dict=True)

        loc = Location.objects.filter(**location_instance_data).order_by('create_date').first()
        if loc:
            created = False
        else:
            loc = Location(**location_instance_data)
            loc.save()
            created = True
        return (loc, created)


class FABSLocationFixer(LocationFixer):

    BROKER_COLUMNS = '*'

    def broker_ids_for_bad_pops(self):
        """Get published_award_financial_assistance_id for places of performance with blank state code"""

        result = TransactionFABS.objects.only('published_award_financial_assistance_id'). \
            filter(transaction__place_of_performance__state_code__isnull=True). \
            filter(transaction__place_of_performance__location_country_code__country_code='USA')

        return result

    def get_broker_data(self, broker_ids):

        id_list = ','.join(b_id.published_award_financial_assistance_id for b_id in broker_ids)
        query = """SELECT {}
            FROM published_award_financial_assistance
            WHERE published_award_financial_assistance_id IN ({})""". \
            format(self.BROKER_COLUMNS, id_list)

        self.broker_cursor.execute(query)
        return dictfetchall(self.broker_cursor)

    def txns_from_broker_row(self, row):
        return TransactionFABS.objects.filter(
            published_award_financial_assistance_id=row['published_award_financial_assistance_id'])

    def broker_ids_for_bad_recipients(self):
        """Get published_award_financial_assistance_id for recipient locations with blank state code"""

        result = TransactionFABS.objects.only('published_award_financial_assistance_id'). \
            filter(transaction__recipient__location__state_code__isnull=True). \
            filter(transaction__recipient__location__location_country_code__country_code='USA')

        return result

    pop_field_map = {
        "city_name": "place_of_performance_city",
        "performance_code": "place_of_performance_code",
        "congressional_code": "place_of_performance_congr",
        "county_name": "place_of_perform_county_na",
        "county_code": "place_of_perform_county_co",
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
