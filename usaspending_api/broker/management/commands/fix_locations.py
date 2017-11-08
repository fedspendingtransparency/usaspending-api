"""
Idempotently re-runs location creation / matching code from broker FPDS data
"""

import logging
import timeit
from datetime import datetime
from functools import wraps
from itertools import count, groupby

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction as db_transaction
from django.db import connection, connections

from usaspending_api.awards.models import (TransactionFABS, TransactionFPDS, TransactionNormalized)
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.broker.management.commands import fabs_nightly_loader, fpds_nightly_loader
from usaspending_api.etl.management.load_base import (format_date, load_data_into_model)
from usaspending_api.references.helpers import canonicalize_location_dict
from usaspending_api.references.models import Location, RefCountryCode

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

BATCH_DOWNLOAD_SIZE = 100


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

    # DAP_COLUMNS = """detached_award_procurement_id, place_of_perform_country_c,
    #   place_of_perform_city_name, place_of_performance_zip4a,
    #   place_of_performance_congr, place_of_perform_county_na,
    #   place_of_performance_state,
    #   legal_entity_state_code,
    #   legal_entity_city_name ,
    #   legal_entity_address_line1 ,
    #   legal_entity_address_line2 ,
    #   legal_entity_address_line3 ,
    #   legal_entity_zip4 ,
    #   legal_entity_congressional ,
    #   legal_entity_country_code
    #   """

    # PFA_COLUMNS = '*'

    # help = "Update historical transaction data for a fiscal year from the Broker."

    # country_code_map = {}

    # def set_lookup_maps(self):
    #     self.country_code_map = {country.country_code: country for country in RefCountryCode.objects.all()}

    # def get_fpds_data(self, db_cursor, filter, parameter):
    #     """Get all detached_award_procurement rows from broker database for a certain filter"""

    #     query = 'SELECT {} FROM detached_award_procurement WHERE {}'.format(self.DAP_COLUMNS, filter)
    #     db_cursor.execute(query, [parameter])
    #     return dictfetchall(db_cursor)

    # def get_fabs_data(self, db_cursor, filter, parameter):
    #     """Get all published_award_financial_assistance rows from broker database for a certain filter"""

    #     query = 'SELECT {} FROM published_award_financial_assistance WHERE {}'.format(self.PFA_COLUMNS, filter)
    #     db_cursor.execute(query, [parameter])
    #     return dictfetchall(db_cursor)

    # def get_fpds_data_by_piid(self, db_cursor, piid):
    #     """Get all detached_award_procurement rows from broker database with given PIID"""
    #     return self.get_fpds_data(db_cursor, 'piid = %s', piid)

    # def get_fpds_data_by_dap_id(self, db_cursor, dap_ids):
    #     """Get all detached_award_procurement rows from broker database with given detached_award_procurement_id"""
    #     qry = 'detached_award_procurement_id IN ({})'.format(','.join(d.detached_award_procurement_id for d in dap_ids))
    #     return self.get_fpds_data(db_cursor, qry, [])

    # def get_fabs_data_by_fa_id(self, db_cursor, fa_ids):
    #     """Get all published_award_financial_assistance rows from broker database with given published_award_financial_assistance_id"""
    #     qry = 'published_award_financial_assistance_id IN ({})'.format(','.join(fa.published_award_financial_assistance_id for fa in fa_ids))

    #     return self.get_fabs_data(db_cursor, qry, [])


    # def apply_broker_filters(self, result, limit, fiscal_year=None, only_final_transactions=False):

    #     if fiscal_year:
    #         result = result.filter(transaction__fiscal_year=fiscal_year)

    #     if only_final_transactions:
    #         # for speed, only fix final transaction of each award,
    #         # which are the only ones now surfaced on website
    #         result = result.filter(transaction__latest_for_award__isnull=False)

    #     return result[:limit]

    # @log_time
    # def dap_ids_for_blank_pops(self, limit, fiscal_year=None, only_final_transactions=False):
    #     """Get detached_award_procurement_ids for places of performance with blank state code"""

    #     result = TransactionFPDS.objects.only('detached_award_procurement_id'). \
    #         filter(transaction__place_of_performance__state_code__isnull=True). \
    #         filter(transaction__place_of_performance__location_country_code__country_code='USA')

    #     return self.apply_broker_filters(result, limit, fiscal_year, only_final_transactions)

    # def fa_ids_for_blank_pops(self, limit, fiscal_year=None, only_final_transactions=False):
    #     """Get published_award_financial_assistance_id for places of performance with blank state code"""

    #     result = TransactionFABS.objects.only('published_award_financial_assistance_id'). \
    #         filter(transaction__place_of_performance__state_code__isnull=True). \
    #         filter(transaction__place_of_performance__location_country_code__country_code='USA')

    #     return self.apply_broker_filters(result, limit, fiscal_year, only_final_transactions)


    # def dap_ids_for_blank_recipients(self, limit, fiscal_year=None, only_final_transactions=False):
    #     """Get detached_award_procurement_ids for recipient locations with blank state code"""

    #     result = TransactionFPDS.objects.only('detached_award_procurement_id'). \
    #         filter(transaction__recipient__location__state_code__isnull=True). \
    #         filter(transaction__recipient__location__location_country_code__country_code='USA')

    #     return self.apply_broker_filters(result, limit, fiscal_year, only_final_transactions)

    # def fa_ids_for_blank_recipients(self, limit, fiscal_year=None, only_final_transactions=False):
    #     """Get published_award_financial_assistance_id for recipient locations with blank state code"""

    #     result = TransactionFABS.objects.only('published_award_financial_assistance_id'). \
    #         filter(transaction__recipient__location__state_code__isnull=True). \
    #         filter(transaction__recipient__location__location_country_code__country_code='USA')

    #     return self.apply_broker_filters(result, limit, fiscal_year, only_final_transactions)


    # def location_from_fpds_row(self, fpds_broker_row, field_map, value_map):
    #     """Find or create Location from a row of broker FPDS data"""

    #     row = canonicalize_location_dict(fpds_broker_row)

    #     # THIS ASSUMPTION DOES NOT HOLD FOR FPDS SINCE IT DOES NOT HAVE A PLACE OF PERFORMANCE CODE
    #     # We can assume that if the country code is blank and the place of performance code is NOT '00FORGN', then
    #     # the country code is USA
    #     # if pop_flag and not country_code and pop_code != '00FORGN':
    #     #     row[field_map.get('location_country_code')] = 'USA'

    #     # Get country code obj
    #     location_country_code = self.country_code_map.get(row[field_map.get('location_country_code')])

    #     # Fix state code periods
    #     state_code = row.get(field_map.get('state_code'))
    #     if state_code is not None:
    #         value_map.update({'state_code': state_code.replace('.', '')})

    #     if location_country_code:
    #         value_map.update({
    #             'location_country_code': location_country_code,
    #             'country_name': location_country_code.country_name
    #         })

    #         if location_country_code.country_code != 'USA':
    #             value_map.update({'state_code': None, 'state_name': None})
    #     else:
    #         # no country found for this code
    #         value_map.update({'location_country_code': None, 'country_name': None})

    #     location_instance_data = load_data_into_model(
    #         Location(), row, value_map=value_map, field_map=field_map, as_dict=True)

    #     loc = Location.objects.filter(**location_instance_data).order_by('create_date').first()
    #     if loc:
    #         created = False
    #     else:
    #         loc = Location(**location_instance_data)
    #         loc.save()
    #         created = True
    #     return (loc, created)

    # def _fpds_rows(self, row):
    #     return TransactionFPDS.objects.filter(
    #                 detached_award_procurement_id=row['detached_award_procurement_id'])

    # def _fabs_rows(self, row):
    #     return TransactionFABS.objects.filter(
    #                 published_award_financial_assistance_id=row['published_award_financial_assistance_id'])

    # def _fix_places_of_performance(self, broker_data, broker_row_getter):

    #     start_time = datetime.now()
    #     value_map = {"place_of_performance_flag": True}
    #     create_count = change_count = 0
    #     for index, row in enumerate(broker_data, 1):
    #         (loc, created) = self.location_from_fpds_row(row, field_map=pop_field_map, value_map=dict(value_map))
    #         create_count += int(created)
    #         for txn in broker_row_getter(row).all():
    #             if txn.transaction.place_of_performance != loc:
    #                 change_count += 1
    #                 txn.transaction.place_of_performance = loc
    #                 txn.transaction.save()
    #             if txn.transaction_id == txn.transaction.award.latest_transaction_id:
    #                 txn.transaction.award.place_of_performance = loc
    #                 txn.transaction.award.save()
    #     return (change_count, create_count)


    # @log_time
    # def fix_fpds_places_of_performance(self, broker_data):

    #     return self._fix_places_of_performance(broker_data, self._fpds_rows)

    # @log_time
    # def fix_fabs_places_of_performance(self, broker_data):

    #     return self._fix_places_of_performance(broker_data, self._fabs_rows)

    # @log_time
    # def fix_fpds_recipient_locations(self, broker_data):

    #     return self._fix_recipient_locations(broker_data, self._fpds_rows)

    # @log_time
    # def fix_fabs_recipient_locations(self, broker_data):

    #     return self._fix_recipient_locations(broker_data, self._fabs_rows)


    # def _fix_recipient_locations(self, broker_data, broker_row_getter):

    #     value_map = {"recipient_flag": True}
    #     create_count = change_count = 0
    #     for index, row in enumerate(broker_data, 1):
    #         for txn in broker_row_getter(row).all():
    #             recip = txn.transaction.recipient
    #             last_txn = recip.transactionnormalized_set.order_by('-action_date')[0]
    #             if txn.transaction == last_txn:
    #                 (loc, created) = self.location_from_fpds_row(row, field_map=le_field_map, value_map=dict(value_map))
    #                 create_count += int(created)
    #                 if recip.location != loc:
    #                     change_count += 1
    #                     recip.location = loc
    #                     recip.save()
    #     return (change_count, create_count)

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

    def handle(self, *args, **options):
        logger.info('Starting FPDS data fix...')
        start = timeit.default_timer()

        if options.get('contracts'):
            fixer = FPDSLocationFixer(options)
        elif options.get('assistance'):
            fixer = FABSLocationFixer(options)

        fixer.fix()



        # self.set_lookup_maps()
        # db_cursor = connections['data_broker'].cursor()
        # for_contracts = options.get('contracts')
        # for_assistance = options.get('assistance')
        # piid = options.get('piid')
        # limit = options.get('limit')
        # fy = options.get('fiscal_year')
        # only_final = options.get('only_final_transactions')
        # if piid:
        #     broker_data = self.get_fpds_data_by_piid(db_cursor=db_cursor, piid=piid)
        #     self.fix_fpds_places_of_performance(broker_data=broker_data)
        #     self.fix_fpds_recipient_locations(broker_data=broker_data)
        # else:
        #     if for_contracts:
        #         blank_pop_id_getter = self.dap_ids_for_blank_pops
        #         blank_recipient_id_getter = self.dap_ids_for_blank_recipients
        #         get_data_by_id  = self.get_fpds_data_by_dap_id
        #         fix_places_of_performance = self.fix_fpds_places_of_performance
        #         fix_recipient_locations = self.fix_fpds_recipient_locations
        #     elif for_assistance:
        #         import ipdb; ipdb.set_trace()
        #         blank_pop_id_getter = self.fa_ids_for_blank_pops
        #         blank_recipient_id_getter = self.fa_ids_for_blank_recipients
        #         get_data_by_id  = self.get_fabs_data_by_fa_id
        #         fix_places_of_performance = self.fix_fabs_places_of_performance
        #         fix_recipient_locations = self.fix_fabs_recipient_locations
        #     else:
        #         raise CommandError('Must use either --piid, --contracts, or --assistance')

        #     for id_chunk in chunks(
        #             blank_pop_id_getter(limit=limit, fiscal_year=fy, only_final_transactions=only_final)):
        #         broker_data = get_data_by_id(db_cursor, id_chunk)
        #         fix_places_of_performance(broker_data=broker_data)
        #     for id_chunk in chunks(
        #             blank_recipient_id_getter(limit=limit, fiscal_year=fy, only_final_transactions=only_final)):
        #         broker_data = get_data_by_id(db_cursor, id_chunk)
        #         fix_recipient_locations(broker_data=broker_data)


def chunks(source_iterable, size=1000):
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

    def fix(self):

        broker_ids = self.broker_ids_for_bad_pops()
        broker_ids = self.apply_option_filters(broker_ids, self.options)
        for some_broker_ids in chunks(broker_ids):
            broker_rows = self.get_broker_data(some_broker_ids)
            self.fix_places_of_performance(broker_rows)

        broker_ids = self.broker_ids_for_bad_recipients()
        broker_ids = self.apply_option_filters(broker_ids, self.options)
        for some_broker_ids in chunks(broker_ids):
            broker_rows = self.get_broker_data(some_broker_ids)
            self.fix_recipients(broker_rows)

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

    def fix_places_of_performance(self, broker_data):

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

    def fix_recipients(self, broker_data):

        value_map = {"recipient_flag": True}
        create_count = change_count = 0
        for row in broker_data:
            for txn in self.txns_from_broker_row(row).all():
                recip = txn.transaction.recipient
                last_txn = recip.transactionnormalized_set.order_by('-action_date')[0]
                if txn.transaction == last_txn:
                    (loc, created) = self.location_from_broker_row(row, field_map=self.le_field_map, value_map=dict(value_map))
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
        return TransactionFPDS.objects.filter(
                    detached_award_procurement_id=row['detached_award_procurement_id'])

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

