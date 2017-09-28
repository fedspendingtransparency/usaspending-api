import logging
import timeit
from datetime import datetime
import sys
import re
from django.db.models import F, Q
from django.core.management.base import BaseCommand
from django.db import connections, transaction as db_transaction
from usaspending_api.references.abbreviations import state_to_code, code_to_state
from django.db.utils import IntegrityError
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.references.models import Agency, SubtierAgency, ToptierAgency, Location, \
    RefCountryCode, RefCityCountyCode
from usaspending_api.etl.management.load_base import copy, load_data_into_model

from usaspending_api.references.helpers import canonicalize_location_dict

from usaspending_api.awards.models import TransactionNormalized

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

# Lists to store for update_awards and update_contract_awards
award_update_id_list = []
award_contract_update_id_list = []

subtier_agency_map = {subtier_agency['subtier_code']: subtier_agency['subtier_agency_id'] for subtier_agency in
                      SubtierAgency.objects.values('subtier_code', 'subtier_agency_id')}
subtier_to_agency_map = {
agency['subtier_agency_id']: {'agency_id': agency['id'], 'toptier_agency_id': agency['toptier_agency_id']} for agency in
Agency.objects.values('id', 'toptier_agency_id', 'subtier_agency_id')}
toptier_agency_map = {toptier_agency['toptier_agency_id']: toptier_agency['cgac_code'] for toptier_agency in
                      ToptierAgency.objects.values('toptier_agency_id', 'cgac_code')}

"""
This is a county code map that needs to be created anytime this file is loaded so that references can be pulled
from memory.
"""
county_code_columns = ['city_code', 'county_code', 'state_code', 'city_name', 'county_name']

regex_solution = True
dict_subset_solution = False

if dict_subset_solution:
    county_code_map = {}
    for _city in RefCityCountyCode.objects.all():
        _key = ''
        _value = {}
        for _column in county_code_columns:
            _column_value = getattr(_city, _column, '')
            _value[_column] = _column_value
            if _column_value:
                _key += _column_value.lower()
        county_code_map[_key] = _value
elif regex_solution:
    county_code_map = {}
    for _city in RefCityCountyCode.objects.all():
        key = "_".join([getattr(_city, col, '') for col in county_code_columns])
        dict = {col: getattr(_city, col, '') for col in county_code_columns}
        county_code_map[key] = dict


def dict_is_subset(d1, d2):
    return set(d1.items()) <= set(d2.items())

def dict_in_mapping(regex, mapping):
    return regex if re.match(regex, mapping) else None

class Command(BaseCommand):
    help = "Create Locations from Location data in the Broker."

    @staticmethod
    def update_location_transaction_assistance(db_cursor, fiscal_year=2017, page=1, limit=500000, save=True):

        list_of_columns = (', '.join(['fain', 'url', 'modification_number']))

        # get the transaction values we need

        query = "SELECT {} FROM published_award_financial_assistance".format(list_of_columns)
        arguments = []

        fy_begin = '10/01/' + str(fiscal_year - 1)
        fy_end = '09/30/' + str(fiscal_year)

        if fiscal_year:
            if arguments:
                query += " AND"
            else:
                query += " WHERE"
            query += ' action_date::Date BETWEEN %s AND %s'
            arguments += [fy_begin]
            arguments += [fy_end]
        query += ' ORDER BY published_award_financial_assistance_id LIMIT %s OFFSET %s'
        arguments += [limit, (page - 1) * limit]

        logger.info("Executing query on Broker DB => " + query % (arguments[0], arguments[1],
                                                                  arguments[2], arguments[3]))

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        award_financial_assistance_data = dictfetchall(db_cursor)

        logger.info("Getting total rows")
        total_rows = len(award_financial_assistance_data)  # - rows_loaded

        logger.info("Processing " + str(total_rows) + " rows of location data")

        bulk_array = []

        start_time = datetime.now()
        for index, row in enumerate(award_financial_assistance_data, 1):
            with db_transaction.atomic():

                if not (index % 100):
                    logger.info('Location Load: Loading row {} of {} ({})'.format(str(index),
                                                                                 str(total_rows),
                                                                                 datetime.now() - start_time))
                # Could also use contract_data__fain
                transaction = TransactionNormalized.objects.filter(award__fain=row['fain'],award__uri=row['uri'],modification_number=row['modification_number'])

                if transaction.recipient and transaction.recipient.location:
                    lel = transaction.recipient.location
                    lel.save()

                if transaction.place_of_performance:
                    pop = transaction.place_of_performance
                    pop.save()

    @staticmethod
    def update_location_transaction_contract(db_cursor, fiscal_year=None, page=1, limit=500000, save=True):

        list_of_columns = (', '.join(['piid', 'modification_number']))

        query = "SELECT {} FROM detached_award_procurement".format(list_of_columns)
        arguments = []

        fy_begin = '10/01/' + str(fiscal_year - 1)
        fy_end = '09/30/' + str(fiscal_year)

        if fiscal_year:
            if arguments:
                query += " AND"
            else:
                query += " WHERE"
            query += ' action_date::Date BETWEEN %s AND %s'
            arguments += [fy_begin]
            arguments += [fy_end]
        query += ' ORDER BY detached_award_procurement_id LIMIT %s OFFSET %s'
        arguments += [limit, (page - 1) * limit]

        logger.info("Executing query on Broker DB => " + query % (arguments[0], arguments[1],
                                                                  arguments[2], arguments[3]))

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        procurement_data = dictfetchall(db_cursor)

        logger.info("Getting total rows")
        # rows_loaded = len(current_ids)
        total_rows = len(procurement_data)  # - rows_loaded

        logger.info("Processing " + str(total_rows) + " rows of procurement data")

        # bulk_array = []

        start_time = datetime.now()
        for index, row in enumerate(procurement_data, 1):
            with db_transaction.atomic():

                if not (index % 100):
                    logger.info('D1 File Load: Added row {} of {} ({})'.format(str(index),
                                                                                 str(total_rows),
                                                                                 datetime.now() - start_time))

                transaction = TransactionNormalized.objects.filter(award__piid=row['piid'],modification_number=row['modification_number'])

                if transaction.recipient and transaction.recipient.location:
                    lel = transaction.recipient.location
                    lel.save()

                if transaction.place_of_performance:
                    pop = transaction.place_of_performance
                    pop.save()

        logger.info('saving locations')
        # logger.info('BULK ARRAY: '.format(bulk_array))

    def add_arguments(self, parser):

        parser.add_argument(
            '--fiscal_year',
            dest="fiscal_year",
            nargs='+',
            type=int,
            help="Year for which to run the historical load"
        )

        parser.add_argument(
            '--assistance',
            action='store_true',
            dest='assistance',
            default=False,
            help='Runs the historical loader only for Award Financial Assistance (Assistance/FABS) data'
        )

        parser.add_argument(
            '--contracts',
            action='store_true',
            dest='contracts',
            default=False,
            help='Runs the historical loader only for Award Procurement (Contract/FPDS) data'
        )

        parser.add_argument(
            '--page',
            dest="page",
            nargs='+',
            type=int,
            help="Page for batching and parallelization"
        )

        parser.add_argument(
            '--limit',
            dest="limit",
            nargs='+',
            type=int,
            help="Limit for batching and parallelization"
        )
        parser.add_argument(
            '--save',
            dest="save",
            default=True,
            help="Decides if the save method is called after loading"
        )

    # @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting historical data load...')

        db_cursor = connections['data_broker'].cursor()
        fiscal_year = options.get('fiscal_year')
        page = options.get('page')
        limit = options.get('limit')
        save = options.get('save')

        if fiscal_year:
            fiscal_year = fiscal_year[0]
            logger.info('Processing data for Fiscal Year ' + str(fiscal_year))
        else:
            fiscal_year = 2017

        page = page[0] if page else 1
        limit = limit[0] if limit else 500000

        if not options['assistance']:
            logger.info('Starting D1 historical data location insert...')
            start = timeit.default_timer()
            self.update_locations_transaction_contract(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit, save=save)
            end = timeit.default_timer()
            logger.info('Finished D1 historical data location insert in ' + str(end - start) + ' seconds')

        if not options['contracts']:
            logger.info('Starting D2 historical data location insert...')
            start = timeit.default_timer()
            self.update_locations_transaction_assistance(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit, save=save)
            end = timeit.default_timer()
            logger.info('Finished D2 historical data location insert in ' + str(end - start) + ' seconds')

        logger.info('FINISHED')