import logging
import timeit
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connections, transaction as db_transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFABS
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.references.helpers import canonicalize_location_dict
from usaspending_api.references.models import RefCountryCode, Location, RefCityCountyCode


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

country_code_map = {country['country_code']: country['country_name'] for country in RefCountryCode.objects.values('country_code', 'country_name')}

pop_lookup = {}
pop_bulk = []

lel_lookup = {}
lel_bulk = []

class Command(BaseCommand):
    help = "Update historical transaction data for a fiscal year from the Broker."

    @staticmethod
    def get_fabs_data(db_cursor, fiscal_year=None, page=1, limit=500000):
        query = "SELECT * FROM published_award_financial_assistance WHERE is_active=TRUE"
        arguments = []

        fy_begin = '10/01/' + str(fiscal_year - 1)
        fy_end = '09/30/' + str(fiscal_year)

        if fiscal_year:
            query += " AND"
            query += ' action_date::Date BETWEEN %s AND %s'
            arguments += [fy_begin]
            arguments += [fy_end]
        query += ' ORDER BY published_award_financial_assistance_id LIMIT %s OFFSET %s'
        arguments += [limit, (page-1)*limit]

        logger.info("Executing query on Broker DB => " + query % tuple(arguments))

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        return dictfetchall(db_cursor)

    @staticmethod
    def load_transaction_fabs(fabs_broker_data, total_rows):
        logger.info('Starting bulk loading for FABS data')

        fabs_bulk = []

        start_time = datetime.now()
        for index, row in enumerate(fabs_broker_data, 1):
            if not (index % 10000):
                logger.info('Transaction FABS: Loading row {} of {} ({})'.format(str(index),
                                                                             str(total_rows),
                                                                             datetime.now() - start_time))

            fab_instance_data = load_data_into_model(
                TransactionFABS(),  # thrown away
                row,
                as_dict=True)

            fabs_instance = TransactionFABS(**fab_instance_data)
            fabs_bulk.append(fabs_instance)

        with db_transaction.atomic():
            logger.info('Bulk creating Transaction FABS...')
            TransactionFABS.objects.bulk_create(fabs_bulk)


    @staticmethod
    def load_pop_locations(fabs_broker_data, total_rows):

        pop_field_map = {
            "city_name": "place_of_performance_city",
            "performance_code": "place_of_performance_code",
            "congressional_code": "place_of_performance_congr",
            "county_name": "place_of_perform_county_na",
            "foreign_location_description": "place_of_performance_forei",
            "state_name": "place_of_perform_state_nam",
            "zip4": "place_of_performance_zip4a",
            "location_country_code": "place_of_perform_country_c"

        }

        start_time = datetime.now()
        for index, row in enumerate(fabs_broker_data, 1):
            if not (index % 10000):
                logger.info('Locations: Loading row {} of {} ({})'.format(str(index),
                                                                             str(total_rows),
                                                                             datetime.now() - start_time))
            location_value_map = {"place_of_performance_flag": True}
            row = canonicalize_location_dict(row)

            country_code = row[pop_field_map.get('location_country_code')]
            pop_code = row[pop_field_map.get('performance_code')]

            # We can assume that if the country code is blank and the place of performance code is NOT '00FORGN', then
            # the country code is USA
            if not country_code and pop_code != '00FORGN':
                row[pop_field_map.get('location_country_code')] = 'USA'

            # Get country code obj
            location_country_code = country_code_map.get(row[pop_field_map.get('location_country_code')])

            # Fix state code periods
            state_code = row.get(pop_field_map.get('state_code'))
            if state_code is not None:
                pop_field_map.update({'state_code': state_code.replace('.', '')})

            if location_country_code:
                location_value_map.update({
                    'location_country_code': location_country_code,
                    'country_name': country_code_map[location_country_code]
                })

                if location_country_code != 'USA':
                    location_value_map.update({
                        'state_code': None,
                        'state_name': None
                    })
            else:
                # no country found for this code
                location_value_map.update({
                    'location_country_code': None,
                    'country_name': None
                })

            pop_instance_data = load_data_into_model(
                Location(),
                row,
                value_map=location_value_map,
                field_map=pop_field_map,
                as_dict=True)

            pop_instance = Location(**pop_instance_data)
            pop_instance.load_country_data()
            pop_instance.load_city_county_data()
            pop_instance.fill_missing_state_data()

            pop_bulk.append(pop_instance)
            pop_lookup[index] = pop_instance

        with db_transaction.atomic():
            logger.info('Bulk creating Place of Performance Locations...')
            Location.objects.bulk_create(pop_bulk)

    def add_arguments(self, parser):

        parser.add_argument(
            '--fiscal_year',
            dest="fiscal_year",
            nargs='+',
            type=int,
            help="Year for which to run the historical load"
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

    def handle(self, *args, **options):
        logger.info('Starting FABS bulk data load...')

        db_cursor = connections['data_broker'].cursor()
        fiscal_year = options.get('fiscal_year')
        page = options.get('page')
        limit = options.get('limit')

        if fiscal_year:
            fiscal_year = fiscal_year[0]
            logger.info('Processing data for Fiscal Year ' + str(fiscal_year))
        else:
            fiscal_year = 2017

        page = page[0] if page else 1
        limit = limit[0] if limit else 500000

        logger.info('Get Broker FABS data...')
        start = timeit.default_timer()
        fabs_broker_data = self.get_fabs_data(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit)
        total_rows = len(fabs_broker_data)
        end = timeit.default_timer()
        logger.info('Finished getting Broker FABS data in ' + str(end - start) + ' seconds')

        logger.info('Loading Transaction FABS data...')
        start = timeit.default_timer()
        self.load_transaction_fabs(fabs_broker_data, total_rows)
        end = timeit.default_timer()
        logger.info('Finished FABS bulk data load in ' + str(end - start) + ' seconds')

        logger.info('Loading POP Location data...')
        start = timeit.default_timer()
        self.load_pop_locations(fabs_broker_data, total_rows)
        end = timeit.default_timer()
        logger.info('Finished POP Location bulk data load in ' + str(end - start) + ' seconds')
