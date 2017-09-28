import logging
import timeit
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connections, transaction as db_transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFABS
from usaspending_api.etl.management.load_base import load_data_into_model



logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


class Command(BaseCommand):
    help = "Update historical transaction data for a fiscal year from the Broker."

    @staticmethod
    def update_transaction_assistance(db_cursor, fiscal_year=None, page=1, limit=500000):
        logger.info('Starting bulk loading for FABS data')

        query = "SELECT * FROM published_award_financial_assistance"
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
        arguments += [limit, (page-1)*limit]

        logger.info("Executing query on Broker DB => " + query % tuple(arguments))

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        fabs_broker_data = dictfetchall(db_cursor)

        fabs_bulk = []

        total_rows = len(fabs_broker_data)
        start_time = datetime.now()
        for index, row in enumerate(fabs_broker_data, 1):
            if not (index % 100):
                logger.info('D1 File Load: Loading row {} of {} ({})'.format(str(index),
                                                                             str(total_rows),
                                                                             datetime.now() - start_time))

            fab_instance_data = load_data_into_model(
                TransactionFABS(),  # thrown away
                row,
                as_dict=True)

            fabs_instance = TransactionFABS(**fab_instance_data)
            fabs_bulk.append(fabs_instance)

        with db_transaction.atomic():
            TransactionFABS.objects.bulk_create(fabs_bulk)

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

        start = timeit.default_timer()
        self.update_transaction_assistance(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit)
        end = timeit.default_timer()
        logger.info('Finished FABS bulk data load in ' + str(end - start) + ' seconds')
