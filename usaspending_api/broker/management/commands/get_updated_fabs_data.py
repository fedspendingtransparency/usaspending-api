import logging
from datetime import datetime
import time

from django.db import connection
from django.core.management.base import BaseCommand, CommandError

from usaspending_api.etl.management.load_base import run_sql_file
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, get_or_create_location
from usaspending_api.references.models import LegalEntity
from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award

logger = logging.getLogger('console')


class Command(BaseCommand):
    help = "Update specific FABS transactions and its related tables"
    pop_location = None
    legal_entity_location = None
    legal_entity = None

    place_of_performance_field_map = {
            'city_name': 'place_of_performance_city',
            'performance_code': 'place_of_performance_code',
            'congressional_code': 'place_of_performance_congr',
            'county_name': 'place_of_perform_county_na',
            'county_code': 'place_of_perform_county_co',
            'foreign_location_description': 'place_of_performance_forei',
            'state_name': 'place_of_perform_state_nam',
            'zip4': 'place_of_performance_zip4a',
            'location_country_code': 'place_of_perform_country_c',
            'country_name': 'place_of_perform_country_n'
        }

    legal_entity_location_field_map = {
        'address_line1': 'legal_entity_address_line1',
        'address_line2': 'legal_entity_address_line2',
        'address_line3': 'legal_entity_address_line3',
        'city_name': 'legal_entity_city_name',
        'city_code': 'legal_entity_city_code',
        'congressional_code': 'legal_entity_congressional',
        'county_code': 'legal_entity_county_code',
        'county_name': 'legal_entity_county_name',
        'foreign_city_name': 'legal_entity_foreign_city',
        'foreign_postal_code': 'legal_entity_foreign_posta',
        'foreign_province': 'legal_entity_foreign_provi',
        'state_code': 'legal_entity_state_code',
        'state_name': 'legal_entity_state_name',
        'zip5': 'legal_entity_zip5',
        'zip_last4': 'legal_entity_zip_last4',
        'location_country_code': 'legal_entity_country_code',
        'country_name': 'legal_entity_country_name'
    }

    def add_arguments(self, parser):

        parser.add_argument('--batch', type=int, default=100000, help='Batch size to fetch from broker db cursor')
        parser.add_argument('--date', type=str, help='Required: Updated date to choose from to pull from broker')

    def handle(self, *args, **options):
        start = datetime.now()
        parameters = {
            'updated_date': options.get('date'),
            'batch': options.get('batch')
        }
        rows_remaining = True

        if not parameters['updated_date']:
            raise CommandError('Must specify --date')

        db_cursor = connection.cursor()
        # Creates cursor for foreign data wrapper connecting to broker
        run_sql_file('usaspending_api/broker/management/commands/get_broker_server.sql', parameters)

        while rows_remaining:
            # Fetches rows that need to be updated based on batches pulled from the cursor
            run_sql_file('usaspending_api/broker/management/commands/get_updated_fabs_data.sql', parameters)

            # Retrieves temporary table with FABS rows that need to be updated
            db_cursor.execute('SELECT * from fabs_transactions_to_update;')
            db_rows = dictfetchall(db_cursor)
            for index, row in enumerate(db_rows, 1):
                logger.info('Updating FABS Rows: Inserting row {} of {} ({})'.format(str(index),
                                                                                     str(parameters['batch']),
                                                                                     datetime.now() - start))
                # Arguments to updated in transaction_normalized
                updated_args = {}

                fabs_qs = TransactionFABS.objects.filter(afa_generated_unique=row['afa_generated_unique'])

                # If FABS transactions does not exist then script will skip row
                # The FABS nightly loader will create the new transaction
                if fabs_qs.exists():
                    self.update_transaction_fabs(row, fabs_qs)
                else:
                    continue

                # Creates a new place_of_performance location if the data has changed
                if row['pop_change']:
                    self.update_pop_locations_fabs(row)
                    updated_args['place_of_performance'] = self.pop_location

                # Creates a new recipient if the data has changed or new recipient location
                if row['le_change'] or row['le_loc_change']:
                    self.update_le_locations_fabs(row)
                    self.update_legal_entity_fabs(row)
                    updated_args['recipient'] = self.legal_entity

                self.update_transaction_normalized(row, updated_args)

            elapsed = time.time() - start
            logger.info('Time to process {} rows: {} seconds'.format(options.get('batch'), elapsed))

            # clear properties for next row
            self.pop_location = None
            self.legal_entity_location = None
            self.legal_entity = None

            if len(db_rows) < parameters['batch']:
                rows_remaining = False
                db_cursor.execute('DROP TABLE fabs_transactions_to_update;')
                logger.info('Finished with first batch updating FABS: {} rows ({})'.format(str(parameters['batch']),
                                                                                           datetime.now() - start
                                                                                           ))

        logger.info('FABS UPDATE FINISHED!')

    @staticmethod
    def update_transaction_fabs(row, fab_qs):
        """Updates transaction in Transaction_FABS Table"""
        financial_assistance_data = load_data_into_model(
            TransactionFABS(),  # thrown away
            row,
            as_dict=True)

        fab_qs.update(**financial_assistance_data)

    @staticmethod
    def update_transaction_normalized(row, updated_args):
        """Updates transaction_normalized and awards tables with modified fields"""
        if row['trans_change']:
            updated_args['period_of_performance_start_date'] = format_date(row['period_of_performance_star'])
            updated_args['period_of_performance_current_end_date'] = format_date(row['period_of_performance_curr'])
            updated_args['action_date'] = format_date(row['action_date'])
            updated_args['last_modified_date'] = datetime.strptime(str(row['modified_at']),
                                                                   '%Y-%m-%d %H:%M:%S.%f').date()
            updated_args['type'] = row['assistance_type']
            updated_args['description'] = row['award_description']
            updated_args['create_date'] = row['create_date']
            updated_args['action_date'] = row['action_date']
            updated_args['federal_action_obligation'] = row['federal_action_obligation']

        if len(updated_args) > 0:
            transaction_id = TransactionFABS.objects.filter(afa_generated_unique=row['afa_generated_unique']) \
                .values('transaction_id').first()['transaction_id']

            trans_qs = TransactionNormalized.objects.filter(id=transaction_id)
            trans_qs.update(**updated_args)

            award_id = TransactionNormalized.objects.filter(id=transaction_id).values('award_id').first['award_id']

            if award_id:
                Award.objects.filter(latest_transaction_id=transaction_id).update(**updated_args)

    def update_pop_locations_fabs(self, row):
        """Creates or gets Place of Performance Location"""
        self.pop_location, created = get_or_create_location(
            self.place_of_performance_field_map, row, {'place_of_performance_flag': True}
        )

    def update_le_locations_fabs(self, row):
        """Creates or gets Recipient Location"""
        self.legal_entity_location, created = get_or_create_location(
            self.legal_entity_location_field_map, row, {'recipient_flag': True}
        )

    def update_legal_entity_fabs(self, row):
        """Creates or gets Recipient"""
        recipient_name = row['awardee_or_recipient_legal']
        if recipient_name is None:
            recipient_name = ''

        # Handling the case of duplicates, just grab the most recently updated match
        legal_entity = LegalEntity.objects.filter(
            recipient_unique_id=row['awardee_or_recipient_uniqu'],
            recipient_name=recipient_name,
            location=self.legal_entity_location
        ).order_by('-update_date').first()
        created = False

        if not legal_entity:
            legal_entity = LegalEntity.objects.create(
                recipient_unique_id=row['awardee_or_recipient_uniqu'],
                recipient_name=recipient_name,
                location=self.legal_entity_location
            )
            created = True

        if created:
            legal_entity_value_map = {
                'location': self.legal_entity_location,
            }
            self.legal_entity = load_data_into_model(
                legal_entity, row, value_map=legal_entity_value_map, save=True
            )


