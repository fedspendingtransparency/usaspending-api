import logging
import timeit
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db import connections, connection, transaction as db_transaction
from usaspending_api.common.helpers import fy

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFPDS, TransactionNormalized, Award
from usaspending_api.etl.management.load_base import load_data_into_model, format_date
from usaspending_api.references.helpers import canonicalize_location_dict
from usaspending_api.references.models import RefCountryCode, Location, LegalEntity, Agency, ToptierAgency, \
    SubtierAgency
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards, update_award_categories

BATCH_SIZE = 100000

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

fpds_bulk = []

pop_bulk = []

lel_bulk = []

legal_entity_lookup = []
legal_entity_bulk = []

awarding_agency_list = []
funding_agency_list = []

award_lookup = []
award_bulk = []
parent_award_lookup = []
parent_award_bulk = []

transaction_normalized_bulk = []

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
        piids = ','.join("'{}'".format(p) for p in piid)
        query = 'SELECT * FROM detached_award_procurement WHERE piid IN ({})'.format(
            piids)
        logger.info("Executing select query on Broker DB")

        db_cursor.execute(query, [piid, ])

        logger.info("Running dictfetchall on db_cursor")
        return dictfetchall(db_cursor)

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

        (loc, created) = Location.objects.get_or_create(**
                                                        location_instance_data)
        loc.save()
        return loc

    def fix_places_of_performance(self, fpds_broker_data):

        start_time = datetime.now()
        value_map = {"place_of_performance_flag": True}
        for index, row in enumerate(fpds_broker_data, 1):
            if not (index % 10000):
                logger.info('Place of performance: fixing row {} ({})'.format(
                    str(index), datetime.now() - start_time))
            loc = self.location_from_fpds_row(row,
                                              field_map=pop_field_map,
                                              value_map=dict(value_map))
            for txn in TransactionFPDS.objects.filter(
                    detached_award_procurement_id=row[
                        'detached_award_procurement_id']).all():
                txn.transaction.place_of_performance = loc
                txn.transaction.save()
                if txn.transaction == txn.transaction.award.latest_transaction:
                    txn.transaction.award.place_of_performance = loc
                    txn.transaction.award.save()

    def fix_recipient_locations(self, fpds_broker_data):

        value_map = {"recipient_flag": True}
        for index, row in enumerate(fpds_broker_data, 1):
            if not (index % 10000):
                logger.info('Recipient location: fixing row {} ({})'.format(
                    str(index), datetime.now() - start_time))
            for txn in TransactionFPDS.objects.filter(
                    detached_award_procurement_id=row[
                        'detached_award_procurement_id']).all():
                recip = txn.transaction.recipient
                last_txn = recip.transactionnormalized_set.order_by(
                    '-action_date')[0]
                if txn.transaction == last_txn:
                    loc = self.location_from_fpds_row(
                        row,
                        field_map=le_field_map,
                        value_map=dict(value_map))
                    recip.location = loc
                    recip.save()

    def add_arguments(self, parser):

        parser.add_argument('--piid',
                            nargs='+',
                            help="PIID of award(s) to reload")

    @db_transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting FPDS data fix...')

        db_cursor = connections['data_broker'].cursor()
        ds_cursor = connection.cursor()
        piid = options.get('piid')

        self.set_lookup_maps()

        logger.info('Processing data for Piid {}'.format(piid))

        # Set lookups after deletions to only get latest
        # self.set_lookup_maps()

        logger.info('Get Broker FPDS data...')
        start = timeit.default_timer()
        fpds_broker_data = self.get_fpds_data(db_cursor=db_cursor, piid=piid)
        end = timeit.default_timer()
        logger.info('Finished getting Broker FPDS data in ' + str(end - start)
                    + ' seconds')

        logger.info('Fixing POP Location data...')
        start = timeit.default_timer()
        self.fix_places_of_performance(fpds_broker_data=fpds_broker_data)
        end = timeit.default_timer()
        logger.info('Finished POP Location bulk data load in ' + str(
            end - start) + ' seconds')

        logger.info('Fixing LE Location data...')
        start = timeit.default_timer()
        self.fix_recipient_locations(fpds_broker_data=fpds_broker_data)
        end = timeit.default_timer()
        logger.info('Finished LE Location bulk data load in ' + str(
            end - start) + ' seconds')
