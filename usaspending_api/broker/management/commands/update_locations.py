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
    def update_transaction_assistance(db_cursor, fiscal_year=2017, page=1, limit=500000, save=True):

        legal_entity_location_field_map = {
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            # "city_code": "legal_entity_city_code", # NOT PRESENT IN FABS!
            "city_name": "legal_entity_city_name",
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
            "location_country_code": "legal_entity_country_code"
        }

        legal_entity_location_value_map = {
            "recipient_flag": True
        }

        place_of_performance_field_map = {
            "city_name": "place_of_performance_city",
            "performance_code": "place_of_performance_code",
            "congressional_code": "place_of_performance_congr",
            "county_name": "place_of_perform_county_na",
            "foreign_location_description": "place_of_performance_forei",
            "state_name": "place_of_perform_state_nam",
            "zip4": "place_of_performance_zip4a",
            "location_country_code": "place_of_perform_country_c"

        }

        place_of_performance_value_map = {
            "place_of_performance_flag": True
        }

        list_of_columns = (', '.join(legal_entity_location_field_map.values())) + ', ' + \
                          (', '.join(place_of_performance_field_map.values()))

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

                lel = get_or_create_location_pre_bulk(
                    legal_entity_location_field_map, row, legal_entity_location_value_map
                )

                # Create the place of performance location
                pop = get_or_create_location_pre_bulk(
                    place_of_performance_field_map, row, place_of_performance_value_map
                )

                if lel:
                    bulk_array.append(lel)
                if pop:
                    bulk_array.append(pop)

        logger.info('Bulk creating...')
        logger.info('BULK ARRAY: {}'.format(bulk_array))
        try:
            Location.objects.bulk_create(bulk_array)
        except IntegrityError:
            logger.info('Some dupes skipped...')


    @staticmethod
    def update_transaction_contract(db_cursor, fiscal_year=None, page=1, limit=500000, save=True):

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

        legal_entity_location_value_map = {
            "recipient_flag": True
        }

        place_of_performance_field_map = {
            # not sure place_of_performance_locat maps exactly to city name
            # "city_name": "place_of_performance_locat", # location id doesn't mean it's a city. Can't use this mapping
            "congressional_code": "place_of_performance_congr",
            "state_code": "place_of_performance_state",
            "zip4": "place_of_performance_zip4a",
            "location_country_code": "place_of_perform_country_c"
        }

        place_of_performance_value_map = {
            "place_of_performance_flag": True
        }

        list_of_columns = (', '.join(legal_entity_location_field_map.values())) + ', ' +  \
                          (', '.join(place_of_performance_field_map.values()))

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

        bulk_array = []

        start_time = datetime.now()
        for index, row in enumerate(procurement_data, 1):
            with db_transaction.atomic():

                if not (index % 100):
                    logger.info('D1 File Load: Added row {} of {} ({})'.format(str(index),
                                                                                 str(total_rows),
                                                                                 datetime.now() - start_time))

                lel = get_or_create_location_pre_bulk(
                    legal_entity_location_field_map, row, copy(legal_entity_location_value_map)
                )
                if lel:
                    bulk_array.append(lel)

                pop = get_or_create_location_pre_bulk(
                    place_of_performance_field_map, row, copy(place_of_performance_value_map))
                if pop:
                    bulk_array.append(pop)

        logger.info('Bulk creating...')
        logger.info('BULK ARRAY: '.format(bulk_array))
        try:
            Location.objects.bulk_create(bulk_array)
        except IntegrityError:
            logger.info('Some dupes skipped...')

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
            self.update_transaction_contract(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit, save=save)
            end = timeit.default_timer()
            logger.info('Finished D1 historical data location insert in ' + str(end - start) + ' seconds')

        if not options['contracts']:
            logger.info('Starting D2 historical data location insert...')
            start = timeit.default_timer()
            self.update_transaction_assistance(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit, save=save)
            end = timeit.default_timer()
            logger.info('Finished D2 historical data location insert in ' + str(end - start) + ' seconds')

        logger.info('FINISHED')


def get_or_create_location_pre_bulk(location_map, row, location_value_map):
    """
    Retrieve or create a location object

    Input parameters:
        - location_map: a dictionary with key = field name on the location model
            and value = corresponding field name on the current row of data
        - row: the row of data currently being loaded
    """
    if location_value_map is None:
        location_value_map = {}

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
            'country_name': location_country.country_name,
            'state_code': None,  # expired
            'state_name': None,
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
        if location_data.get('place_of_performance_flag') or location_data.get('recipient_flag'):

            # Create a new location object, handling the things in Location.save:
            '''self.load_country_data()
                self.load_city_county_data()
                 self.fill_missing_state_data()'''

            # self.load_country_data()
            if location_country is None:
                # TODO: Is this USA by default?
                if location_data.get('state_code'):
                    location_data['country_code'] = 'USA'
                else:
                    location_data['country_code'] = None

            # print(location_data)

            if location_data.get('country_code'):
                location_data['country_name'] = RefCountryCode.objects.get(country_code=location_data.get('country_code')).country_name

            # self.load_city_county_data() ** For USA only
            # Do a lookup, insert back into dict
            if location_data.get('country_code') == 'USA':

                ref_city_lkup_key = location_data.get('city_code', '') + location_data.get('county_code', '') + \
                                    location_data.get('state_code', '') + location_data.get('city_name', '') + \
                                    location_data.get('county_name', '')
                ref_city_lkup_key = ref_city_lkup_key.lower()
                ref_city_lkup_dict = {}
                for _column in county_code_columns:
                    _column_value = location_data.get(_column, '')
                    if _column_value:
                        ref_city_lkup_dict[_column] = _column_value

                if regex_solution:
                    values = [ref_city_lkup_dict[col] if col in ref_city_lkup_dict else ".*" for col in county_code_columns]
                    regex = "_".join(values)

                if ref_city_lkup_dict:
                    if dict_subset_solution:
                        matched_reference = list(filter(lambda d: d[0] if dict_is_subset(ref_city_lkup_dict, d[1]) else None, county_code_map.items()))
                    elif regex_solution:
                        matched_reference = [dict for key, dict in county_code_map.items() if re.match(regex, key)]
                    # print(matched_reference)
                    if len(matched_reference) == 1:
                        if dict_subset_solution:
                            matched_reference = matched_reference[0][1]
                        elif regex_solution:
                            matched_reference = matched_reference[0]
                        location_data['city_code'] = matched_reference.get('city_code')
                        location_data['county_code'] = matched_reference.get('county_code')
                        location_data['state_code'] = matched_reference.get('state_code')
                        location_data['city_name'] = matched_reference.get('city_name')
                        location_data['county_name'] = matched_reference.get('county_name')



            # self.fill_missing_state_data() ** For USA only
            if location_data.get('country_code') == 'USA' and \
                    (location_data.get('state_code') or location_data.get('state_name')):
                if not location_data.get('state_code'):
                    location_data['state_code'] = state_to_code.get(location_data['state_name'])
                elif not location_data.get('state_name'):
                    location_data['state_name'] = code_to_state.get(location_data['state_code'])

            # Populate everything!
            if location_data.get('country_code'):
                country_code_object = RefCountryCode.objects.get(country_code=location_data['country_code'])
            else:
                country_code_object = None


            new_loc = Location(
                location_country_code = country_code_object,
                country_name = location_data.get('country_name'),
                state_code = location_data.get('state_code'),
                state_name = location_data.get('state_name'),
                state_description = location_data.get('state_description'),
                city_name = location_data.get('city_name'),
                city_code = location_data.get('city_code'),
                county_name = location_data.get('county_name'),
                county_code = location_data.get('county_code'),
                address_line1 = location_data.get('address_line1'),
                address_line2 = location_data.get('address_line2'),
                address_line3 = location_data.get('address_line3'),
                foreign_location_description = location_data.get('foreign_location_description'),
                zip4 = '8675309',
                zip_4a = location_data.get('zip_4a'),
                congressional_code = location_data.get('congressional_code'),
                performance_code = location_data.get('performance_code'),
                zip_last4 = location_data.get('zip_last4'),
                zip5 = location_data.get('zip5'),
                foreign_postal_code = location_data.get('foreign_postal_code'),
                foreign_province = location_data.get('foreign_province'),
                foreign_city_name = location_data.get('foreign_city_name'),
                reporting_period_start = location_data.get('reporting_period_start'),
                reporting_period_end = location_data.get('reporting_period_end'),
                last_modified_date = location_data.get('last_modified_date'),
                certified_date = location_data.get('certified_date')
            )

            return new_loc

    else:
        # record had no location information at all
        return None
