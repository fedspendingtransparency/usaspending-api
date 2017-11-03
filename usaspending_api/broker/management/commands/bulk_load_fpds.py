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
    "location_country_code": "place_of_perform_country_c"

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
        self.country_code_map = {country.country_code: country for country in RefCountryCode.objects.all()}
        self.subtier_agency_map = {
            subtier_agency['subtier_code']: subtier_agency['subtier_agency_id']
            for subtier_agency in SubtierAgency.objects.values('subtier_code', 'subtier_agency_id')
            }
        self.subtier_to_agency_map = {
            agency['subtier_agency_id']: {'agency_id': agency['id'], 'toptier_agency_id': agency['toptier_agency_id']}
            for agency in Agency.objects.values('id', 'toptier_agency_id', 'subtier_agency_id')
            }
        self.toptier_agency_map = {
            toptier_agency['toptier_agency_id']: toptier_agency['cgac_code']
            for toptier_agency in ToptierAgency.objects.values('toptier_agency_id', 'cgac_code')
            }
        self.agency_no_sub_map = {
            (agency.toptier_agency.cgac_code, agency.subtier_agency.subtier_code): agency
            for agency in Agency.objects.filter(subtier_agency__isnull=False)
            }
        self.agency_sub_only_map = {
            agency.toptier_agency.cgac_code: agency
            for agency in Agency.objects.filter(subtier_agency__isnull=True)
            }
        self.agency_toptier_map = {
            agency.toptier_agency.cgac_code: agency
            for agency in Agency.objects.filter(toptier_flag=True)
            }
        self.award_map = {award.piid: award for award in Award.objects.filter(piid__isnull=False)}
        self.le_map = {(le.recipient_unique_id, le.recipient_name): le for le in LegalEntity.objects.all()}

    def diff_fpds_data(self, db_cursor, ds_cursor, fiscal_year=None):
        db_query = 'SELECT detached_award_procurement_id ' \
                'FROM detached_award_procurement'
        db_arguments = []

        ds_query = 'SELECT detached_award_procurement_id ' \
                   'FROM transaction_fpds'
        ds_arguments = []

        if fiscal_year:
            if db_arguments:
                db_query += ' AND'
            else:
                db_query += ' WHERE'

            if ds_arguments:
                ds_query += ' AND'
            else:
                ds_query += ' WHERE'

            fy_begin = '10/01/' + str(fiscal_year - 1)
            fy_end = '09/30/' + str(fiscal_year)

            db_query += ' action_date::Date BETWEEN %s AND %s'
            db_arguments += [fy_begin, fy_end]

            ds_query += ' action_date::Date BETWEEN %s AND %s'
            ds_arguments += [fy_begin, fy_end]

        db_cursor.execute(db_query, db_arguments)
        ds_cursor.execute(ds_query, ds_arguments)

        db_dict = dictfetchall(db_cursor)
        ds_dict = dictfetchall(ds_cursor)

        db_set = set(map(lambda db_entry: int(db_entry['detached_award_procurement_id']), db_dict))
        ds_set = set(map(lambda ds_entry: int(ds_entry['detached_award_procurement_id']), ds_dict))

        to_insert = db_set - ds_set
        to_delete = ds_set - db_set

        logger.info('Number of records to insert: %s' % str(len(to_insert)))
        logger.info('Number of records to delete: %s' % str(len(to_delete)))

        # Return what is not currently in our database (to insert) and what we have that Broker does not (to delete)
        return to_insert, to_delete

    def get_fpds_data(self, db_cursor, fiscal_year=None, to_insert=None):
        query = 'SELECT * FROM detached_award_procurement'
        arguments = []

        if to_insert:
            if arguments:
                query += ' AND'
            else:
                query += ' WHERE'
            query += ' detached_award_procurement_id IN %s'
            arguments += [tuple(to_insert)]

        if fiscal_year:
            if arguments:
                query += ' AND'
            else:
                query += ' WHERE'
            fy_begin = '10/01/' + str(fiscal_year - 1)
            fy_end = '09/30/' + str(fiscal_year)
            query += ' action_date::Date BETWEEN %s AND %s'
            arguments += [fy_begin, fy_end]

        query += ' ORDER BY detached_award_procurement_id'

        logger.info("Executing select query on Broker DB")

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        return dictfetchall(db_cursor)

    def load_locations(self, fpds_broker_data, total_rows, pop_flag=False):

        start_time = datetime.now()
        for index, row in enumerate(fpds_broker_data, 1):
            if not (index % 10000):
                logger.info('Locations: Loading row {} of {} ({})'.format(str(index),
                                                                          str(total_rows),
                                                                          datetime.now() - start_time))
            if pop_flag:
                location_value_map = {"place_of_performance_flag": True}
                field_map = pop_field_map
            else:
                location_value_map = {'recipient_flag': True}
                field_map = le_field_map

            row = canonicalize_location_dict(row)

            # THIS ASSUMPTION DOES NOT HOLD FOR FPDS SINCE IT DOES NOT HAVE A PLACE OF PERFORMANCE CODE
            # We can assume that if the country code is blank and the place of performance code is NOT '00FORGN', then
            # the country code is USA
            # if pop_flag and not country_code and pop_code != '00FORGN':
            #     row[field_map.get('location_country_code')] = 'USA'

            # Get country code obj
            location_country_code_obj = self.country_code_map.get(row[field_map.get('location_country_code')])

            # Fix state code periods
            state_code = row.get(field_map.get('state_code'))
            if state_code is not None:
                location_value_map.update({'state_code': state_code.replace('.', '')})

            if location_country_code_obj:
                location_value_map.update({
                    'location_country_code': location_country_code_obj,
                    'country_name': location_country_code_obj.country_name
                })

                if location_country_code_obj.country_code != 'USA':
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

            location_instance_data = load_data_into_model(
                Location(),
                row,
                value_map=location_value_map,
                field_map=field_map,
                as_dict=True)

            loc_instance = Location(**location_instance_data)
            loc_instance.load_country_data()
            loc_instance.load_city_county_data()
            loc_instance.fill_missing_state_data()

            if pop_flag:
                pop_bulk.append(loc_instance)
            else:
                lel_bulk.append(loc_instance)

        if pop_flag:
            logger.info('Bulk creating POP Locations (batch_size: {})...'.format(BATCH_SIZE))
            Location.objects.bulk_create(pop_bulk, batch_size=BATCH_SIZE)
        else:
            logger.info('Bulk creating LE Locations (batch_size: {})...'.format(BATCH_SIZE))
            Location.objects.bulk_create(lel_bulk, batch_size=BATCH_SIZE)

    def load_legal_entity(self, fpds_broker_data, total_rows):

        start_time = datetime.now()
        for index, row in enumerate(fpds_broker_data, 1):
            if not (index % 10000):
                logger.info('Legal Entity: Loading row {} of {} ({})'.format(str(index),
                                                                             str(total_rows),
                                                                             datetime.now() - start_time))

            recipient_name = row['awardee_or_recipient_legal']
            if recipient_name is None:
                recipient_name = ''
            recipient_unique_id = row['awardee_or_recipient_uniqu']

            lookup_key = (recipient_unique_id, recipient_name)
            legal_entity = self.le_map.get(lookup_key)

            if not legal_entity:
                legal_entity = LegalEntity(
                    recipient_unique_id=row['awardee_or_recipient_uniqu'],
                    recipient_name=recipient_name
                )

                legal_entity = load_data_into_model(
                    legal_entity,
                    row,
                    value_map={"location": lel_bulk[index-1]},
                    save=False)

                LegalEntity.update_business_type_categories(legal_entity)

                self.le_map[lookup_key] = legal_entity
                legal_entity_bulk.append(legal_entity)
            legal_entity_lookup.append(legal_entity)

        logger.info('Bulk creating Legal Entities (batch_size: {})...'.format(BATCH_SIZE))
        LegalEntity.objects.bulk_create(legal_entity_bulk, batch_size=BATCH_SIZE)

    def load_parent_awards(self, fpds_broker_data, total_rows):
        start_time = datetime.now()
        for index, row in enumerate(fpds_broker_data, 1):
            if not (index % 10000):
                logger.info('Parent Awards: Loading row {} of {} ({})'.format(str(index),
                                                                              str(total_rows),
                                                                              datetime.now() - start_time))

            # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
            # use the sub tier code to look it up. This code assumes that all incoming
            # records will supply an awarding subtier agency code
            if row['awarding_agency_code'] is None or len(row['awarding_agency_code'].strip()) < 1:
                awarding_subtier_agency_id = self.subtier_agency_map[row["awarding_sub_tier_agency_c"]]
                awarding_toptier_agency_id = self.subtier_to_agency_map[awarding_subtier_agency_id]['toptier_agency_id']
                awarding_cgac_code = self.toptier_agency_map[awarding_toptier_agency_id]
                row['awarding_agency_code'] = awarding_cgac_code

            # Find the award that this award transaction belongs to. If it doesn't exist, create it.
            awarding_agency = self.agency_no_sub_map.get((
                row['awarding_agency_code'],
                row["awarding_sub_tier_agency_c"]
            ))

            if awarding_agency is None:
                awarding_agency = self.agency_sub_only_map.get(row['awarding_agency_code'])

            # parent_award_id from the row = parent piid
            parent_award_piid = row.get('parent_award_id')
            parent_award = None
            if parent_award_piid:
                parent_award = self.award_map.get(parent_award_piid)
                if not parent_award:
                    create_kwargs = {'awarding_agency': awarding_agency, 'piid': parent_award_piid}
                    parent_award = Award(**create_kwargs)
                    self.award_map[parent_award_piid] = parent_award
                    parent_award_bulk.append(parent_award)

            parent_award_lookup.append(parent_award)

        logger.info('Bulk creating Parent Awards (batch_size: {})...'.format(BATCH_SIZE))
        Award.objects.bulk_create(parent_award_bulk, batch_size=BATCH_SIZE)

    def load_awards(self, fpds_broker_data, total_rows):
        start_time = datetime.now()
        for index, row in enumerate(fpds_broker_data, 1):
            if not (index % 10000):
                logger.info('Awards: Loading row {} of {} ({})'.format(str(index),
                                                                       str(total_rows),
                                                                       datetime.now() - start_time))

            # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
            # use the sub tier code to look it up. This code assumes that all incoming
            # records will supply an awarding subtier agency code
            if row['awarding_agency_code'] is None or len(row['awarding_agency_code'].strip()) < 1:
                awarding_subtier_agency_id = self.subtier_agency_map[row["awarding_sub_tier_agency_c"]]
                awarding_toptier_agency_id = self.subtier_to_agency_map[awarding_subtier_agency_id]['toptier_agency_id']
                awarding_cgac_code = self.toptier_agency_map[awarding_toptier_agency_id]
                row['awarding_agency_code'] = awarding_cgac_code

            # If funding toptier agency code (aka CGAC) is empty, try using the sub
            # tier funding code to look it up. Unlike the awarding agency, we can't
            # assume that the funding agency subtier code will always be present.
            if row['funding_agency_code'] is None or len(row['funding_agency_code'].strip()) < 1:
                funding_subtier_agency_id = self.subtier_agency_map.get(row["funding_sub_tier_agency_co"])
                if funding_subtier_agency_id is not None:
                    funding_toptier_agency_id = self.subtier_to_agency_map[funding_subtier_agency_id][
                        'toptier_agency_id']
                    funding_cgac_code = self.toptier_agency_map[funding_toptier_agency_id]
                else:
                    funding_cgac_code = None
                row['funding_agency_code'] = funding_cgac_code

            # Find the award that this award transaction belongs to. If it doesn't exist, create it.
            awarding_agency = self.agency_no_sub_map.get((
                row['awarding_agency_code'],
                row["awarding_sub_tier_agency_c"]
            ))

            if awarding_agency is None:
                awarding_agency = self.agency_sub_only_map.get(row['awarding_agency_code'])

            funding_agency = self.agency_no_sub_map.get((
                row['funding_agency_code'],
                row["funding_sub_tier_agency_co"]
            ))

            if funding_agency is None:
                funding_agency = self.agency_sub_only_map.get(row['funding_agency_code'])

            awarding_agency_list.append(awarding_agency)
            funding_agency_list.append(funding_agency)

            piid = row.get('piid')
            award = self.award_map.get(piid)
            if award and awarding_agency is not None and award.awarding_agency_id is not None:
                if award.awarding_agency_id != awarding_agency.id:
                    award = None

            if not award:
                # create the award since it wasn't found
                create_kwargs = {'awarding_agency': awarding_agency, 'piid': piid}
                award = Award(**create_kwargs)
                award.parent_award = parent_award_lookup[index-1]
                self.award_map[piid] = award
                award_bulk.append(award)

            award_lookup.append(award)

        logger.info('Bulk creating Awards (batch_size: {})...'.format(BATCH_SIZE))
        Award.objects.bulk_create(award_bulk, batch_size=BATCH_SIZE)

    def load_transaction_normalized(self, fpds_broker_data, total_rows):
        start_time = datetime.now()
        for index, row in enumerate(fpds_broker_data, 1):
            if not (index % 10000):
                logger.info('Transaction Normalized: Loading row {} of {} ({})'.format(str(index),
                                                                                       str(total_rows),
                                                                                       datetime.now() - start_time))
            parent_txn_value_map = {
                "award": award_lookup[index - 1],
                "awarding_agency": awarding_agency_list[index - 1],
                "funding_agency": funding_agency_list[index - 1],
                "recipient": legal_entity_lookup[index - 1],
                "place_of_performance": pop_bulk[index - 1],
                "period_of_performance_start_date": format_date(row['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
                "action_date": format_date(row['action_date']),
            }

            contract_field_map = {
                "type": "contract_award_type",
                "description": "award_description"
            }

            transaction_normalized = load_data_into_model(
                TransactionNormalized(),
                row,
                field_map=contract_field_map,
                value_map=parent_txn_value_map,
                as_dict=False,
                save=False)

            transaction_normalized.fiscal_year = fy(transaction_normalized.action_date)
            transaction_normalized_bulk.append(transaction_normalized)

        logger.info('Bulk creating Transaction Normalized (batch_size: {})...'.format(BATCH_SIZE))
        TransactionNormalized.objects.bulk_create(transaction_normalized_bulk, batch_size=BATCH_SIZE)

    def load_transaction_fpds(self, fpds_broker_data, total_rows):
        logger.info('Starting bulk loading for FPDS data')

        start_time = datetime.now()
        for index, row in enumerate(fpds_broker_data, 1):
            if not (index % 10000):
                logger.info('Transaction FPDS: Loading row {} of {} ({})'.format(str(index),
                                                                                 str(total_rows),
                                                                                 datetime.now() - start_time))

            fpds_instance_data = load_data_into_model(
                TransactionFPDS(),  # thrown away
                row,
                as_dict=True)

            fpds_instance = TransactionFPDS(**fpds_instance_data)
            fpds_instance.transaction = transaction_normalized_bulk[index-1]
            fpds_bulk.append(fpds_instance)

        logger.info('Bulk creating Transaction FPDS (batch_size: {})...'.format(BATCH_SIZE))
        TransactionFPDS.objects.bulk_create(fpds_bulk, batch_size=BATCH_SIZE)

    def delete_stale_fpds(self, to_delete=None):

        if not to_delete:
            return

        # This cascades deletes for TransactionFPDS & Awards in addition to deleting TransactionNormalized records
        TransactionNormalized.objects.filter(contract_data__detached_award_procurement_id__in=to_delete).delete()

    def add_arguments(self, parser):

        parser.add_argument(
            '--fiscal_year',
            dest="fiscal_year",
            nargs='+',
            type=int,
            help="Year for which to run the historical load"
        )

    @db_transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting FPDS bulk data load...')

        db_cursor = connections['data_broker'].cursor()
        ds_cursor = connection.cursor()
        fiscal_year = options.get('fiscal_year')

        if fiscal_year:
            fiscal_year = fiscal_year[0]
        else:
            fiscal_year = 2017

        logger.info('Processing data for Fiscal Year ' + str(fiscal_year))

        logger.info('Diff-ing FPDS data...')
        start = timeit.default_timer()
        to_insert, to_delete = self.diff_fpds_data(db_cursor=db_cursor, ds_cursor=ds_cursor, fiscal_year=fiscal_year)
        end = timeit.default_timer()
        logger.info('Finished diff-ing FPDS data in ' + str(end - start) + ' seconds')

        total_rows = len(to_insert)
        total_rows_delete = len(to_delete)

        if total_rows_delete > 0:
            logger.info('Deleting stale FPDS data...')
            start = timeit.default_timer()
            self.delete_stale_fpds(to_delete=to_delete)
            end = timeit.default_timer()
            logger.info('Finished deleting stale FPDS data in ' + str(end - start) + ' seconds')

        if total_rows > 0:
            # Set lookups after deletions to only get latest
            self.set_lookup_maps()

            logger.info('Get Broker FPDS data...')
            start = timeit.default_timer()
            fpds_broker_data = self.get_fpds_data(db_cursor=db_cursor, fiscal_year=fiscal_year, to_insert=to_insert)
            end = timeit.default_timer()
            logger.info('Finished getting Broker FPDS data in ' + str(end - start) + ' seconds')

            logger.info('Loading POP Location data...')
            start = timeit.default_timer()
            self.load_locations(fpds_broker_data=fpds_broker_data, total_rows=total_rows, pop_flag=True)
            end = timeit.default_timer()
            logger.info('Finished POP Location bulk data load in ' + str(end - start) + ' seconds')

            logger.info('Loading LE Location data...')
            start = timeit.default_timer()
            self.load_locations(fpds_broker_data=fpds_broker_data, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished LE Location bulk data load in ' + str(end - start) + ' seconds')

            logger.info('Loading Legal Entity data...')
            start = timeit.default_timer()
            self.load_legal_entity(fpds_broker_data=fpds_broker_data, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished Legal Entity bulk data load in ' + str(end - start) + ' seconds')

            logger.info('Loading Parent Award data...')
            start = timeit.default_timer()
            self.load_parent_awards(fpds_broker_data=fpds_broker_data, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished Parent Award bulk data load in ' + str(end - start) + ' seconds')

            logger.info('Loading Award data...')
            start = timeit.default_timer()
            self.load_awards(fpds_broker_data=fpds_broker_data, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished Award bulk data load in ' + str(end - start) + ' seconds')

            logger.info('Loading Transaction Normalized data...')
            start = timeit.default_timer()
            self.load_transaction_normalized(fpds_broker_data=fpds_broker_data, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished Transaction Normalized bulk data load in ' + str(end - start) + ' seconds')

            logger.info('Loading Transaction FPDS data...')
            start = timeit.default_timer()
            self.load_transaction_fpds(fpds_broker_data=fpds_broker_data, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished FPDS bulk data load in ' + str(end - start) + ' seconds')

            award_update_id_list = [award.id for award in award_lookup]

            logger.info('Updating awards to reflect their latest associated transaction info...')
            start = timeit.default_timer()
            update_awards(tuple(award_update_id_list))
            end = timeit.default_timer()
            logger.info('Finished updating awards in ' + str(end - start) + ' seconds')

            logger.info('Updating contract-specific awards to reflect their latest transaction info...')
            start = timeit.default_timer()
            update_contract_awards(tuple(award_update_id_list))
            end = timeit.default_timer()
            logger.info('Finished updating contract specific awards in ' + str(end - start) + ' seconds')

            logger.info('Updating award category variables...')
            start = timeit.default_timer()
            update_award_categories(tuple(award_update_id_list))
            end = timeit.default_timer()
            logger.info('Finished updating award category variables in ' + str(end - start) + ' seconds')
        else:
            logger.info('Nothing to insert...FINISHED!')
