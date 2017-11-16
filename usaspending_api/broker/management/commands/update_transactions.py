import logging
import timeit
from datetime import datetime
from usaspending_api.common.helpers import fy
from django.core.management.base import BaseCommand
from django.db import connections, transaction as db_transaction, IntegrityError

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, LegalEntity, SubtierAgency, ToptierAgency, Location
from usaspending_api.etl.management.load_base import copy, get_or_create_location, format_date, load_data_into_model
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards, update_award_categories

# start = timeit.default_timer()
# function_call
# end = timeit.default_timer()
# time elapsed = str(end - start)


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

# Lists to store for update_awards and update_contract_awards
award_update_id_list = []
award_contract_update_id_list = []

subtier_agency_map = {
    subtier_agency['subtier_code']: subtier_agency['subtier_agency_id']
    for subtier_agency in SubtierAgency.objects.values('subtier_code', 'subtier_agency_id')
    }
subtier_to_agency_map = {
    agency['subtier_agency_id']: {'agency_id': agency['id'], 'toptier_agency_id': agency['toptier_agency_id']}
    for agency in Agency.objects.values('id', 'toptier_agency_id', 'subtier_agency_id')
    }
toptier_agency_map = {
    toptier_agency['toptier_agency_id']: toptier_agency['cgac_code']
    for toptier_agency in ToptierAgency.objects.values('toptier_agency_id', 'cgac_code')
    }


class Command(BaseCommand):
    help = "Update historical transaction data for a fiscal year from the Broker."

    @staticmethod
    def update_transaction_assistance(db_cursor, fiscal_year=None, page=1, limit=500000):

        # logger.info("Getting IDs for what's currently in the DB...")
        # current_ids = TransactionFABS.objects
        #
        # if fiscal_year:
        #     current_ids = current_ids.filter(action_date__fy=fiscal_year)
        #
        # current_ids = current_ids.values_list('published_award_financial_assistance_id', flat=True)

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

        logger.info("Executing query on Broker DB => " + query % (arguments[0], arguments[1],
                                                                  arguments[2], arguments[3]))

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        award_financial_assistance_data = dictfetchall(db_cursor)

        legal_entity_location_field_map = {
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
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

        fad_field_map = {
            "type": "assistance_type",
            "description": "award_description",
        }

        logger.info("Getting total rows")
        # rows_loaded = len(current_ids)
        total_rows = len(award_financial_assistance_data)  # - rows_loaded

        logger.info("Processing " + str(total_rows) + " rows of assistance data")

        # skip_count = 0


# ROW ITERATION STARTS HERE

        lel_bulk = []
        pop_bulk = []
        legal_entity_bulk = []
        award_bulk = []

        transaction_assistance_bulk = []
        transaction_normalized_bulk = []

        logger.info('Getting legal entity location objects for {} rows...'.format(len(award_financial_assistance_data)))
        for index, row in enumerate(award_financial_assistance_data, 1):

            # Recipient flag is true for LeL
            legal_entity_location = get_or_create_location(
                legal_entity_location_field_map, row, {"recipient_flag": True}, save=False
            )

            lel_bulk.append(legal_entity_location)

        logger.info('Bulk creating {} legal entity location rows...'.format(len(lel_bulk)))
        try:
            Location.objects.bulk_create(lel_bulk)
        except IntegrityError:
            logger.info('!!! DUPLICATES FOUND. Continuing... ')

        logger.info('Getting place of performance objects for {} rows...'.format(len(award_financial_assistance_data)))
        for index, row in enumerate(award_financial_assistance_data, 1):

            # Place of Performance flag is true for PoP
            pop_location = get_or_create_location(
                place_of_performance_field_map, row, {"place_of_performance_flag": True}, save=False
            )

            pop_bulk.append(pop_location)

        logger.info('Bulk creating {} place of performance rows...'.format(len(pop_bulk)))
        try:
            Location.objects.bulk_create(pop_bulk)
        except IntegrityError:
            logger.info('!!! DUPLICATES FOUND. Continuing... ')

        logger.info('Getting legal entity objects for {} rows...'.format(len(award_financial_assistance_data)))
        for index, row in enumerate(award_financial_assistance_data, 1):

            recipient_name = row.get('awardee_or_recipient_legal', '')

            legal_entity = LegalEntity.objects.filter(recipient_unique_id=row['awardee_or_recipient_uniqu'],
                                                      recipient_name=recipient_name).first()

            if legal_entity is None:
                legal_entity = LegalEntity(recipient_unique_id=row['awardee_or_recipient_uniqu'],
                                           recipient_name=recipient_name)
                legal_entity_value_map = {
                    "location": lel_bulk[index - 1],
                }
                legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=False)

            legal_entity_bulk.append(legal_entity)

        logger.info('Bulk creating {} legal entity rows...'.format(len(legal_entity_bulk)))
        try:
            LegalEntity.objects.bulk_create(legal_entity_bulk)
        except IntegrityError:
            logger.info('!!! DUPLICATES FOUND. Continuing... ')

        awarding_agency_list = []
        funding_agency_list = []

        logger.info('Getting award objects for {} rows...'.format(len(award_financial_assistance_data)))
        for index, row in enumerate(award_financial_assistance_data, 1):

                # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
                # use the sub tier code to look it up. This code assumes that all incoming
                # records will supply an awarding subtier agency code
                if row['awarding_agency_code'] is None or len(row['awarding_agency_code'].strip()) < 1:
                    awarding_subtier_agency_id = subtier_agency_map[row["awarding_sub_tier_agency_c"]]
                    awarding_toptier_agency_id = subtier_to_agency_map[awarding_subtier_agency_id]['toptier_agency_id']
                    awarding_cgac_code = toptier_agency_map[awarding_toptier_agency_id]
                    row['awarding_agency_code'] = awarding_cgac_code

                # If funding toptier agency code (aka CGAC) is empty, try using the sub
                # tier funding code to look it up. Unlike the awarding agency, we can't
                # assume that the funding agency subtier code will always be present.
                if row['funding_agency_code'] is None or len(row['funding_agency_code'].strip()) < 1:
                    funding_subtier_agency_id = subtier_agency_map.get(row["funding_sub_tier_agency_co"])
                    if funding_subtier_agency_id is not None:
                        funding_toptier_agency_id = \
                            subtier_to_agency_map[funding_subtier_agency_id]['toptier_agency_id']
                        funding_cgac_code = toptier_agency_map[funding_toptier_agency_id]
                    else:
                        funding_cgac_code = None
                    row['funding_agency_code'] = funding_cgac_code

                # Find the award that this award transaction belongs to. If it doesn't exist, create it.
                awarding_agency = Agency.get_by_toptier_subtier(
                    row['awarding_agency_code'],
                    row["awarding_sub_tier_agency_c"]
                )
                funding_agency = Agency.get_by_toptier_subtier(
                    row['funding_agency_code'],
                    row["funding_sub_tier_agency_co"]
                )

                awarding_agency_list.append(awarding_agency)
                funding_agency_list.append(funding_agency)

                # award.save() is called in Award.get_or_create_summary_award by default
                created, award = Award.get_or_create_summary_award(
                    awarding_agency=awarding_agency,
                    fain=row.get('fain'),
                    uri=row.get('uri'),
                    save=False
                )

                award_bulk.append(award)
                award_update_id_list.append(award.id)

        logger.info('Bulk creating {} award rows...'.format(len(award_bulk)))
        try:
            Award.objects.bulk_create(award_bulk)
        except IntegrityError:
            logger.info('!!! DUPLICATES FOUND. Continuing... ')

        logger.info('Getting transaction_normalized for {} rows...'.format(len(award_financial_assistance_data)))
        for index, row in enumerate(award_financial_assistance_data, 1):

            parent_txn_value_map = {
                "award": award_bulk[index - 1],
                "awarding_agency": awarding_agency_list[index - 1],
                "funding_agency": funding_agency_list[index - 1],
                "recipient": legal_entity_bulk[index - 1],
                "place_of_performance": pop_bulk[index - 1],
                "period_of_performance_start_date": format_date(row['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
                "action_date": format_date(row['action_date']),
            }

            transaction_dict = load_data_into_model(
                TransactionNormalized(),  # thrown away
                row,
                field_map=fad_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            transaction_normalized = TransactionNormalized.get_or_create_transaction(**transaction_dict)
            transaction_normalized.fiscal_year = fy(transaction_normalized.action_date)
            transaction_normalized_bulk.append(transaction_normalized)

        logger.info('Bulk creating {} TransactionNormalized rows...'.format(len(transaction_normalized_bulk)))
        try:
            TransactionNormalized.objects.bulk_create(transaction_normalized_bulk)
        except IntegrityError:
            logger.info('Tried and failed to insert duplicate transaction_normalized row. Continuing... ')

        for index, row in enumerate(award_financial_assistance_data, 1):
            financial_assistance_data = load_data_into_model(
                TransactionFABS(),  # thrown away
                row,
                as_dict=True)

            transaction_assistance = TransactionFABS(transaction=transaction_normalized_bulk[index - 1],
                                                     **financial_assistance_data)
            transaction_assistance_bulk.append(transaction_assistance)

        logger.info('Bulk creating TransactionFABS rows...')
        try:
            TransactionFABS.objects.bulk_create(transaction_assistance_bulk)
        except IntegrityError:
            logger.info('!!! DUPLICATES FOUND. Continuing... ')


######################################################

    @staticmethod
    def update_transaction_contract(db_cursor, fiscal_year=None, page=1, limit=500000):

        # logger.info("Getting IDs for what's currently in the DB...")
        # current_ids = TransactionFPDS.objects
        #
        # if fiscal_year:
        #     current_ids = current_ids.filter(action_date__fy=fiscal_year)
        #
        # current_ids = current_ids.values_list('detached_award_procurement_id', flat=True)

        query = "SELECT * FROM detached_award_procurement"
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
        arguments += [limit, (page-1)*limit]

        logger.info("Executing query on Broker DB => " + query % (arguments[0], arguments[1],
                                                                  arguments[2], arguments[3]))

        db_cursor.execute(query, arguments)

        logger.info("Running dictfetchall on db_cursor")
        procurement_data = dictfetchall(db_cursor)

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

        contract_field_map = {
            "type": "contract_award_type",
            "description": "award_description"
        }

        logger.info("Getting total rows")
        # rows_loaded = len(current_ids)
        total_rows = len(procurement_data)  # - rows_loaded

        logger.info("Processing " + str(total_rows) + " rows of procurement data")

        # skip_count = 0

        start_time = datetime.now()
        for index, row in enumerate(procurement_data, 1):
            with db_transaction.atomic():
                # if TransactionFPDS.objects.values('detached_award_procurement_id').\
                #         filter(detached_award_procurement_id=str(row['detached_award_procurement_id'])).first():
                #     skip_count += 1
                #
                #     if not (skip_count % 100):
                #         logger.info('Skipped {} records so far'.format(str(skip_count)))

                if not (index % 100):
                    logger.info('D1 File Load: Loading row {} of {} ({})'.format(str(index),
                                                                                 str(total_rows),
                                                                                 datetime.now() - start_time))

                recipient_name = row['awardee_or_recipient_legal']
                if recipient_name is None:
                    recipient_name = ""

                legal_entity_location, created = get_or_create_location(
                    legal_entity_location_field_map, row, copy(legal_entity_location_value_map)
                )

                # Create the legal entity if it doesn't exist
                legal_entity, created = LegalEntity.objects.get_or_create(
                    recipient_unique_id=row['awardee_or_recipient_uniqu'],
                    recipient_name=recipient_name
                )

                if created:
                    legal_entity_value_map = {
                        "location": legal_entity_location,
                    }
                    legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

                # Create the place of performance location
                pop_location, created = get_or_create_location(
                    place_of_performance_field_map, row, copy(place_of_performance_value_map))

                # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
                # use the sub tier code to look it up. This code assumes that all incoming
                # records will supply an awarding subtier agency code
                if row['awarding_agency_code'] is None or len(row['awarding_agency_code'].strip()) < 1:
                    awarding_subtier_agency_id = subtier_agency_map[row["awarding_sub_tier_agency_c"]]
                    awarding_toptier_agency_id = subtier_to_agency_map[awarding_subtier_agency_id]['toptier_agency_id']
                    awarding_cgac_code = toptier_agency_map[awarding_toptier_agency_id]
                    row['awarding_agency_code'] = awarding_cgac_code

                # If funding toptier agency code (aka CGAC) is empty, try using the sub
                # tier funding code to look it up. Unlike the awarding agency, we can't
                # assume that the funding agency subtier code will always be present.
                if row['funding_agency_code'] is None or len(row['funding_agency_code'].strip()) < 1:
                    funding_subtier_agency_id = subtier_agency_map.get(row["funding_sub_tier_agency_co"])
                    if funding_subtier_agency_id is not None:
                        funding_toptier_agency_id = \
                            subtier_to_agency_map[funding_subtier_agency_id]['toptier_agency_id']
                        funding_cgac_code = toptier_agency_map[funding_toptier_agency_id]
                    else:
                        funding_cgac_code = None
                    row['funding_agency_code'] = funding_cgac_code

                # Find the award that this award transaction belongs to. If it doesn't exist, create it.
                awarding_agency = Agency.get_by_toptier_subtier(
                    row['awarding_agency_code'],
                    row["awarding_sub_tier_agency_c"]
                )
                created, award = Award.get_or_create_summary_award(
                    awarding_agency=awarding_agency,
                    piid=row.get('piid'),
                    fain=row.get('fain'),
                    uri=row.get('uri'),
                    parent_award_id=row.get('parent_award_id'))
                award.save()

                award_update_id_list.append(award.id)
                award_contract_update_id_list.append(award.id)

                parent_txn_value_map = {
                    "award": award,
                    "awarding_agency": awarding_agency,
                    "funding_agency": Agency.get_by_toptier_subtier(row['funding_agency_code'],
                                                                    row["funding_sub_tier_agency_co"]),
                    "recipient": legal_entity,
                    "place_of_performance": pop_location,
                    "period_of_performance_start_date": format_date(row['period_of_performance_star']),
                    "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
                    "action_date": format_date(row['action_date']),
                }

                transaction_dict = load_data_into_model(
                    TransactionNormalized(),  # thrown away
                    row,
                    field_map=contract_field_map,
                    value_map=parent_txn_value_map,
                    as_dict=True)

                transaction = TransactionNormalized.get_or_create_transaction(**transaction_dict)
                transaction.save()

                contract_instance = load_data_into_model(
                    TransactionFPDS(),  # thrown away
                    row,
                    as_dict=True)

                transaction_contract = TransactionFPDS(transaction=transaction, **contract_instance)
                # catch exception and do nothing if we see
                # "django.db.utils.IntegrityError: duplicate key value violates unique constraint"
                try:
                    transaction_contract.save()
                except IntegrityError:
                    pass

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
            help='Runs the historical loader only for Award Financial Assistance (Assistance) data'
        )

        parser.add_argument(
            '--contracts',
            action='store_true',
            dest='contracts',
            default=False,
            help='Runs the historical loader only for Award Procurement (Contract) data'
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

    # @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting historical data load...')

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

        if not options['assistance']:
            logger.info('Starting D1 historical data load...')
            start = timeit.default_timer()
            self.update_transaction_contract(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit)
            end = timeit.default_timer()
            logger.info('Finished D1 historical data load in ' + str(end - start) + ' seconds')

        if not options['contracts']:
            logger.info('Starting D2 historical data load...')
            start = timeit.default_timer()
            self.update_transaction_assistance(db_cursor=db_cursor, fiscal_year=fiscal_year, page=page, limit=limit)
            end = timeit.default_timer()
            logger.info('Finished D2 historical data load in ' + str(end - start) + ' seconds')

        logger.info('Updating awards to reflect their latest associated transaction info...')
        start = timeit.default_timer()
        update_awards(tuple(award_update_id_list))
        end = timeit.default_timer()
        logger.info('Finished updating awards in ' + str(end - start) + ' seconds')

        logger.info('Updating contract-specific awards to reflect their latest transaction info...')
        start = timeit.default_timer()
        update_contract_awards(tuple(award_contract_update_id_list))
        end = timeit.default_timer()
        logger.info('Finished updating contract specific awards in ' + str(end - start) + ' seconds')

        logger.info('Updating award category variables...')
        start = timeit.default_timer()
        update_award_categories(tuple(award_update_id_list))
        end = timeit.default_timer()
        logger.info('Finished updating award category variables in ' + str(end - start) + ' seconds')

        # Done!
        logger.info('FINISHED')
