import logging
import timeit
from datetime import datetime

from django.core.management.base import BaseCommand
from django.db import connections, transaction as db_transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.broker.models import TransactionNew, TransactionAssistanceNew, TransactionContractNew
from usaspending_api.awards.models import Transaction, TransactionAssistance, TransactionContract, Award
from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, LegalEntity
from usaspending_api.etl.management.load_base import copy, get_or_create_location, format_date, load_data_into_model
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards, update_award_categories

# start = timeit.default_timer()
# function_call
# end = timeit.default_timer()
# time elapsed = str(end - start)


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

# Lists to store for update_awards and update_contract_awards
AWARD_UPDATE_ID_LIST = []
AWARD_CONTRACT_UPDATE_ID_LIST = []


class Command(BaseCommand):
    help = "Checks what TASs are in Broker but not in Data Store"

    @staticmethod
    def update_transaction_assistance(db_cursor, fiscal_year=None, start_row=1):
        query = 'SELECT * FROM published_award_financial_assistance'
        arguments = []
        if fiscal_year:
            query += ' WHERE FY(action_date) = %s'
            arguments = [fiscal_year]
        query += ' ORDER BY afa_generated_unique'
        db_cursor.execute(query, arguments)
        award_financial_assistance_data = dictfetchall(db_cursor)

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

        fad_field_map = {
            "type": "assistance_type",
            "description": "award_description",
        }

        total_rows = len(award_financial_assistance_data)

        start_time = datetime.now()
        for index, row in enumerate(award_financial_assistance_data, start_row):
            if not (index % 100):
                logger.info('D2 File Load: Loading row {} of {} ({})'.format(str(index),
                                                                             str(total_rows),
                                                                             datetime.now() - start_time))

            legal_entity_location, created = get_or_create_location(
                legal_entity_location_field_map, row, legal_entity_location_value_map
            )

            recipient_name = row['awardee_or_recipient_legal']
            if recipient_name is None:
                recipient_name = ""

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
                place_of_performance_field_map, row, place_of_performance_value_map
            )

            # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
            # use the sub tier code to look it up. This code assumes that all incoming
            # records will supply an awarding subtier agency code
            if row['awarding_agency_code'] is None or len(row['awarding_agency_code'].strip()) < 1:
                row['awarding_agency_code'] = Agency.get_by_subtier(
                    row["awarding_sub_tier_agency_c"]).toptier_agency.cgac_code
            # If funding toptier agency code (aka CGAC) is empty, try using the sub
            # tier funding code to look it up. Unlike the awarding agency, we can't
            # assume that the funding agency subtier code will always be present.
            if row['funding_agency_code'] is None or len(row['funding_agency_code'].strip()) < 1:
                funding_agency = Agency.get_by_subtier(row["funding_sub_tier_agency_co"])
                row['funding_agency_code'] = (
                    funding_agency.toptier_agency.cgac_code if funding_agency is not None
                    else None)

            # Find the award that this award transaction belongs to. If it doesn't exist, create it.
            awarding_agency = Agency.get_by_toptier_subtier(
                row['awarding_agency_code'],
                row["awarding_sub_tier_agency_c"]
            )
            created, award = Award.get_or_create_summary_award(
                awarding_agency=awarding_agency,
                # piid=transaction_assistance.get('piid'), # not found
                fain=row.get('fain'),
                uri=row.get('uri'))
                # parent_award_id=transaction_assistance.get('parent_award_id')) # not found
            award.save()

            AWARD_UPDATE_ID_LIST.append(award.id)

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
                TransactionNew(),  # thrown away
                row,
                field_map=fad_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            transaction = TransactionNew.get_or_create_transaction(**transaction_dict)
            transaction.save()

            financial_assistance_data = load_data_into_model(
                TransactionAssistanceNew(),  # thrown away
                row,
                as_dict=True)

            transaction_assistance = TransactionAssistanceNew.get_or_create_2(transaction=transaction,
                                                                           **financial_assistance_data)
            transaction_assistance.save()


    @staticmethod
    def update_transaction_contract(db_cursor, fiscal_year=None, start_row=1):

        logger.info("Getting IDs for what's currently in the DB...")
        current_ids = TransactionContractNew.objects.values_list('detached_award_procurement_id', flat=True)
        current_ids_str = tuple(current_ids)  # str(current_ids).replace('[', '(').replace(']', ')')

        query = ""
        arguments = []

        if fiscal_year:
            query += "WITH fy_filtered_detached_award_procurement AS " \
                     "(SELECT * FROM detached_award_procurement WHERE FY(action_date) = %s) "
            arguments += [fiscal_year]

        query += "SELECT * FROM fy_filtered_detached_award_procurement"

        if current_ids:
            query += " WHERE detached_award_procurement_id NOT IN %s"
            arguments += [current_ids_str]

        # query += ' ORDER BY detached_award_proc_unique'

        logger.info("Executing query on Broker DB => " + query)

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
        total_rows = len(procurement_data)

        logger.info("Processing " + str(total_rows) + " rows of procurement data")

        start_time = datetime.now()
        for index, row in enumerate(procurement_data, start_row):
            with db_transaction.atomic():
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

                # If awarding toptier agency code (aka CGAC) is not supplied on the D1 record,
                # use the sub tier code to look it up. This code assumes that all incoming
                # records will supply an awarding subtier agency code
                if row['awarding_agency_code'] is None or len(row['awarding_agency_code'].strip()) < 1:
                    row['awarding_agency_code'] = Agency.get_by_subtier(
                        row["awarding_sub_tier_agency_c"]).toptier_agency.cgac_code
                # If funding toptier agency code (aka CGAC) is empty, try using the sub
                # tier funding code to look it up. Unlike the awarding agency, we can't
                # assume that the funding agency subtier code will always be present.
                if row['funding_agency_code'] is None or len(row['funding_agency_code'].strip()) < 1:
                    funding_agency = Agency.get_by_subtier(row["funding_sub_tier_agency_co"])
                    row['funding_agency_code'] = (
                        funding_agency.toptier_agency.cgac_code if funding_agency is not None
                        else None)

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

                AWARD_UPDATE_ID_LIST.append(award.id)
                AWARD_CONTRACT_UPDATE_ID_LIST.append(award.id)

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
                    TransactionNew(),  # thrown away
                    row,
                    field_map=contract_field_map,
                    value_map=parent_txn_value_map,
                    as_dict=True)

                transaction = TransactionNew.get_or_create_transaction(**transaction_dict)
                transaction.save()

                contract_instance = load_data_into_model(
                    TransactionContractNew(),  # thrown away
                    row,
                    as_dict=True)

                transaction_contract = TransactionContractNew(transaction=transaction, **contract_instance)
                transaction_contract.save()

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
            '--contracts_row',
            dest="contracts_row",
            nargs='+',
            type=int,
            help="Row to start contracts"
        )

        parser.add_argument(
            '--assistance_row',
            dest="assistance_row",
            nargs='+',
            type=int,
            help="Row to start assistance"
        )

    # @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting historical data load...')

        db_cursor = connections['data_broker'].cursor()
        fiscal_year = options.get('fiscal_year')
        contracts_row = options.get('contracts_row')
        assistance_row = options.get('assistance_row')

        if fiscal_year:
            fiscal_year = fiscal_year[0]
            logger.info('Processing data for Fiscal Year ' + str(fiscal_year))
        else:
            fiscal_year = 2017

        contracts_row = contracts_row[0] if contracts_row else 1
        assistance_row = assistance_row[0] if assistance_row else 1
        
        if not options['assistance']:
            logger.info('Starting D1 historical data load...')
            start = timeit.default_timer()
            self.update_transaction_contract(db_cursor = db_cursor, fiscal_year=fiscal_year, start_row=contracts_row)
            end = timeit.default_timer()
            logger.info('Finished D1 historical data load in ' + str(end - start) + ' seconds')

        if not options['contracts']:
            logger.info('Starting D2 historical data load...')
            start = timeit.default_timer()
            self.update_transaction_assistance(db_cursor = db_cursor, fiscal_year=fiscal_year, start_row=assistance_row)
            end = timeit.default_timer()
            logger.info('Finished D2 historical data load in ' + str(end - start) + ' seconds')

        logger.info('Updating awards to reflect their latest associated transaction info...')
        start = timeit.default_timer()
        update_awards(tuple(AWARD_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating awards in ' + str(end - start) + ' seconds')

        logger.info('Updating contract-specific awards to reflect their latest transaction info...')
        start = timeit.default_timer()
        update_contract_awards(tuple(AWARD_CONTRACT_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating contract specific awards in ' + str(end - start) + ' seconds')

        logger.info('Updating award category variables...')
        start = timeit.default_timer()
        update_award_categories(tuple(AWARD_UPDATE_ID_LIST))
        end = timeit.default_timer()
        logger.info('Finished updating award category variables in ' + str(end - start) + ' seconds')

        # Done!
        logger.info('FINISHED')