import logging

from django.core.management.base import BaseCommand
from django.db import connections

from usaspending_api.etl.broker_etl_helpers import PhonyCursor
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.etl.broker_etl_helpers import dictfetchall

from usaspending_api.broker.models import TransactionNew, TransactionAssistanceNew, TransactionContractNew, TransactionMap
from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, LegalEntity
from usaspending_api.etl.management.load_base import copy, get_or_create_location, format_date, load_data_into_model


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

# Lists to store for update_awards and update_contract_awards
AWARD_UPDATE_ID_LIST = []
AWARD_CONTRACT_UPDATE_ID_LIST = []


class Command(BaseCommand):
    help = "Checks what TASs are in Broker but not in Data Store"

    # TODO: ensure references to old d2 broker model is updated to match D2 model
    def update_transaction_assistance(self):

        legal_entity_location_field_map = {
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "city_code": "legal_entity_city_code",
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

        all_transaction_assistance = TransactionAssistanceNew.objects.using('data_broker').all()
        print(len(all_transaction_assistance))

        for transaction_assistance in all_transaction_assistance[0]: # TODO: REMOVE INDEX SELECTION TO RUN ON ALL
            legal_entity_location, created = get_or_create_location(
                legal_entity_location_field_map, transaction_assistance, legal_entity_location_value_map
            )

            # Create the legal entity if it doesn't exist
            legal_entity, created = LegalEntity.objects.get_or_create(
                recipient_unique_id=transaction_assistance['awardee_or_recipient_uniqu'],
                recipient_name=transaction_assistance['awardee_or_recipient_legal']
            )

            if created:
                legal_entity_value_map = {
                    "location": legal_entity_location,
                }
                legal_entity = load_data_into_model(legal_entity, transaction_assistance, value_map=legal_entity_value_map, save=True)

            # Create the place of performance location
            pop_location, created = get_or_create_location(
                place_of_performance_field_map, transaction_assistance, place_of_performance_value_map
            )

            # If awarding toptier agency code (aka CGAC) is not supplied on the D2 record,
            # use the sub tier code to look it up. This code assumes that all incoming
            # records will supply an awarding subtier agency code
            if transaction_assistance['awarding_agency_code'] is None or len(transaction_assistance['awarding_agency_code'].strip()) < 1:
                transaction_assistance['awarding_agency_code'] = Agency.get_by_subtier(
                    transaction_assistance["awarding_sub_tier_agency_c"]).toptier_agency.cgac_code
            # If funding toptier agency code (aka CGAC) is empty, try using the sub
            # tier funding code to look it up. Unlike the awarding agency, we can't
            # assume that the funding agency subtier code will always be present.
            if transaction_assistance['funding_agency_code'] is None or len(transaction_assistance['funding_agency_code'].strip()) < 1:
                funding_agency = Agency.get_by_subtier(transaction_assistance["funding_sub_tier_agency_co"])
                transaction_assistance['funding_agency_code'] = (
                    funding_agency.toptier_agency.cgac_code if funding_agency is not None
                    else None)

            # Find the award that this award transaction belongs to. If it doesn't exist, create it.
            awarding_agency = Agency.get_by_toptier_subtier(
                transaction_assistance['awarding_agency_code'],
                transaction_assistance["awarding_sub_tier_agency_c"]
            )
            created, award = Award.get_or_create_summary_award(
                awarding_agency=awarding_agency,
                # piid=transaction_assistance.get('piid'), # not found
                fain=transaction_assistance.get('fain'),
                uri=transaction_assistance.get('uri'))
                # parent_award_id=transaction_assistance.get('parent_award_id')) # not found
            award.save()

            AWARD_UPDATE_ID_LIST.append(award.id)

            parent_txn_value_map = {
                "award": award,
                "awarding_agency": awarding_agency,
                "funding_agency": Agency.get_by_toptier_subtier(transaction_assistance['funding_agency_code'],
                                                                transaction_assistance["funding_sub_tier_agency_co"]),
                "recipient": legal_entity,
                "place_of_performance": pop_location,
                "period_of_performance_start_date": format_date(transaction_assistance['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(transaction_assistance['period_of_performance_curr']),
                "action_date": format_date(transaction_assistance['action_date']),
            }

            transaction_dict = load_data_into_model(
                TransactionNew(),  # thrown away
                transaction_assistance,
                field_map=fad_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            transaction = TransactionNew.get_or_create_transaction(**transaction_dict)
            transaction.save()

            transaction_map = TransactionMap()
            transaction_map.transaction_id = transaction.transaction_id
            transaction_map.transaction_assistance_id = transaction_assistance['published_award_financial_assistance_id']
            transaction_map.save()


    def update_transaction_contract(self):

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
            "city_name": "place_of_performance_locat",
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

        all_transaction_contract = TransactionContractNew.using('data_broker').objects.all()

        for transaction_contract in all_transaction_contract:
            legal_entity_location, created = get_or_create_location(
                legal_entity_location_field_map, transaction_contract, copy(legal_entity_location_value_map)
            )

            # Create the legal entity if it doesn't exist
            legal_entity, created = LegalEntity.objects.get_or_create(
                recipient_unique_id=transaction_contract['awardee_or_recipient_uniqu'],
                recipient_name=transaction_contract['awardee_or_recipient_legal']
            )

            if created:
                legal_entity_value_map = {
                    "location": legal_entity_location,
                }
                legal_entity = load_data_into_model(legal_entity, transaction_contract, value_map=legal_entity_value_map, save=True)

            # Create the place of performance location
            pop_location, created = get_or_create_location(
                place_of_performance_field_map, transaction_contract, copy(place_of_performance_value_map))

            # If awarding toptier agency code (aka CGAC) is not supplied on the D1 record,
            # use the sub tier code to look it up. This code assumes that all incoming
            # records will supply an awarding subtier agency code
            if transaction_contract['awarding_agency_code'] is None or len(transaction_contract['awarding_agency_code'].strip()) < 1:
                transaction_contract['awarding_agency_code'] = Agency.get_by_subtier(
                    transaction_contract["awarding_sub_tier_agency_c"]).toptier_agency.cgac_code
            # If funding toptier agency code (aka CGAC) is empty, try using the sub
            # tier funding code to look it up. Unlike the awarding agency, we can't
            # assume that the funding agency subtier code will always be present.
            if transaction_contract['funding_agency_code'] is None or len(transaction_contract['funding_agency_code'].strip()) < 1:
                funding_agency = Agency.get_by_subtier(transaction_contract["funding_sub_tier_agency_co"])
                transaction_contract['funding_agency_code'] = (
                    funding_agency.toptier_agency.cgac_code if funding_agency is not None
                    else None)

            # Find the award that this award transaction belongs to. If it doesn't exist, create it.
            awarding_agency = Agency.get_by_toptier_subtier(
                transaction_contract['awarding_agency_code'],
                transaction_contract["awarding_sub_tier_agency_c"]
            )
            created, award = Award.get_or_create_summary_award(
                awarding_agency=awarding_agency,
                piid=transaction_contract.get('piid'),
                fain=transaction_contract.get('fain'),
                uri=transaction_contract.get('uri'),
                parent_award_id=transaction_contract.get('parent_award_id'))
            award.save()

            AWARD_UPDATE_ID_LIST.append(award.id)
            AWARD_CONTRACT_UPDATE_ID_LIST.append(award.id)

            parent_txn_value_map = {
                "award": award,
                "awarding_agency": awarding_agency,
                "funding_agency": Agency.get_by_toptier_subtier(transaction_contract['funding_agency_code'],
                                                                transaction_contract["funding_sub_tier_agency_co"]),
                "recipient": legal_entity,
                "place_of_performance": pop_location,
                "period_of_performance_start_date": format_date(transaction_contract['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(transaction_contract['period_of_performance_curr']),
                "action_date": format_date(transaction_contract['action_date']),
            }

            transaction_dict = load_data_into_model(
                TransactionNew(),  # thrown away
                transaction_contract,
                field_map=contract_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            transaction = TransactionNew.get_or_create_transaction(**transaction_dict)
            transaction.save()


    def handle(self, *args, **options):
        print('Starting updates to Transactions')
        # Grab the data broker database connections
        # if not options.get('test'):
        #     try:
        #         db_conn = connections['data_broker']
        #         db_cursor = db_conn.cursor()
        #     except Exception as err:
        #         logger.critical('Could not connect to database. Is DATA_BROKER_DATABASE_URL set?')
        #         logger.critical(print(err))
        #         return
        # else:
        #     db_cursor = PhonyCursor()

        self.update_transaction_assistance()

        print('Finished updating transaction assistance')

        # self.update_transaction_contract()
