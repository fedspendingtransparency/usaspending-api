import logging
import timeit
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.broker import lookups
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, get_or_create_location
from usaspending_api.references.models import LegalEntity, Agency, ToptierAgency, SubtierAgency
from usaspending_api.etl.award_helpers import update_awards, update_award_categories

# start = timeit.default_timer()
# function_call
# end = timeit.default_timer()
# time elapsed = str(end - start)


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

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

award_update_id_list = []


class Command(BaseCommand):
    help = "Update FABS data nightly"

    @staticmethod
    def get_fabs_data(date):
        db_cursor = connections['data_broker'].cursor()

        # The ORDER BY is important here because deletions must happen in a specific order and that order is defined
        # by the Broker's PK since every modification is a new row
        db_query = 'SELECT * ' \
                   'FROM published_award_financial_assistance ' \
                   'WHERE created_at >= %s' \
                   'ORDER BY published_award_financial_assistance_id ASC'
        db_args = [date]

        db_cursor.execute(db_query, db_args)
        db_rows = dictfetchall(db_cursor)  # this returns an OrderedDict

        ids_to_delete = []

        # Iterate through the result dict and determine what needs to be deleted and what needs to be added
        for row in db_rows:
            if row['correction_late_delete_ind'] and row['correction_late_delete_ind'].upper() == 'D':
                ids_to_delete += [row['published_award_financial_assistance_id']]
                # remove the row from the list of rows from the Broker since once we delete it, we don't care about it.
                # all that'll be left in db_rows are rows we want to insert
                db_rows.remove(row)

        logger.info('Number of records to insert/update: %s' % str(len(db_rows)))
        logger.info('Number of records to delete: %s' % str(len(ids_to_delete)))

        return db_rows, ids_to_delete

    @staticmethod
    def delete_stale_fabs(ids_to_delete=None):
        logger.info('Starting deletion of stale FABS data')

        if not ids_to_delete:
            return

        # This cascades deletes for TransactionFABS & Awards in addition to deleting TransactionNormalized records
        TransactionNormalized.objects.\
            filter(assistance_data__published_award_financial_assistance_id__in=ids_to_delete).delete()

    @staticmethod
    def insert_new_fabs(to_insert, total_rows):
        logger.info('Starting insertion of new FABS data')

        place_of_performance_field_map = {
            "city_name": "place_of_performance_city",
            "performance_code": "place_of_performance_code",
            "congressional_code": "place_of_performance_congr",
            "county_name": "place_of_perform_county_na",
            "county_code": "place_of_perform_county_c",
            "foreign_location_description": "place_of_performance_forei",
            "state_name": "place_of_perform_state_nam",
            "zip4": "place_of_performance_zip4a",
            "location_country_code": "place_of_perform_country_c",
            "country_name": "place_of_perform_country_n"
        }

        legal_entity_location_field_map = {
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "city_name": "legal_entity_city_name",
            "city_code": "legal_entity_city_code",
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
            "location_country_code": "legal_entity_country_code",
            "country_name": "legal_entity_country_name"
        }

        start_time = datetime.now()

        for index, row in enumerate(to_insert, 1):
            if not (index % 1000):
                logger.info('Inserting Stale FABS: Inserting row {} of {} ({})'.format(str(index),
                                                                                       str(total_rows),
                                                                                       datetime.now() - start_time))

            legal_entity_location, created = get_or_create_location(
                legal_entity_location_field_map, row, {"recipient_flag": True}
            )

            recipient_name = row['awardee_or_recipient_legal']
            if recipient_name is None:
                recipient_name = ""

            # Handling the case of duplicates, just grab the most recently updated match
            legal_entity = LegalEntity.objects.filter(
                recipient_unique_id=row['awardee_or_recipient_uniqu'],
                recipient_name=recipient_name
            ).order_by('-update_date').first()
            created = False

            if not legal_entity:
                legal_entity = LegalEntity.objects.create(
                    recipient_unique_id=row['awardee_or_recipient_uniqu'],
                    recipient_name=recipient_name
                )
                created = True

            if created:
                legal_entity_value_map = {
                    "location": legal_entity_location,
                }
                legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

            # Create the place of performance location
            pop_location, created = get_or_create_location(
                place_of_performance_field_map, row, {"place_of_performance_flag": True}
            )

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
                    funding_toptier_agency_id = subtier_to_agency_map[funding_subtier_agency_id]['toptier_agency_id']
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
                "last_modified_date": row['modified_at']
            }

            fad_field_map = {
                "type": "assistance_type",
                "description": "award_description",
            }

            transaction_dict = load_data_into_model(
                TransactionNormalized(),  # thrown away
                row,
                field_map=fad_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            transaction = TransactionNormalized.get_or_create_transaction(**transaction_dict)
            transaction.save()

            financial_assistance_data = load_data_into_model(
                TransactionFABS(),  # thrown away
                row,
                as_dict=True)

            afa_generated_unique = financial_assistance_data['afa_generated_unique']

            if TransactionFABS.objects.filter(afa_generated_unique=afa_generated_unique).exists():
                TransactionFABS.objects.filter(afa_generated_unique=afa_generated_unique).\
                    update(**financial_assistance_data)
            else:
                transaction_assistance = TransactionFABS(transaction=transaction, **financial_assistance_data)
                transaction_assistance.save()

    def add_arguments(self, parser):

        parser.add_argument(
            '--date',
            dest="date",
            nargs='+',
            type=str,
            help="(OPTIONAL) Date from which to start the nightly loader. Expected format: MM/DD/YYYY"
        )

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting historical data load...')

        if options.get('date'):
            date = options.get('date')[0]
        else:
            data_load_date_obj = ExternalDataLoadDate.objects. \
                filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).first()
            if not data_load_date_obj:
                date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                date = data_load_date_obj.last_load_date

        logger.info('Processing data for FABS starting from %s' % date)

        logger.info('Retrieving FABS Data...')
        start = timeit.default_timer()
        to_insert, ids_to_delete = self.get_fabs_data(date=date)
        end = timeit.default_timer()
        logger.info('Finished diff-ing FABS data in ' + str(end - start) + ' seconds')

        total_rows = len(to_insert)
        total_rows_delete = len(ids_to_delete)

        if total_rows_delete > 0:
            logger.info('Deleting stale FABS data...')
            start = timeit.default_timer()
            self.delete_stale_fabs(ids_to_delete=ids_to_delete)
            end = timeit.default_timer()
            logger.info('Finished deleting stale FABS data in ' + str(end - start) + ' seconds')
        else:
            logger.info('Nothing to delete...')

        if total_rows > 0:
            logger.info('Inserting new FABS data...')
            start = timeit.default_timer()
            self.insert_new_fabs(to_insert=to_insert, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished inserting new FABS data in ' + str(end - start) + ' seconds')

            logger.info('Updating awards to reflect their latest associated transaction info...')
            start = timeit.default_timer()
            update_awards(tuple(award_update_id_list))
            end = timeit.default_timer()
            logger.info('Finished updating awards in ' + str(end - start) + ' seconds')

            logger.info('Updating award category variables...')
            start = timeit.default_timer()
            update_award_categories(tuple(award_update_id_list))
            end = timeit.default_timer()
            logger.info('Finished updating award category variables in ' + str(end - start) + ' seconds')
        else:
            logger.info('Nothing to insert...')

        # Update the date for the last time the data load was run
        ExternalDataLoadDate.objects.filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).delete()
        ExternalDataLoadDate(last_load_date=datetime.now().strftime('%Y-%m-%d'),
                             external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).save()

        logger.info('FABS NIGHTLY UPDATE FINISHED!')
