import logging
import timeit
import os
import boto
import smart_open
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.conf import settings

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.broker import lookups
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, create_location
from usaspending_api.references.models import LegalEntity, Agency, ToptierAgency, SubtierAgency
from usaspending_api.etl.award_helpers import update_awards, update_award_categories

# start = timeit.default_timer()
# function_call
# end = timeit.default_timer()
# time elapsed = str(end - start)


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

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
                   'WHERE created_at >= %s ' \
                   'AND (is_active = True OR UPPER(correction_late_delete_ind) = \'D\') ' \
                   'ORDER BY published_award_financial_assistance_id ASC'
        db_args = [date]

        db_cursor.execute(db_query, db_args)
        db_rows = dictfetchall(db_cursor)  # this returns an OrderedDict

        ids_to_delete = []

        # Iterate through the result dict and determine what needs to be deleted and what needs to be added
        for row in db_rows:
            if row['correction_late_delete_ind'] and row['correction_late_delete_ind'].upper() == 'D':
                ids_to_delete += [row['afa_generated_unique']]
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
        TransactionNormalized.objects.filter(assistance_data__afa_generated_unique__in=ids_to_delete).delete()

    @staticmethod
    def insert_new_fabs(to_insert, total_rows):
        logger.info('Starting insertion of new FABS data')

        place_of_performance_field_map = {
            "city_name": "place_of_performance_city",
            "performance_code": "place_of_performance_code",
            "congressional_code": "place_of_performance_congr",
            "county_name": "place_of_perform_county_na",
            "county_code": "place_of_perform_county_co",
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

            legal_entity_location = create_location(legal_entity_location_field_map, row, {"recipient_flag": True})

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
            pop_location = create_location(place_of_performance_field_map, row, {"place_of_performance_flag": True})

            # Find the award that this award transaction belongs to
            awarding_agency = Agency.get_by_subtier_only(row["awarding_sub_tier_agency_c"])

            (created, award) = Award.get_or_create_summary_award(
                awarding_agency=awarding_agency,
                fain=row.get('fain'),
                uri=row.get('uri'),
                parent_award_id=row.get('parent_award_id'),
                record_type=row.get('record_type'))
            award.parent_award_piid = row.get('parent_award_id')
            award.save()

            award_update_id_list.append(award.id)

            funding_agency = Agency.get_by_subtier_only(row["funding_sub_tier_agency_co"])

            try:
                last_mod_date = datetime.strptime(str(row['modified_at']), "%Y-%m-%d %H:%M:%S.%f").date()
            except ValueError:
                last_mod_date = datetime.strptime(str(row['modified_at']), "%Y-%m-%d %H:%M:%S").date()

            parent_txn_value_map = {
                "award": award,
                "awarding_agency": awarding_agency,
                "funding_agency": funding_agency,
                "recipient": legal_entity,
                "place_of_performance": pop_location,
                "period_of_performance_start_date": format_date(row['period_of_performance_star']),
                "period_of_performance_current_end_date": format_date(row['period_of_performance_curr']),
                "action_date": format_date(row['action_date']),
                "last_modified_date": last_mod_date
            }

            fad_field_map = {
                "type": "assistance_type",
                "description": "award_description",
            }

            transaction_normalized_dict = load_data_into_model(
                TransactionNormalized(),  # thrown away
                row,
                field_map=fad_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            financial_assistance_data = load_data_into_model(
                TransactionFABS(),  # thrown away
                row,
                as_dict=True)

            afa_generated_unique = financial_assistance_data['afa_generated_unique']
            unique_fabs = TransactionFABS.objects.filter(afa_generated_unique=afa_generated_unique)

            if unique_fabs.first():
                # Update TransactionNormalized
                TransactionNormalized.objects.filter(id=unique_fabs.first().transaction.id).\
                    update(**transaction_normalized_dict)

                # Update TransactionFABS
                unique_fabs.update(**financial_assistance_data)
            else:
                # Create TransactionNormalized
                transaction = TransactionNormalized(**transaction_normalized_dict)
                transaction.save()

                # Create TransactionFABS
                transaction_fabs = TransactionFABS(transaction=transaction, **financial_assistance_data)
                transaction_fabs.save()

    @staticmethod
    def send_deletes_to_elasticsearch(ids_to_delete):
        logger.info('Uploading FABS delete data to Elasticsearch bucket')

        # Make timestamp
        seconds = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())
        file_name = datetime.utcnow().strftime('%Y-%m-%d') + "_FABSdeletions_" + str(seconds) + ".csv"
        file_with_headers = ['afa_generated_unique'] + ids_to_delete

        if settings.IS_LOCAL:
            # Write to local file
            file_path = settings.CSV_LOCAL_PATH + file_name
            with open(file_path, 'w') as writer:
                for row in file_with_headers:
                    writer.write(row + '\n')
        else:
            # Write to file in S3 bucket directly
            aws_region = os.environ.get('AWS_REGION')
            elasticsearch_bucket_name = os.environ.get('FPDS_BUCKET_NAME')
            s3_bucket = boto.s3.connect_to_region(aws_region).get_bucket(elasticsearch_bucket_name)
            conn = s3_bucket.new_key(file_name)
            with smart_open.smart_open(conn, 'w') as writer:
                for row in file_with_headers:
                    writer.write(row + '\n')

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
        logger.info('Starting FABS nightly data load...')

        # Use date provided or pull most recent ExternalDataLoadDate
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

        # Retrieve FABS data
        logger.info('Retrieving FABS Data...')
        start = timeit.default_timer()
        to_insert, ids_to_delete = self.get_fabs_data(date=date)
        end = timeit.default_timer()
        logger.info('Finished diff-ing FABS data in ' + str(end - start) + ' seconds')

        total_rows = len(to_insert)
        total_rows_delete = len(ids_to_delete)

        if total_rows_delete > 0:
            # Create a file with the deletion IDs and place in a bucket for ElasticSearch
            self.send_deletes_to_elasticsearch(ids_to_delete)

            # Delete FABS records by ID
            logger.info('Deleting stale FABS data...')
            start = timeit.default_timer()
            self.delete_stale_fabs(ids_to_delete=ids_to_delete)
            end = timeit.default_timer()
            logger.info('Finished deleting stale FABS data in ' + str(end - start) + ' seconds')
        else:
            logger.info('Nothing to delete...')

        if total_rows > 0:
            # Add FABS records
            logger.info('Inserting new FABS data...')
            start = timeit.default_timer()
            self.insert_new_fabs(to_insert=to_insert, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished inserting new FABS data in ' + str(end - start) + ' seconds')

            # Update Awards based on changed FABS records
            logger.info('Updating awards to reflect their latest associated transaction info...')
            start = timeit.default_timer()
            update_awards(tuple(award_update_id_list))
            end = timeit.default_timer()
            logger.info('Finished updating awards in ' + str(end - start) + ' seconds')

            # Update AwardCategories based on changed FABS records
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
