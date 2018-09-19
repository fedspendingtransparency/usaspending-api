import logging
import os
import boto
import smart_open
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.models import Count
from django.conf import settings

from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.common.helpers.generic_helper import fy, timer, upper_case_dict_values
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.broker import lookups
from usaspending_api.broker.helpers import get_business_categories
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, create_location
from usaspending_api.references.models import LegalEntity, Agency
from usaspending_api.etl.award_helpers import update_awards, update_award_categories


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
                   'AND (is_active IS True OR UPPER(correction_delete_indicatr) = \'D\')'
        db_args = [date]

        db_cursor.execute(db_query, db_args)
        db_rows = dictfetchall(db_cursor)  # this returns an OrderedDict

        ids_to_delete = []
        final_db_rows = []

        # Iterate through the result dict and determine what needs to be deleted and what needs to be added
        for row in db_rows:
            if row['correction_delete_indicatr'] and row['correction_delete_indicatr'].upper() == 'D':
                ids_to_delete.append(row['afa_generated_unique'].upper())
            else:
                final_db_rows.append(row)

        logger.info('Number of records to insert/update: %s' % str(len(final_db_rows)))
        logger.info('Number of records to delete: %s' % str(len(ids_to_delete)))

        return final_db_rows, ids_to_delete

    def find_related_awards(self, transactions):
        related_award_ids = [result[0] for result in transactions.values_list('award_id')]
        tn_count = TransactionNormalized.objects.filter(award_id__in=related_award_ids).values('award_id') \
            .annotate(transaction_count=Count('id')).values_list('award_id', 'transaction_count')
        tn_count_filtered = transactions.values('award_id').annotate(transaction_count=Count('id'))\
            .values_list('award_id', 'transaction_count')
        tn_count_mapping = {award_id: transaction_count for award_id, transaction_count in tn_count}
        tn_count_filtered_mapping = {award_id: transaction_count for award_id, transaction_count in tn_count_filtered}
        # only delete awards if and only if all their transactions are deleted, otherwise update the award
        update_awards = [award_id for award_id, transaction_count in tn_count_mapping.items()
                         if tn_count_filtered_mapping[award_id] != transaction_count]
        delete_awards = [award_id for award_id, transaction_count in tn_count_mapping.items()
                         if tn_count_filtered_mapping[award_id] == transaction_count]
        return update_awards, delete_awards

    @transaction.atomic
    def delete_stale_fabs(self, ids_to_delete=None):
        logger.info('Starting deletion of stale FABS data')

        if not ids_to_delete:
            return

        transactions = TransactionNormalized.objects.filter(assistance_data__afa_generated_unique__in=ids_to_delete)
        update_award_ids, delete_award_ids = self.find_related_awards(transactions)

        delete_transaction_ids = [delete_result[0] for delete_result in transactions.values_list('id')]
        delete_transaction_str_ids = ','.join([str(deleted_result) for deleted_result in delete_transaction_ids])
        update_award_str_ids = ','.join([str(update_result) for update_result in update_award_ids])
        delete_award_str_ids = ','.join([str(deleted_result) for deleted_result in delete_award_ids])

        db_cursor = connections['default'].cursor()

        queries = []
        # Transaction FABS
        if delete_transaction_ids:
            fabs = 'DELETE ' \
                   'FROM "transaction_fabs" tf ' \
                   'WHERE tf."transaction_id" IN ({});'.format(delete_transaction_str_ids)
            # Transaction Normalized
            tn = 'DELETE ' \
                 'FROM "transaction_normalized" tn ' \
                 'WHERE tn."id" IN ({});'.format(delete_transaction_str_ids)
            queries.extend([fabs, tn])
        # Update Awards
        if update_award_ids:
            # Adding to award_update_id_list so the latest_transaction will be recalculated
            award_update_id_list.extend(update_award_ids)
            update_awards_query = 'UPDATE "awards" ' \
                                  'SET "latest_transaction_id" = null ' \
                                  'WHERE "id" IN ({});'.format(update_award_str_ids)
            queries.append(update_awards_query)
        if delete_award_ids:
            # Financial Accounts by Awards
            fa = 'UPDATE "financial_accounts_by_awards" ' \
                 'SET "award_id" = null '\
                 'WHERE "award_id" IN ({});'.format(delete_award_str_ids)
            # Subawards
            sub = 'UPDATE "subaward" ' \
                  'SET "award_id" = null ' \
                  'WHERE "award_id" IN ({});'.format(delete_award_str_ids)
            # Delete Awards
            delete_awards_query = 'DELETE ' \
                                  'FROM "awards" a ' \
                                  'WHERE a."id" IN ({});'.format(delete_award_str_ids)
            queries.extend([fa, sub, delete_awards_query])
        if queries:
            db_query = ''.join(queries)
            db_cursor.execute(db_query, [])

    @transaction.atomic
    def insert_new_fabs(self, to_insert, total_rows):
        logger.info('Starting insertion of new FABS data')

        place_of_performance_field_map = {
            "location_country_code": "place_of_perform_country_c",
            "country_name": "place_of_perform_country_n",
            "state_code": "place_of_perfor_state_code",
            "state_name": "place_of_perform_state_nam",
            "city_name": "place_of_performance_city",
            "county_name": "place_of_perform_county_na",
            "county_code": "place_of_perform_county_co",
            "foreign_location_description": "place_of_performance_forei",
            "zip_4a": "place_of_performance_zip4a",
            "congressional_code": "place_of_performance_congr",
            "performance_code": "place_of_performance_code",
            "zip_last4": "place_of_perform_zip_last4",
            "zip5": "place_of_performance_zip5"
        }

        legal_entity_location_field_map = {
            "location_country_code": "legal_entity_country_code",
            "country_name": "legal_entity_country_name",
            "state_code": "legal_entity_state_code",
            "state_name": "legal_entity_state_name",
            "city_name": "legal_entity_city_name",
            "city_code": "legal_entity_city_code",
            "county_name": "legal_entity_county_name",
            "county_code": "legal_entity_county_code",
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "foreign_location_description": "legal_entity_foreign_descr",
            "congressional_code": "legal_entity_congressional",
            "zip_last4": "legal_entity_zip_last4",
            "zip5": "legal_entity_zip5",
            "foreign_postal_code": "legal_entity_foreign_posta",
            "foreign_province": "legal_entity_foreign_provi",
            "foreign_city_name": "legal_entity_foreign_city"
        }

        start_time = datetime.now()

        for index, row in enumerate(to_insert, 1):
            if not (index % 1000):
                logger.info('Inserting Stale FABS: Inserting row {} of {} ({})'.format(str(index), str(total_rows),
                                                                                       datetime.now() - start_time))

            upper_case_dict_values(row)

            # Create new LegalEntityLocation and LegalEntity from the row data
            legal_entity_location = create_location(legal_entity_location_field_map, row, {"recipient_flag": True})
            recipient_name = row['awardee_or_recipient_legal']
            legal_entity = LegalEntity.objects.create(
                recipient_unique_id=row['awardee_or_recipient_uniqu'],
                recipient_name=recipient_name if recipient_name is not None else "",
                parent_recipient_unique_id=row['ultimate_parent_unique_ide']
            )
            legal_entity_value_map = {
                "location": legal_entity_location,
                "business_categories": get_business_categories(row=row, data_type='fabs'),
                "business_types_description": row['business_types_desc']
            }
            legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

            # Create the place of performance location
            pop_location = create_location(place_of_performance_field_map, row, {"place_of_performance_flag": True})

            # Find the toptier awards from the subtier awards
            awarding_agency = Agency.get_by_subtier_only(row["awarding_sub_tier_agency_c"])
            funding_agency = Agency.get_by_subtier_only(row["funding_sub_tier_agency_co"])

            # Generate the unique Award ID
            # "ASST_AW_" + awarding_sub_tier_agency_c + fain + uri

            # this will raise an exception if the cast to an int fails, that's ok since we don't want to process
            # non-numeric record type values
            record_type_int = int(row['record_type'])
            if record_type_int == 1:
                uri = row['uri'] if row['uri'] else '-NONE-'
                fain = '-NONE-'
            elif record_type_int in (2, 3):
                uri = '-NONE-'
                fain = row['fain'] if row['fain'] else '-NONE-'
            else:
                raise Exception('Invalid record type encountered for the following afa_generated_unique record: %s' %
                                row['afa_generated_unique'])

            generated_unique_id = 'ASST_AW_' +\
                (row['awarding_sub_tier_agency_c'] if row['awarding_sub_tier_agency_c'] else '-NONE-') + '_' + \
                fain + '_' + uri

            # Create the summary Award
            (created, award) = Award.get_or_create_summary_award(generated_unique_award_id=generated_unique_id,
                                                                 fain=row['fain'],
                                                                 uri=row['uri'],
                                                                 record_type=row['record_type'])
            award.save()

            # Append row to list of Awards updated
            award_update_id_list.append(award.id)

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
                "last_modified_date": last_mod_date,
                "type_description": row['assistance_type_desc'],
                "transaction_unique_id": row['afa_generated_unique'],
                "generated_unique_award_id": generated_unique_id
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
                transaction_normalized_dict["update_date"] = datetime.utcnow()
                transaction_normalized_dict["fiscal_year"] = fy(transaction_normalized_dict["action_date"])

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

            # Update legal entity to map back to transaction
            legal_entity.transaction_unique_id = afa_generated_unique
            legal_entity.save()

    @staticmethod
    def send_deletes_to_s3(ids_to_delete):
        logger.info('Uploading FABS delete data to FPDS bucket')

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
            aws_region = os.environ.get('USASPENDING_AWS_REGION')
            fpds_bucket_name = os.environ.get('FPDS_BUCKET_NAME')
            s3_bucket = boto.s3.connect_to_region(aws_region).get_bucket(fpds_bucket_name)
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

    def handle(self, *args, **options):
        logger.info('Starting FABS nightly data load...')

        # Use date provided or pull most recent ExternalDataLoadDate
        if options.get('date'):
            date = options.get('date')[0]
        else:
            data_load_date_obj = ExternalDataLoadDate.objects. \
                filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).first()
            if not data_load_date_obj:
                date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                date = data_load_date_obj.last_load_date
        start_date = datetime.utcnow().strftime('%Y-%m-%d')

        logger.info('Processing data for FABS starting from %s' % date)

        # Retrieve FABS data
        with timer('retrieving/diff-ing FABS Data', logger.info):
            to_insert, ids_to_delete = self.get_fabs_data(date=date)

        total_rows = len(to_insert)
        total_rows_delete = len(ids_to_delete)

        if total_rows_delete > 0:
            # Create a file with the deletion IDs and place in a bucket for ElasticSearch
            self.send_deletes_to_s3(ids_to_delete)

            # Delete FABS records by ID
            with timer('deleting stale FABS data', logger.info):
                self.delete_stale_fabs(ids_to_delete=ids_to_delete)
        else:
            logger.info('Nothing to delete...')

        if total_rows > 0:
            # Add FABS records
            with timer('inserting new FABS data', logger.info):
                self.insert_new_fabs(to_insert=to_insert, total_rows=total_rows)

            # Update Awards based on changed FABS records
            with timer('updating awards to reflect their latest associated transaction info', logger.info):
                update_awards(tuple(award_update_id_list))

            # Update AwardCategories based on changed FABS records
            with timer('updating award category variables', logger.info):
                update_award_categories(tuple(award_update_id_list))

            # Check the linkages from file C to FABS records and update any that are missing
            with timer('updating C->D linkages', logger.info):
                update_c_to_d_linkages('assistance')
        else:
            logger.info('Nothing to insert...')

        # Update the date for the last time the data load was run
        ExternalDataLoadDate.objects.filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).delete()
        ExternalDataLoadDate(last_load_date=start_date,
                             external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).save()

        logger.info('FABS NIGHTLY UPDATE FINISHED!')
