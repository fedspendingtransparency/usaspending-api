import boto3
import csv
import logging
import os
import re
import urllib.request
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.models import Count
from django.conf import settings

from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.common.helpers.generic_helper import fy, timer, upper_case_dict_values
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFPDS, TransactionNormalized, Award
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.broker import lookups
from usaspending_api.broker.helpers import get_business_categories, set_legal_entity_boolean_fields
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, create_location
from usaspending_api.references.models import LegalEntity, Agency
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards, update_award_categories


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

award_update_id_list = []


class Command(BaseCommand):
    help = "Update FPDS data nightly"

    @staticmethod
    def get_fpds_data(date):
        if not hasattr(date, 'month'):
            date = datetime.strptime(date, '%Y-%m-%d').date()

        db_cursor = connections['data_broker'].cursor()

        # The ORDER BY is important here because deletions must happen in a specific order and that order is defined
        # by the Broker's PK since every modification is a new row
        db_query = 'SELECT * ' \
                   'FROM detached_award_procurement ' \
                   'WHERE updated_at >= %s'
        db_args = [date]

        db_cursor.execute(db_query, db_args)
        db_rows = dictfetchall(db_cursor)  # this returns an OrderedDict

        ids_to_delete = []

        if settings.IS_LOCAL:
            for file in os.listdir(settings.CSV_LOCAL_PATH):
                if re.search('.*_delete_records_(IDV|award).*', file) and \
                            datetime.strptime(file[:file.find('_')], '%m-%d-%Y').date() >= date:
                    with open(settings.CSV_LOCAL_PATH + file, 'r') as current_file:
                        # open file, split string to array, skip the header
                        reader = csv.reader(current_file.read().splitlines())
                        next(reader)
                        unique_key_list = [rows[0] for rows in reader]

                        ids_to_delete += unique_key_list
        else:
            # Connect to AWS
            aws_region = os.environ.get('USASPENDING_AWS_REGION')
            fpds_bucket_name = os.environ.get('FPDS_BUCKET_NAME')

            if not (aws_region or fpds_bucket_name):
                raise Exception('Missing required environment variables: USASPENDING_AWS_REGION, FPDS_BUCKET_NAME')

            s3client = boto3.client('s3', region_name=aws_region)
            s3resource = boto3.resource('s3', region_name=aws_region)
            s3_bucket = s3resource.Bucket(fpds_bucket_name)

            # make an array of all the keys in the bucket
            file_list = [item.key for item in s3_bucket.objects.all()]

            # Only use files that match the date we're currently checking
            for item in file_list:
                # if the date on the file is the same day as we're checking
                if re.search('.*_delete_records_(IDV|award).*', item) and '/' not in item and \
                                datetime.strptime(item[:item.find('_')], '%m-%d-%Y').date() >= date:
                    # make the url params to pass
                    url_params = {
                        'Bucket': fpds_bucket_name,
                        'Key': item
                    }
                    # get the url for the current file
                    file_path = s3client.generate_presigned_url('get_object', Params=url_params)
                    current_file = urllib.request.urlopen(file_path)
                    reader = csv.reader(current_file.read().decode("utf-8").splitlines())
                    # skip the header, the reader doesn't ignore it for some reason
                    next(reader)
                    # make an array of all the detached_award_procurement_ids
                    unique_key_list = [rows[0] for rows in reader]

                    ids_to_delete += unique_key_list

        logger.info('Number of records to insert/update: %s' % str(len(db_rows)))
        logger.info('Number of records to delete: %s' % str(len(ids_to_delete)))

        return db_rows, ids_to_delete

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
    def delete_stale_fpds(self, ids_to_delete=None):
        logger.info('Starting deletion of stale FPDS data')

        if not ids_to_delete:
            return

        transactions = TransactionNormalized.objects.filter(
            contract_data__detached_award_procurement_id__in=ids_to_delete)
        update_award_ids, delete_award_ids = self.find_related_awards(transactions)

        delete_transaction_ids = [delete_result[0] for delete_result in transactions.values_list('id')]
        delete_transaction_str_ids = ','.join([str(deleted_result) for deleted_result in delete_transaction_ids])
        update_award_str_ids = ','.join([str(update_result) for update_result in update_award_ids])
        delete_award_str_ids = ','.join([str(deleted_result) for deleted_result in delete_award_ids])

        db_cursor = connections['default'].cursor()

        queries = []
        # Transaction FPDS
        if delete_transaction_ids:
            fpds = 'DELETE ' \
                   'FROM "transaction_fpds" tf '\
                   'WHERE tf."transaction_id" IN ({});'.format(delete_transaction_str_ids)
            # Transaction Normalized
            tn = 'DELETE ' \
                 'FROM "transaction_normalized" tn '\
                 'WHERE tn."id" IN ({});'.format(delete_transaction_str_ids)
            queries.extend([fpds, tn])
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
            # Delete Subawards
            delete_awards_query = 'DELETE ' \
                                  'FROM "awards" a ' \
                                  'WHERE a."id" IN ({});'.format(delete_award_str_ids)
            queries.extend([fa, sub, delete_awards_query])
        if queries:
            db_query = ''.join(queries)
            db_cursor.execute(db_query, [])

    @transaction.atomic
    def insert_new_fpds(self, to_insert, total_rows):
        logger.info('Starting insertion of new FPDS data')

        place_of_performance_field_map = {
            "location_country_code": "place_of_perform_country_c",
            "country_name": "place_of_perf_country_desc",
            "state_code": "place_of_performance_state",
            "state_name": "place_of_perfor_state_desc",
            "city_name": "place_of_perform_city_name",
            "county_name": "place_of_perform_county_na",
            "county_code": "place_of_perform_county_co",
            "zip_4a": "place_of_performance_zip4a",
            "congressional_code": "place_of_performance_congr",
            "zip_last4": "place_of_perform_zip_last4",
            "zip5": "place_of_performance_zip5"
        }

        legal_entity_location_field_map = {
            "location_country_code": "legal_entity_country_code",
            "country_name": "legal_entity_country_name",
            "state_code": "legal_entity_state_code",
            "state_name": "legal_entity_state_descrip",
            "city_name": "legal_entity_city_name",
            "county_name": "legal_entity_county_name",
            "county_code": "legal_entity_county_code",
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "zip4": "legal_entity_zip4",
            "congressional_code": "legal_entity_congressional",
            "zip_last4": "legal_entity_zip_last4",
            "zip5": "legal_entity_zip5"
        }

        start_time = datetime.now()

        for index, row in enumerate(to_insert, 1):
            if not (index % 1000):
                logger.info('Inserting Stale FPDS: Inserting row {} of {} ({})'.format(str(index), str(total_rows),
                                                                                       datetime.now() - start_time))

            upper_case_dict_values(row)

            # Create new LegalEntityLocation and LegalEntity from the row data
            legal_entity_location = create_location(legal_entity_location_field_map, row, {"recipient_flag": True,
                                                                                           "is_fpds": True})
            recipient_name = row['awardee_or_recipient_legal']
            legal_entity = LegalEntity.objects.create(
                recipient_unique_id=row['awardee_or_recipient_uniqu'],
                recipient_name=recipient_name if recipient_name is not None else ""
            )
            legal_entity_value_map = {
                "location": legal_entity_location,
                "business_categories": get_business_categories(row=row, data_type='fpds'),
                "is_fpds": True
            }
            set_legal_entity_boolean_fields(row)
            legal_entity = load_data_into_model(legal_entity, row, value_map=legal_entity_value_map, save=True)

            # Create the place of performance location
            pop_location = create_location(place_of_performance_field_map, row, {"place_of_performance_flag": True})

            # Find the toptier awards from the subtier awards
            awarding_agency = Agency.get_by_subtier_only(row["awarding_sub_tier_agency_c"])
            funding_agency = Agency.get_by_subtier_only(row["funding_sub_tier_agency_co"])

            # Generate the unique Award ID
            # "CONT_AW_" + agency_id + referenced_idv_agency_iden + piid + parent_award_id
            generated_unique_id = 'CONT_AW_' + (row['agency_id'] if row['agency_id'] else '-NONE-') + '_' + \
                (row['referenced_idv_agency_iden'] if row['referenced_idv_agency_iden'] else '-NONE-') + '_' + \
                (row['piid'] if row['piid'] else '-NONE-') + '_' + \
                (row['parent_award_id'] if row['parent_award_id'] else '-NONE-')

            # Create the summary Award
            (created, award) = Award.get_or_create_summary_award(generated_unique_award_id=generated_unique_id,
                                                                 piid=row['piid'])
            award.parent_award_piid = row.get('parent_award_id')
            award.save()

            # Append row to list of Awards updated
            award_update_id_list.append(award.id)

            try:
                last_mod_date = datetime.strptime(str(row['last_modified']), "%Y-%m-%d %H:%M:%S.%f").date()
            except ValueError:
                last_mod_date = datetime.strptime(str(row['last_modified']), "%Y-%m-%d %H:%M:%S").date()
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
                "transaction_unique_id": row['detached_award_proc_unique'],
                "generated_unique_award_id": generated_unique_id,
                "is_fpds": True
            }

            contract_field_map = {
                "type": "contract_award_type",
                "type_description": "contract_award_type_desc",
                "description": "award_description"
            }

            transaction_normalized_dict = load_data_into_model(
                TransactionNormalized(),  # thrown away
                row,
                field_map=contract_field_map,
                value_map=parent_txn_value_map,
                as_dict=True)

            contract_instance = load_data_into_model(
                TransactionFPDS(),  # thrown away
                row,
                as_dict=True)

            detached_award_proc_unique = contract_instance['detached_award_proc_unique']
            unique_fpds = TransactionFPDS.objects.filter(detached_award_proc_unique=detached_award_proc_unique)

            if unique_fpds.first():
                transaction_normalized_dict["update_date"] = datetime.utcnow()
                transaction_normalized_dict["fiscal_year"] = fy(transaction_normalized_dict["action_date"])

                # update TransactionNormalized
                TransactionNormalized.objects.filter(id=unique_fpds.first().transaction.id).\
                    update(**transaction_normalized_dict)

                # update TransactionFPDS
                unique_fpds.update(**contract_instance)
            else:
                # create TransactionNormalized
                transaction = TransactionNormalized(**transaction_normalized_dict)
                transaction.save()

                # create TransactionFPDS
                transaction_fpds = TransactionFPDS(transaction=transaction, **contract_instance)
                transaction_fpds.save()

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            dest="date",
            nargs='+',
            type=str,
            help="(OPTIONAL) Date from which to start the nightly loader. Expected format: MM/DD/YYYY"
        )

    def handle(self, *args, **options):
        logger.info('Starting FPDS nightly data load...')

        if options.get('date'):
            date = options.get('date')[0]
            date = datetime.strptime(date, '%Y-%m-%d').date()
        else:
            data_load_date_obj = ExternalDataLoadDate.objects. \
                filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fpds']).first()
            if not data_load_date_obj:
                date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                date = data_load_date_obj.last_load_date
        start_date = datetime.utcnow().strftime('%Y-%m-%d')

        logger.info('Processing data for FPDS starting from %s' % date)

        with timer('retrieving/diff-ing FPDS Data', logger.info):
            to_insert, ids_to_delete = self.get_fpds_data(date=date)

        total_rows = len(to_insert)
        total_rows_delete = len(ids_to_delete)

        if total_rows_delete > 0:
            with timer('deleting stale FPDS data', logger.info):
                self.delete_stale_fpds(ids_to_delete=ids_to_delete)
        else:
            logger.info('Nothing to delete...')

        if total_rows > 0:
            # Add FPDS records
            with timer('inserting new FPDS data', logger.info):
                self.insert_new_fpds(to_insert=to_insert, total_rows=total_rows)

            # Update Awards based on changed FPDS records
            with timer('updating awards to reflect their latest associated transaction info', logger.info):
                update_awards(tuple(award_update_id_list))

            # Update FPDS-specific Awards based on the info in child transactions
            with timer('updating contract-specific awards to reflect their latest transaction info', logger.info):
                update_contract_awards(tuple(award_update_id_list))

            # Update AwardCategories based on changed FPDS records
            with timer('updating award category variables', logger.info):
                update_award_categories(tuple(award_update_id_list))

            # Check the linkages from file C to FPDS records and update any that are missing
            with timer('updating C->D linkages', logger.info):
                update_c_to_d_linkages('contract')
        else:
            logger.info('Nothing to insert...')

        # Update the date for the last time the data load was run
        ExternalDataLoadDate.objects.filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fpds']).delete()
        ExternalDataLoadDate(last_load_date=start_date,
                             external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fpds']).save()

        logger.info('FPDS NIGHTLY UPDATE FINISHED!')
