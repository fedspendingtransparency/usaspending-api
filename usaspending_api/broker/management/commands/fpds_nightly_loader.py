import boto3
import csv
import logging
import os
import re
import timeit
import urllib.request
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction, IntegrityError

from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFPDS, TransactionNormalized, Award
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.broker import lookups
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, get_or_create_location
from usaspending_api.references.models import LegalEntity, Agency, ToptierAgency, SubtierAgency
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards, update_award_categories

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
    help = "Update FPDS data nightly"

    @staticmethod
    def get_fpds_data(db_cursor, date):
        # Connect to AWS
        aws_region = os.environ.get('AWS_REGION')
        fpds_bucket_name = os.environ.get('FPDS_BUCKET_NAME')

        if not (aws_region or fpds_bucket_name):
            raise Exception('Missing required environment variables: AWS_REGION, FPDS_BUCKET_NAME')
        
        s3client = boto3.client('s3', region_name=aws_region)
        s3resource = boto3.resource('s3', region_name=aws_region)
        s3_bucket = s3resource.Bucket(fpds_bucket_name)
        
        # The ORDER BY is important here because deletions must happen in a specific order and that order is defined
        # by the Broker's PK since every modification is a new row
        db_query = 'SELECT * ' \
                   'FROM detached_award_procurement ' \
                   'WHERE updated_at >= %s ' \
                   'ORDER BY detached_award_procurement_id ASC'
        db_args = [date]

        db_cursor.execute(db_query, db_args)
        db_rows = dictfetchall(db_cursor)  # this returns an OrderedDict

        ids_to_delete = []

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

    @staticmethod
    def delete_stale_fpds(ids_to_delete=None):
        logger.info('Starting deletion of stale FPDS data')

        if not ids_to_delete:
            return

        TransactionNormalized.objects.filter(contract_data__detached_award_procurement_id__in=ids_to_delete).delete()

    @staticmethod
    def insert_new_fpds(to_insert, total_rows):
        logger.info('Starting insertion of new FPDS data')

        place_of_performance_field_map = {
            # "city_name": "place_of_performance_locat", # location id doesn't mean it's a city. Can't use this mapping
            "congressional_code": "place_of_performance_congr",
            "state_code": "place_of_performance_state",
            "zip4": "place_of_performance_zip4a",
            "location_country_code": "place_of_perform_country_c"

        }

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

        start_time = datetime.now()

        for index, row in enumerate(to_insert, 1):
            if not (index % 100):
                logger.info('Inserting Stale FPDS: Inserting row {} of {} ({})'.format(str(index),
                                                                                       str(total_rows),
                                                                                       datetime.now() - start_time))

            legal_entity_location, created = get_or_create_location(
                legal_entity_location_field_map, row, {"recipient_flag": True}
            )

            recipient_name = row['awardee_or_recipient_legal']
            if recipient_name is None:
                recipient_name = ""

            # Handling the case of duplicates, just grab the first match
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
            }

            contract_field_map = {
                "type": "contract_award_type",
                "description": "award_description"
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

            detached_award_proc_unique = contract_instance['detached_award_proc_unique']

            if TransactionFPDS.objects.filter(detached_award_proc_unique=detached_award_proc_unique).exists():
                TransactionFPDS.objects.filter(detached_award_proc_unique=detached_award_proc_unique).\
                    update(**contract_instance)
            else:
                transaction_contract = TransactionFPDS(transaction=transaction, **contract_instance)
                transaction_contract.save()

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

        db_cursor = connections['data_broker'].cursor()
        if options.get('date'):
            date = options.get('date')[0]
        else:
            data_load_date_obj = ExternalDataLoadDate.objects. \
                filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fpds']).first()
            if not data_load_date_obj:
                date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                date = data_load_date_obj.last_load_date

        logger.info('Processing data for FPDS starting from %s' % date)

        logger.info('Retrieving FPDS Data...')
        start = timeit.default_timer()
        to_insert, ids_to_delete = self.get_fpds_data(db_cursor=db_cursor, date=date)
        end = timeit.default_timer()
        logger.info('Finished diff-ing FPDS data in ' + str(end - start) + ' seconds')

        total_rows = len(to_insert)
        total_rows_delete = len(ids_to_delete)

        if total_rows_delete > 0:
            logger.info('Deleting stale FPDS data...')
            start = timeit.default_timer()
            self.delete_stale_fpds(ids_to_delete=ids_to_delete)
            end = timeit.default_timer()
            logger.info('Finished deleting stale FPDS data in ' + str(end - start) + ' seconds')
        else:
            logger.info('Nothing to delete...')

        if total_rows > 0:
            logger.info('Inserting new FPDS data...')
            start = timeit.default_timer()
            self.insert_new_fpds(to_insert=to_insert, total_rows=total_rows)
            end = timeit.default_timer()
            logger.info('Finished inserting new FPDS data in ' + str(end - start) + ' seconds')

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
            logger.info('Nothing to insert...')

        # Update the date for the last time the data load was run
        ExternalDataLoadDate.objects.filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fpds']).delete()
        ExternalDataLoadDate(last_load_date=datetime.now().strftime('%Y-%m-%d'),
                             external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fpds']).save()

        logger.info('FPDS NIGHTLY UPDATE FINISHED!')
