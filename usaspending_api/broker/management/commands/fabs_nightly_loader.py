import boto3
import logging
import time

from datetime import datetime, timedelta, timezone
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.db.models import Count

from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award
from usaspending_api.broker import lookups
from usaspending_api.broker.helpers import get_business_categories
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values
from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.common.helpers.generic_helper import fy, timer
from usaspending_api.etl.award_helpers import update_awards, update_award_categories
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.management.load_base import load_data_into_model, format_date, create_location
from usaspending_api.references.models import LegalEntity, Agency


logger = logging.getLogger("console")

AWARD_UPDATE_ID_LIST = []
BATCH_FETCH_SIZE = 25000


class Command(BaseCommand):
    help = "Update FABS data in USAspending from a Broker DB"

    @staticmethod
    def get_fabs_records_to_delete(date, end_date):
        db_cursor = connections["data_broker"].cursor()
        db_query = """
            SELECT UPPER(afa_generated_unique) AS afa_generated_unique
            FROM   published_award_financial_assistance
            WHERE  created_at >= %(start_date)s
            AND    (created_at < %(end_date)s OR %(end_date)s IS NULL)
            AND    UPPER(correction_delete_indicatr) = 'D'
        """

        db_cursor.execute(db_query, {'start_date': date, 'end_date': end_date})
        db_rows = [id[0] for id in db_cursor.fetchall()]
        logger.info("Number of records to delete: %s" % str(len(db_rows)))
        return db_rows

    @staticmethod
    def get_fabs_transaction_ids(date, end_date):
        db_cursor = connections["data_broker"].cursor()
        db_query = """
            SELECT published_award_financial_assistance_id
            FROM   published_award_financial_assistance
            WHERE  created_at >= %(start_date)s
            AND    (created_at < %(end_date)s OR %(end_date)s IS NULL)
            AND    is_active IS TRUE
            AND    UPPER(correction_delete_indicatr) IS DISTINCT FROM 'D'
        """

        db_cursor.execute(db_query, {'start_date': date, 'end_date': end_date})
        db_rows = [id[0] for id in db_cursor.fetchall()]
        logger.info("Number of records to insert/update: %s" % str(len(db_rows)))
        return db_rows

    @staticmethod
    def fetch_fabs_data_generator(dap_uid_list):
        db_cursor = connections["data_broker"].cursor()
        db_query = " ".join(
            [
                "SELECT * FROM published_award_financial_assistance",
                "WHERE published_award_financial_assistance_id IN ({});"
            ]
        )

        total_uid_count = len(dap_uid_list)

        for i in range(0, total_uid_count, BATCH_FETCH_SIZE):
            start_time = time.perf_counter()
            max_index = i + BATCH_FETCH_SIZE if i + BATCH_FETCH_SIZE < total_uid_count else total_uid_count
            fabs_ids_batch = dap_uid_list[i:max_index]

            log_msg = "Fetching {}-{} out of {} records from broker"
            logger.info(log_msg.format(i + 1, max_index, total_uid_count))

            db_cursor.execute(db_query.format(",".join(str(id) for id in fabs_ids_batch)))
            logger.info("Fetching records took {:.2f}s".format(time.perf_counter() - start_time))
            yield dictfetchall(db_cursor)  # this returns an OrderedDict

    def find_related_awards(self, transactions):
        related_award_ids = [result[0] for result in transactions.values_list('award_id')]
        tn_count = (
            TransactionNormalized.objects.filter(award_id__in=related_award_ids)
            .values('award_id')
            .annotate(transaction_count=Count('id'))
            .values_list('award_id', 'transaction_count')
        )
        tn_count_filtered = (
            transactions.values('award_id')
            .annotate(transaction_count=Count('id'))
            .values_list('award_id', 'transaction_count')
        )
        tn_count_mapping = {award_id: transaction_count for award_id, transaction_count in tn_count}
        tn_count_filtered_mapping = {award_id: transaction_count for award_id, transaction_count in tn_count_filtered}
        # only delete awards if and only if all their transactions are deleted, otherwise update the award
        update_awards = [
            award_id
            for award_id, transaction_count in tn_count_mapping.items()
            if tn_count_filtered_mapping[award_id] != transaction_count
        ]
        delete_awards = [
            award_id
            for award_id, transaction_count in tn_count_mapping.items()
            if tn_count_filtered_mapping[award_id] == transaction_count
        ]
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
            fabs = 'DELETE FROM "transaction_fabs" tf WHERE tf."transaction_id" IN ({});'
            tn = 'DELETE FROM "transaction_normalized" tn WHERE tn."id" IN ({});'
            queries.extend([fabs.format(delete_transaction_str_ids), tn.format(delete_transaction_str_ids)])
        # Update Awards
        if update_award_ids:
            # Adding to AWARD_UPDATE_ID_LIST so the latest_transaction will be recalculated
            AWARD_UPDATE_ID_LIST.extend(update_award_ids)
            update_awards = 'UPDATE "awards" SET "latest_transaction_id" = null WHERE "id" IN ({});'
            update_awards_query = update_awards.format(update_award_str_ids)
            queries.append(update_awards_query)
        if delete_award_ids:
            # Financial Accounts by Awards
            faba = 'UPDATE "financial_accounts_by_awards" SET "award_id" = null WHERE "award_id" IN ({});'
            # Subawards
            sub = 'UPDATE "subaward" SET "award_id" = null WHERE "award_id" IN ({});'.format(delete_award_str_ids)
            # Delete Awards
            delete_awards_query = 'DELETE FROM "awards" a WHERE a."id" IN ({});'.format(delete_award_str_ids)
            queries.extend([faba.format(delete_award_str_ids), sub, delete_awards_query])
        if queries:
            db_query = ''.join(queries)
            db_cursor.execute(db_query, [])

    @transaction.atomic
    def insert_all_new_fabs(self, all_new_to_insert):
        for to_insert in self.fetch_fabs_data_generator(all_new_to_insert):
            start = time.perf_counter()
            self.insert_new_fabs(to_insert=to_insert)
            logger.info("FABS insertions took {:.2f}s".format(time.perf_counter() - start))

    def insert_new_fabs(self, to_insert):
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
            "zip5": "place_of_performance_zip5",
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
            "foreign_city_name": "legal_entity_foreign_city",
        }

        for row in to_insert:
            upper_case_dict_values(row)

            # Create new LegalEntityLocation and LegalEntity from the row data
            legal_entity_location = create_location(legal_entity_location_field_map, row, {"recipient_flag": True})
            recipient_name = row['awardee_or_recipient_legal']
            legal_entity = LegalEntity.objects.create(
                recipient_unique_id=row['awardee_or_recipient_uniqu'],
                recipient_name=recipient_name if recipient_name is not None else "",
                parent_recipient_unique_id=row['ultimate_parent_unique_ide'],
            )
            legal_entity_value_map = {
                "location": legal_entity_location,
                "business_categories": get_business_categories(row=row, data_type='fabs'),
                "business_types_description": row['business_types_desc'],
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
                msg = "Invalid record type encountered for the following afa_generated_unique record: {}"
                raise Exception(msg.format(row['afa_generated_unique']))

            astac = row["awarding_sub_tier_agency_c"] if row["awarding_sub_tier_agency_c"] else "-NONE-"
            generated_unique_id = "ASST_AW_{}_{}_{}".format(astac, fain, uri)

            # Create the summary Award
            (created, award) = Award.get_or_create_summary_award(
                generated_unique_award_id=generated_unique_id,
                fain=row['fain'],
                uri=row['uri'],
                record_type=row['record_type'],
            )
            award.save()

            # Append row to list of Awards updated
            AWARD_UPDATE_ID_LIST.append(award.id)

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
                "generated_unique_award_id": generated_unique_id,
            }

            fad_field_map = {
                "type": "assistance_type",
                "description": "award_description",
                "funding_amount": "total_funding_amount",
            }

            transaction_normalized_dict = load_data_into_model(
                TransactionNormalized(),  # thrown away
                row,
                field_map=fad_field_map,
                value_map=parent_txn_value_map,
                as_dict=True,
            )

            financial_assistance_data = load_data_into_model(TransactionFABS(), row, as_dict=True)  # thrown away

            afa_generated_unique = financial_assistance_data['afa_generated_unique']
            unique_fabs = TransactionFABS.objects.filter(afa_generated_unique=afa_generated_unique)

            if unique_fabs.first():
                transaction_normalized_dict["update_date"] = datetime.now(timezone.utc)
                transaction_normalized_dict["fiscal_year"] = fy(transaction_normalized_dict["action_date"])

                # Update TransactionNormalized
                TransactionNormalized.objects.filter(id=unique_fabs.first().transaction.id).update(
                    **transaction_normalized_dict
                )

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
    def store_deleted_fabs(ids_to_delete):
        seconds = int(time.time())  # adds enough uniqueness to filename
        file_name = datetime.now(timezone.utc).strftime('%Y-%m-%d') + "_FABSdeletions_" + str(seconds) + ".csv"
        file_with_headers = ['afa_generated_unique'] + ids_to_delete

        if settings.IS_LOCAL:
            file_path = settings.CSV_LOCAL_PATH + file_name
            logger.info("storing deleted transaction IDs at: {}".format(file_path))
            with open(file_path, 'w') as writer:
                for row in file_with_headers:
                    writer.write(row + '\n')
        else:
            logger.info('Uploading FABS delete data to S3 bucket')
            aws_region = settings.USASPENDING_AWS_REGION
            fpds_bucket_name = settings.FPDS_BUCKET_NAME
            s3client = boto3.client('s3', region_name=aws_region)
            contents = bytes()
            for row in file_with_headers:
                contents += bytes('{}\n'.format(row).encode())
            s3client.put_object(Bucket=fpds_bucket_name, Key=file_name, Body=contents)

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            help="(OPTIONAL) Run the loader for FABS submissions created on or after this date. "
                 "Omitting this value will cause submissions created since the last run to be loaded. "
                 "Expected format: MM/DD/YYYY",
            metavar='START_DATE',
            required=False
        )

        parser.add_argument(
            '--end-date',
            help="(OPTIONAL) Run the loader for FABS submissions created before this date. "
                 "Omitting this value will result in all submissions created from START_DATE onward to be loaded. "
                 "Expected format: MM/DD/YYYY",
            required=False
        )

    def handle(self, *args, **options):
        logger.info("Starting FABS data load script...")
        start_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        fabs_load_db_id = lookups.EXTERNAL_DATA_TYPE_DICT['fabs']
        data_load_date_obj = ExternalDataLoadDate.objects.filter(external_data_type_id=fabs_load_db_id).first()

        if options.get("date"):  # if provided, use cli data
            load_from_date = options.get("date")
        elif data_load_date_obj:  # else if last run is in DB, use that
            load_from_date = data_load_date_obj.last_load_date
        else:  # Default is yesterday at midnight
            load_from_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')

        load_to_date = options.get("end_date")

        end_log = ' ending before %s' % load_to_date if load_to_date else ''
        logger.info('Processing data for FABS starting from %s%s' % (load_from_date, end_log))

        with timer('retrieving/diff-ing FABS Data', logger.info):
            upsert_transactions = self.get_fabs_transaction_ids(load_from_date, load_to_date)

        with timer("obtaining delete records", logger.info):
            ids_to_delete = self.get_fabs_records_to_delete(load_from_date, load_to_date)

        if ids_to_delete:
            self.store_deleted_fabs(ids_to_delete)

            # Delete FABS records by ID
            with timer("deleting stale FABS data", logger.info):
                self.delete_stale_fabs(ids_to_delete=ids_to_delete)
                del ids_to_delete
        else:
            logger.info("Nothing to delete...")

        if upsert_transactions:
            # Add FABS records
            with timer('inserting new FABS data', logger.info):
                self.insert_all_new_fabs(all_new_to_insert=upsert_transactions)

            # Update Awards based on changed FABS records
            with timer('updating awards to reflect their latest associated transaction info', logger.info):
                update_awards(tuple(AWARD_UPDATE_ID_LIST))

            # Update AwardCategories based on changed FABS records
            with timer('updating award category variables', logger.info):
                update_award_categories(tuple(AWARD_UPDATE_ID_LIST))

            # Check the linkages from file C to FABS records and update any that are missing
            with timer('updating C->D linkages', logger.info):
                update_c_to_d_linkages('assistance')
        else:
            logger.info('Nothing to insert...')

        # If an end date was provided, do NOT save the last_load_date.
        # last_load_date is designed for incremental, periodic updates,
        # not targeted time periods.
        if load_to_date is None:

            # Update the date for the last time the data load was run
            ExternalDataLoadDate.objects.filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).delete()
            ExternalDataLoadDate(
                last_load_date=start_date, external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']
            ).save()

        logger.info('FABS UPDATE FINISHED!')
