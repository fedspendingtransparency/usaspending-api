import logging
import os
import boto
import smart_open
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.conf import settings

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import fy, timer
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.awards.models import TransactionFABS, TransactionNormalized, Award
from usaspending_api.broker.models import ExternalDataLoadDate
from usaspending_api.broker import lookups
from usaspending_api.broker.helpers import (get_business_categories, get_business_type_description,
                                            get_assistance_type_description)
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
                   'AND (is_active IS True OR UPPER(correction_late_delete_ind) = \'D\')'
        db_args = [date]

        db_cursor.execute(db_query, db_args)
        db_rows = dictfetchall(db_cursor)  # this returns an OrderedDict

        ids_to_delete = []
        final_db_rows = []

        # Iterate through the result dict and determine what needs to be deleted and what needs to be added
        for row in db_rows:
            if row['correction_late_delete_ind'] and row['correction_late_delete_ind'].upper() == 'D':
                ids_to_delete.append(row['afa_generated_unique'].upper())
            else:
                final_db_rows.append(row)

        logger.info('Number of records to insert/update: %s' % str(len(final_db_rows)))
        logger.info('Number of records to delete: %s' % str(len(ids_to_delete)))

        return final_db_rows, ids_to_delete

    @staticmethod
    def get_fabs_historical_data(date, fiscal_year=None):
        logger.info('Getting historical data...')
        db_cursor = connections['data_broker'].cursor()

        # The ORDER BY is important here because deletions must happen in a specific order and that order is defined
        # by the Broker's PK since every modification is a new row
        db_query = '''
            SELECT *
            FROM published_award_financial_assistance AS pafa
            WHERE
                is_historical IS TRUE
                AND
                is_active IS TRUE
                AND
                updated_at = %s
        '''

        date_time = date + ' 00:00:00'
        db_args = [date_time]
        if fiscal_year:
            if db_args:
                db_query += ' AND'
            else:
                db_query += ' WHERE'

            fy_begin = '10/01/' + str(fiscal_year - 1)
            fy_end = '09/30/' + str(fiscal_year)

            db_query += ' action_date::Date BETWEEN %s AND %s'
            db_args += [fy_begin, fy_end]

        db_cursor.execute(db_query, db_args)
        db_rows = dictfetchall(db_cursor)  # this returns an OrderedDict

        logger.info('Number of records to insert/update: %s' % str(len(db_rows)))

        return db_rows, []

    @staticmethod
    def delete_stale_fabs(ids_to_delete=None):
        logger.info('Starting deletion of stale FABS data')

        if not ids_to_delete:
            return

        # This cascades deletes for TransactionFABS & Awards in addition to deleting TransactionNormalized records
        TransactionNormalized.objects.filter(assistance_data__afa_generated_unique__in=ids_to_delete).delete()

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

            for key in row:
                if isinstance(row[key], str):
                    row[key] = row[key].upper()

            # Create new LegalEntityLocation and LegalEntity from the row data
            legal_entity_location = create_location(legal_entity_location_field_map, row, {"recipient_flag": True})
            recipient_name = row['awardee_or_recipient_legal']
            legal_entity = LegalEntity.objects.create(
                recipient_unique_id=row['awardee_or_recipient_uniqu'],
                recipient_name=recipient_name if recipient_name is not None else ""
            )
            legal_entity_value_map = {
                "location": legal_entity_location,
                "business_categories": get_business_categories(row=row, data_type='fabs'),
                "business_types_description": get_business_type_description(row['business_types'])
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
            elif record_type_int == 2:
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
                "type_description": get_assistance_type_description(row['assistance_type']),
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
            '-d',
            '--date',
            dest="date",
            nargs='+',
            type=str,
            help="(OPTIONAL) Date from which to start the nightly loader. Expected format: MM/DD/YYYY"
        )

        parser.add_argument(
            '-hl',
            '--historical',
            dest="historical",
            action='store_true',
            default=False,
            help='Flag to run FABS nightly load for historical data'
        )

        parser.add_argument(
            '-fy',
            '--fiscal_year',
            dest="fiscal_year",
            nargs='+',
            type=int,
            help="Year for which to run the historical load"
        )

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting FABS nightly data load...')
        historical_flag = options.get('historical')
        fiscal_year = options.get('fiscal_year')

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

        to_insert, ids_to_delete = 0, 0
        # Retrieve FABS data
        with timer('retrieving/diff-ing FABS Data', logger.info):
            if historical_flag:
                try:
                    fiscal_year = int(fiscal_year[0])
                except:
                    raise InvalidParameterException('Fiscal year is not in the proper integer format: YYYY')

                to_insert, ids_to_delete = self.get_fabs_historical_data(date=date, fiscal_year=fiscal_year)
            else:
                to_insert, ids_to_delete = self.get_fabs_data(date=date)

        total_rows = len(to_insert)
        total_rows_delete = len(ids_to_delete)

        if total_rows_delete > 0:
            # Create a file with the deletion IDs and place in a bucket for ElasticSearch
            self.send_deletes_to_elasticsearch(ids_to_delete)

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
        else:
            logger.info('Nothing to insert...')

        # Update the date for the last time the data load was run
        ExternalDataLoadDate.objects.filter(external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).delete()
        ExternalDataLoadDate(last_load_date=start_date,
                             external_data_type_id=lookups.EXTERNAL_DATA_TYPE_DICT['fabs']).save()

        logger.info('FABS NIGHTLY UPDATE FINISHED!')
