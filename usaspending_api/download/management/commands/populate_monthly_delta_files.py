import boto3
import logging
import os
import pandas as pd
import re
import shutil
import subprocess
import tempfile

from collections import OrderedDict
from datetime import datetime, date
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Case, When, Value, CharField, F

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings as all_ats_mappings
from usaspending_api.common.csv_helpers import count_rows_in_csv_file
from usaspending_api.common.helpers.sql_helpers import generate_raw_quoted_query
from usaspending_api.download.filestreaming.csv_generation import split_and_zip_csvs
from usaspending_api.download.filestreaming.csv_source import CsvSource
from usaspending_api.download.helpers import pull_modified_agencies_cgacs, multipart_upload
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.references.models import ToptierAgency, SubtierAgency


logger = logging.getLogger('console')

AWARD_MAPPINGS = {
    'Contracts': {
        'agency_field': 'agency_id',
        'award_types': ['contracts', 'idvs'],
        'column_headers': {
            0: 'agency_id', 1: 'parent_award_agency_id', 2: 'award_id_piid', 3: 'modification_number',
            4: 'parent_award_id', 5: 'transaction_number'
        },
        'correction_delete_ind': 'correction_delete_ind',
        'date_filter': 'updated_at',
        'letter_name': 'd1',
        'match': re.compile(r'(?P<month>\d{2})-(?P<day>\d{2})-(?P<year>\d{4})_delete_records_(IDV|award)_\d{10}.csv'),
        'model': 'contract_data',
        'unique_iden': 'detached_award_proc_unique'
    },
    'Assistance': {
        'agency_field': 'awarding_sub_agency_code',
        'award_types': ['grants', 'direct_payments', 'loans', 'other_financial_assistance'],
        'column_headers': {
            0: 'modification_number', 1: 'awarding_sub_agency_code', 2: 'award_id_fain', 3: 'award_id_uri'
        },
        'correction_delete_ind': 'correction_delete_ind',
        'date_filter': 'modified_at',
        'letter_name': 'd2',
        'match': re.compile(r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})_FABSdeletions_\d{10}.csv'),
        'model': 'assistance_data',
        'unique_iden': 'afa_generated_unique'
    }
}


class Command(BaseCommand):

    def download(self, award_type, agency='all', generate_since=None):
        """ Create a delta file based on award_type, and agency_code (or all agencies) """
        logger.info('Starting generation. {}, Agency: {}'.format(award_type, agency if agency == 'all' else
                                                                 agency['name']))
        award_map = AWARD_MAPPINGS[award_type]

        # Create Source and update fields to include correction_delete_ind
        source = CsvSource('transaction', award_map['letter_name'].lower(), 'transactions',
                           'all' if agency == 'all' else agency['toptier_agency_id'])
        source.query_paths.update({
            'correction_delete_ind': award_map['correction_delete_ind']
        })
        if award_type == 'Contracts':
            # Add the agency_id column to the mappings
            source.query_paths.update({'agency_id': 'transaction__contract_data__agency_id'})
            source.query_paths.move_to_end('agency_id', last=False)
        source.query_paths.move_to_end('correction_delete_ind', last=False)
        source.human_names = list(source.query_paths.keys())

        # Apply filters to the queryset
        filters, agency_code = self.parse_filters(award_map['award_types'], agency)
        source.queryset = VALUE_MAPPINGS['transactions']['filter_function'](filters)

        if award_type == 'Contracts':
            source.queryset = source.queryset.annotate(
                correction_delete_ind=Case(
                    When(transaction__contract_data__created_at__lt=generate_since, then=Value('C')),
                    default=Value(''),
                    output_field=CharField()))
        else:
            indicator_field = F('transaction__assistance_data__correction_delete_indicatr')
            source.queryset = source.queryset.annotate(
                correction_delete_ind=Case(
                    When(transaction__assistance_data__updated_at__gt=generate_since, then=indicator_field),
                    When(transaction__transactiondelta__isnull=False, then=Value('C')),
                    default=indicator_field,
                    output_field=CharField()))

        transaction_delta_queryset = source.queryset
        source.queryset = source.queryset.filter(**{
            'transaction__{}__{}__gte'.format(award_map['model'], award_map['date_filter']): generate_since
        })

        # UNION the normal results to the transaction_delta results.
        source.queryset = source.queryset.union(
            transaction_delta_queryset.filter(transaction__transactiondelta__isnull=False))

        # Generate file
        file_path = self.create_local_file(award_type, source, agency_code, generate_since)
        if file_path is None:
            logger.info('No new, modified, or deleted data; discarding file')
        elif not settings.IS_LOCAL:
            # Upload file to S3 and delete local version
            logger.info('Uploading file to S3 bucket and deleting local copy')
            multipart_upload(settings.MONTHLY_DOWNLOAD_S3_BUCKET_NAME, settings.USASPENDING_AWS_REGION, file_path,
                             os.path.basename(file_path))
            os.remove(file_path)

        logger.info('Finished generation. {}, Agency: {}'.format(award_type, agency if agency == 'all' else
                                                                 agency['name']))

    def create_local_file(self, award_type, source, agency_code, generate_since):
        """ Generate complete file from SQL query and S3 bucket deletion files, then zip it locally """
        logger.info('Generating CSV file with creations and modifications')

        # Create file paths and working directory
        timestamp = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S%f')
        working_dir = '{}_{}_delta_gen_{}/'.format(settings.CSV_LOCAL_PATH, agency_code, timestamp)
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)
        source_name = '{}_{}_Delta_{}'.format(agency_code, award_type, datetime.strftime(date.today(), '%Y%m%d'))
        source_path = os.path.join(working_dir, '{}.csv'.format(source_name))

        # Create a unique temporary file with the raw query
        raw_quoted_query = generate_raw_quoted_query(source.row_emitter(None))  # None requests all headers
        csv_query_annotated = self.apply_annotations_to_sql(raw_quoted_query, source.human_names)
        (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix='bd_sql_', dir='/tmp')
        with open(temp_sql_file_path, 'w') as file:
            file.write('\\copy ({}) To STDOUT with CSV HEADER'.format(csv_query_annotated))

        logger.info('Generated temp SQL file {}'.format(temp_sql_file_path))
        # Generate the csv with \copy
        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        try:
            subprocess.check_output(['psql', '-o', source_path, os.environ['DOWNLOAD_DATABASE_URL'], '-v',
                                     'ON_ERROR_STOP=1'], stdin=cat_command.stdout, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            logger.exception(e.output)
            raise e

        # Append deleted rows to the end of the file
        self.add_deletion_records(source_path, working_dir, award_type, agency_code, source, generate_since)
        if count_rows_in_csv_file(source_path, has_header=True, safe=True) > 0:
            # Split the CSV into multiple files and zip it up
            zipfile_path = '{}{}.zip'.format(settings.CSV_LOCAL_PATH, source_name)

            logger.info('Creating compressed file: {}'.format(os.path.basename(zipfile_path)))
            split_and_zip_csvs(zipfile_path, source_path, source_name)
        else:
            zipfile_path = None

        os.close(temp_sql_file)
        os.remove(temp_sql_file_path)
        shutil.rmtree(working_dir)

        return zipfile_path

    def add_deletion_records(self, source_path, working_dir, award_type, agency_code, source, generate_since):
        """ Retrieve deletion files from S3 and append necessary records to the end of the the file """
        logger.info('Retrieving deletion records from S3 files and appending to the CSV')

        # Retrieve all SubtierAgency IDs within this TopTierAgency
        subtier_agencies = list(SubtierAgency.objects.filter(agency__toptier_agency__cgac_code=agency_code).
                                values_list('subtier_code', flat=True))

        # Create a list of keys in the bucket that match the date range we want
        bucket = boto3.resource('s3', region_name=settings.USASPENDING_AWS_REGION).Bucket(settings.FPDS_BUCKET_NAME)

        all_deletions = pd.DataFrame()
        for key in bucket.objects.all():
            match_date = self.check_regex_match(award_type, key.key, generate_since)
            if match_date:
                # Create a local copy of the deletion file
                delete_filepath = '{}{}'.format(working_dir, key.key)
                bucket.download_file(key.key, delete_filepath)
                df = pd.read_csv(delete_filepath)
                os.remove(delete_filepath)

                # Split unique identifier into usable columns and add unused columns
                df = df[AWARD_MAPPINGS[award_type]['unique_iden']].apply(lambda x: pd.Series(x.split('_'))) \
                    .replace('-none-', '', regex=True).replace('-NONE-', '', regex=True) \
                    .rename(columns=AWARD_MAPPINGS[award_type]['column_headers'])

                # Only include records within the correct agency, and populated files
                if len(df.index) == 0:
                    continue
                if agency_code != 'all':
                    df = df[df[AWARD_MAPPINGS[award_type]['agency_field']].isin(subtier_agencies)]
                    if len(df.index) == 0:
                        continue

                # Reorder columns to make it CSV-ready, and append
                df = self.organize_deletion_columns(source, df, award_type, match_date)
                logger.info('Found {} deletion records to include'.format(len(df.index)))
                all_deletions = all_deletions.append(df, ignore_index=True)

        # Only append to file if there are any records
        if len(all_deletions.index) == 0:
            logger.info('No deletion records to append to file')
        else:
            self.add_deletions_to_file(all_deletions, award_type, source_path)

    def organize_deletion_columns(self, source, dataframe, award_type, match_date):
        """ Ensure that the dataframe has all necessary columns in the correct order """
        if award_type == 'Contracts':
            ordered_columns = ['correction_delete_ind'] + source.columns(None)
        else:
            ordered_columns = source.columns(None)

        # Loop through columns and populate rows for each
        unique_values_map = {
            'correction_delete_ind': 'D', 'last_modified_date': match_date
        }
        for header in ordered_columns:
            if header in unique_values_map:
                dataframe[header] = [unique_values_map[header]] * len(dataframe.index)

            elif header not in list(AWARD_MAPPINGS[award_type]['column_headers'].values()):
                dataframe[header] = [''] * len(dataframe.index)

        # Ensure columns are in correct order
        return dataframe[ordered_columns]

    def add_deletions_to_file(self, df, award_type, source_path):
        """ Append the deletion records to the end of the CSV file """
        logger.info('Removing duplicates from deletion records')
        df = df.sort_values(['last_modified_date'] + list(AWARD_MAPPINGS[award_type]['column_headers'].values()))
        deduped_df = df.drop_duplicates(subset=list(AWARD_MAPPINGS[award_type]['column_headers'].values()), keep='last')
        logger.info('Removed {} duplicated deletion records'.format(len(df.index) - len(deduped_df.index)))

        logger.info('Appending {} records to the end of the file'.format(len(deduped_df.index)))
        deduped_df.to_csv(source_path, mode='a', header=False, index=False)

    def check_regex_match(self, award_type, file_name, generate_since):
        """ Create a date object from a regular expression match """
        re_match = re.match(AWARD_MAPPINGS[award_type]['match'], file_name)
        if not re_match:
            return False

        year = re_match.group('year')
        month = re_match.group('month')
        day = re_match.group('day')

        # Ignore files that are not within the script's time frame
        # Note: Contract deletion files are made in the evening, Assistance files in the morning
        file_date = date(int(year), int(month), int(day))
        generate_since_date = datetime.strptime(generate_since, "%Y-%m-%d").date()
        if (award_type == 'Assistance' and file_date <= generate_since_date) or \
           (award_type == 'Contracts' and file_date < generate_since_date):
            return False

        return '{}-{}-{}'.format(year, month, day)

    def apply_annotations_to_sql(self, raw_query, aliases):
        """ The csv_generation version of this function would incorrectly annotate the D1 correction_delete_ind.
        Reusing the code but including the correction_delete_ind col for both file types """
        select_string = re.findall('SELECT (.*?) FROM', raw_query)[0]

        selects, cases = [], []
        for select in select_string.split(','):
            if 'CASE' not in select:
                selects.append(select.strip())
            else:
                case = select.strip()
                cases.append(case)
                aliases.remove(re.findall('AS "(.*?)"', case)[0].strip())
        if len(selects) != len(aliases):
            raise Exception("Length of alises doesn't match the columns in selects")

        ordered_map = OrderedDict(zip(aliases, selects))
        selects_str = ", ".join(cases + ['{} AS \"{}\"'.format(select, alias) for alias, select in ordered_map.items()])

        return raw_query.replace(select_string, selects_str)

    def parse_filters(self, award_types, agency):
        """ Convert readable filters to a filter object usable for the matview filter """
        filters = {
            'award_type_codes': [award_type for sublist in award_types for award_type in all_ats_mappings[sublist]]
        }

        agency_code = agency
        if agency != 'all':
            agency_code = agency['cgac_code']
            filters['agencies'] = [{'type': 'awarding', 'tier': 'toptier', 'name': agency['name']}]

        return filters, agency_code

    def add_arguments(self, parser):
        """ Add arguments to the parser """
        parser.add_argument('--agencies', dest='agencies', nargs='+', default=None, type=str,
                            help='Specific toptier agency database ids. Note \'all\' may be provided to account for '
                                 'the downloads that comprise all agencies. Defaults to \'all\' and all individual '
                                 'agencies.')
        parser.add_argument('--award_types', dest='award_types', nargs='+', default=['assistance', 'contracts'],
                            type=str, help='Specific award types, must be \'contracts\' and/or \'assistance\'. '
                                           'Defaults to both.')
        parser.add_argument('--last_date', dest='last_date', default=None, type=str, required=True,
                            help='Date of last Delta file creation.')

    def handle(self, *args, **options):
        """ Run the application. """
        agencies = options['agencies']
        award_types = options['award_types']
        last_date = options['last_date']

        toptier_agencies = ToptierAgency.objects.filter(cgac_code__in=set(pull_modified_agencies_cgacs()))
        include_all = True
        if agencies:
            if 'all' in agencies:
                agencies.remove('all')
            else:
                include_all = False
            toptier_agencies = ToptierAgency.objects.filter(toptier_agency_id__in=agencies)
        toptier_agencies = list(toptier_agencies.order_by('cgac_code').values('name', 'toptier_agency_id', 'cgac_code'))

        if include_all:
            toptier_agencies.append('all')

        for agency in toptier_agencies:
            for award_type in award_types:
                self.download(award_type.capitalize(), agency, last_date)

        logger.info(
            'IMPORTANT: Be sure to run synchronize_transaction_delta management command '
            'after a successful monthly delta run')
