import logging
import os
import re
import shutil
import subprocess
import tempfile
import zipfile

from collections import OrderedDict
from datetime import datetime, date
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Case, When, Value, CharField

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.common.helpers import generate_raw_quoted_query, generate_fiscal_year
from usaspending_api.download.filestreaming.csv_generation import CsvSource
from usaspending_api.download.helpers import split_csv, pull_modified_agencies_cgacs, multipart_upload
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger('console')

EXCEL_ROW_LIMIT = 1000000
AWARD_MAPPINGS = {
    'Contracts': {
        'award_types': ['contracts'],
        'correction_delete_ind': 'correction_delete_ind',
        'date_filter': 'updated_at',
        'letter_name': 'd1',
        'model': 'contract_data'
    },
    'Assistance': {
        'award_types': ['grants', 'direct_payments', 'loans', 'other_financial_assistance'],
        'correction_delete_ind': 'transaction__assistance_data__correction_late_delete_ind',
        'date_filter': 'modified_at',
        'letter_name': 'd2',
        'model': 'assistance_data'
    }
}


class Command(BaseCommand):

    def download(self, award_type, fiscal_year, agency='all', generate_since=None):
        """Create a delta file based on award_type, fiscal_year, and agency_code (or all agencies)"""
        award_map = AWARD_MAPPINGS[award_type]

        # Create Source and update fields to include correction_delete_ind
        source = CsvSource('transaction', award_map['letter_name'].lower(), 'transactions')
        source.query_paths.update({
            'correction_delete_ind': award_map['correction_delete_ind']
        })
        source.query_paths.move_to_end('correction_delete_ind', last=False)
        source.human_names = list(source.query_paths.keys())

        # Apply filters to the queryset
        filters, agency_code = self.parse_filters(award_map['award_types'], fiscal_year, agency)
        source.queryset = VALUE_MAPPINGS['transactions']['filter_function'](filters)
        if award_type == 'Contracts':
            # Derive the correction_delete_ind from the created_at of the records
            source.queryset = source.queryset. \
                annotate(correction_delete_ind=Case(When(transaction__contract_data__created_at__gte=generate_since,
                                                    then=Value('C')), default=Value(''), output_field=CharField()))
        source.queryset = source.queryset.filter(**{
            'transaction__{}__{}__gte'.format(award_map['model'], award_map['date_filter']): generate_since
        })
        source_query = source.row_emitter(None)

        if source_query.count() == 0:
            logger.info('No data for {}, FY{}, Agency: {}'.format(award_type, fiscal_year, agency['name']
                                                                  if agency != 'all' else 'all agencies'))
        else:
            logger.info('Starting generation. {}, FY{}, Agency: {}'.format(award_type, fiscal_year, agency['name']
                                                                           if agency != 'all' else 'all agencies'))
            # Generate file
            file_path = self.create_local_file(self, award_type, source_query, source, fiscal_year, agency_code)

            if not settings.is_local:
                # Upload file to S3
                multipart_upload(settings.MONTHLY_DOWNLOAD_S3_BUCKET_NAME, settings.BULK_DOWNLOAD_AWS_REGION, file_path,
                                 os.path.basename(file_path))
                # Delete file
                os.remove(file_path)

    def create_local_file(self, award_type, source_query, source, fiscal_year, agency_code):
        # Create file paths and working directory
        working_dir = settings.CSV_LOCAL_PATH + 'delta_gen/'
        if not os.path.exists(working_dir):
            os.mkdir(working_dir)
        source_name = '{}_{}'.format(award_type, VALUE_MAPPINGS['transactions']['download_name'])
        source_path = os.path.join(working_dir, '{}.csv'.format(source_name))

        # Create a unique temporary file with the raw query
        raw_quoted_query = generate_raw_quoted_query(source_query)
        csv_query_annotated = self.apply_annotations_to_sql(raw_quoted_query, source.human_names)
        (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix='bd_sql_', dir='/tmp')
        with open(temp_sql_file_path, 'w') as file:
            file.write('\\copy ({}) To STDOUT with CSV HEADER'.format(csv_query_annotated))

        # Generate the csv with \copy
        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.check_output(['psql', '-o', source_path, os.environ['DOWNLOAD_DATABASE_URL'], '-v',
                                 'ON_ERROR_STOP=1'], stdin=cat_command.stdout, stderr=subprocess.STDOUT)

        # Split CSV into separate files
        split_csvs = split_csv(source_path, row_limit=EXCEL_ROW_LIMIT, output_path=os.path.dirname(source_path),
                               output_name_template='{}_delta_%s.csv'.format(source_name))

        # Zip the split CSVs into one zipfile
        zipfile_path = '{}{}_{}_{}_Delta_{}.zip'.format(settings.CSV_LOCAL_PATH, fiscal_year, agency_code, award_type,
                                                        datetime.strftime(date.today(), '%Y%m%d'))
        zipped_csvs = zipfile.ZipFile(zipfile_path, 'a', compression=zipfile.ZIP_DEFLATED, allowZip64=True)
        for split_csv_part in split_csvs:
            zipped_csvs.write(split_csv_part, os.path.basename(split_csv_part))

        os.close(temp_sql_file)
        os.remove(temp_sql_file_path)
        shutil.rmtree(working_dir)

        return source_path

    def apply_annotations_to_sql(self, raw_query, aliases):
        """The csv_generation version of this function would incorrectly annotate the D1 correction_delete_ind.
        Reusing the code but including the correction_delete_ind col for both file types"""
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

    def parse_filters(self, award_types, fiscal_year, agency):
        """Convert readable filters to a filter object usable for the matview filter"""
        award_type_codes = []
        for award_type in award_types:
            award_type_codes = award_type_codes + all_award_types_mappings[award_type]
        time_periods_list = [{
            'start_date': '{}-{}-{}'.format(str(fiscal_year-1), '10', '01'),
            'end_date': '{}-{}-{}'.format(str(fiscal_year), '09', '30'),
            'date_type': 'action_date'}]

        filters = {'award_type_codes': award_type_codes, 'time_period': time_periods_list}
        agency_code = agency
        if agency != 'all':
            agency_code = agency['cgac_code']
            filters['agencies'] = [{'type': 'awarding', 'tier': 'toptier', 'name': agency['name']}]

        return filters, agency_code

    def add_arguments(self, parser):
        """Add arguments to the parser"""
        parser.add_argument('--agencies', dest='agencies', nargs='+', default=None, type=int,
                            help='Specific toptier agency database ids. Note \'all\' may be provided to account for '
                                 'the downloads that comprise all agencies for a fiscal_year. Defaults to \'all\' and '
                                 'all individual agencies.')
        parser.add_argument('--award_types', dest='award_types', nargs='+', default=['assistance', 'contracts'],
                            type=str, help='Specific award types, must be \'contracts\' and/or \'assistance\'. '
                                           'Defaults to both.')
        parser.add_argument('--fiscal_years', dest='fiscal_years', nargs='+', default=None, type=int,
                            help='Specific Fiscal Years. Defaults to all FY since 2001.')
        parser.add_argument('--last_date', dest='last_date', default=None, type=str, required=True,
                            help='Date of last Delta file creation.')

    def handle(self, *args, **options):
        """Run the application."""
        agencies = options['agencies']
        award_types = options['award_types']
        fiscal_years = options['fiscal_years']
        last_date = options['last_date']

        if not fiscal_years:
            fiscal_years = range(2008, generate_fiscal_year(date.today())+1)

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
                for fiscal_year in fiscal_years:
                    self.download(award_type.capitalize(), fiscal_year, agency, last_date)
