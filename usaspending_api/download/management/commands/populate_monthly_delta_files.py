import os
import re
import subprocess
import tempfile
import zipfile

from collections import OrderedDict
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Case, When, Value, CharField

from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.common.helpers import generate_raw_quoted_query
from usaspending_api.download.filestreaming.csv_generation import CsvSource
from usaspending_api.download.helpers import split_csv
from usaspending_api.download.lookups import VALUE_MAPPINGS

EXCEL_ROW_LIMIT = 1000000


class Command(BaseCommand):

    def download(self, award_type, generate_since, fiscal_year, agency='all'):
        """Create a delta file from the """
        award_type_mapping = {
            'contracts': {
                'letter_name': 'd1',
                'model': 'contract_data',
                'date_filter': 'updated_at',
                'correction_delete_ind': 'CASE WHEN transaction_fpds.created_at >= \'{}\' THEN \'C\' ELSE \'\' END'.
                                         format(generate_since)
            },
            'assistance': {
                'letter_name': 'd2',
                'model': 'assistance_data',
                'date_filter': 'modified_at',
                'correction_delete_ind': 'transaction__assistance_data__correction_late_delete_ind'
            }
        }
        award_map = award_type_mapping[award_type]
        working_dir = settings.CSV_LOCAL_PATH

        # Create Source and update fields to include correction_delete_ind
        source = CsvSource('transaction', award_map['letter_name'].lower(), 'transactions')
        self.add_correction_delete_ind(award_type, source)

        # Apply filters to the queryset
        filters, agency_code = self.parse_filters(fiscal_year, agency)
        source.queryset = VALUE_MAPPINGS['transactions']['filter_function'](filters)
        if award_type == 'contracts':
            # Derive the correction_delete_ind from the created_at of the records
            source.queryset = source.queryset. \
                annotate(correction_delete_ind=Case(When(transaction__contract_data__created_at__gte=generate_since,
                                                    then=Value('C')), default=Value(''), output_field=CharField())). \
                filter(transaction__contract_data__updated_at__gte=generate_since)
        else:
            source.queryset = source.queryset.filter(transaction__assistance_data__updated_at__gte=generate_since)

        source_query = source.row_emitter(None)
        source_name = '{}_{}'.format(award_type.capitalize(), VALUE_MAPPINGS['transactions']['download_name'])
        source_path = os.path.join(working_dir, 'deltas/{}.csv'.format(source_name))

        # Create a unique temporary file with the raw query
        raw_quoted_query = generate_raw_quoted_query(source_query)
        csv_query_annotated = self.apply_annotations_to_sql(raw_quoted_query, source.human_names)
        (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix='bd_sql_', dir='/tmp')
        with open(temp_sql_file_path, 'w') as file:
            file.write('\copy ({}) To STDOUT with CSV HEADER'.format(csv_query_annotated))

        # Generate the csv with \copy
        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        subprocess.check_output(['psql', '-o', source_path, os.environ['DOWNLOAD_DATABASE_URL'], '-v',
                                 'ON_ERROR_STOP=1'], stdin=cat_command.stdout, stderr=subprocess.STDOUT)

        # Split CSV into separate files
        split_csvs = split_csv(source_path, row_limit=EXCEL_ROW_LIMIT, output_path=os.path.dirname(source_path),
                               output_name_template='{}_delta_%s.csv'.format(source_name))

        # Zip the split CSVs into one zipfile
        zipfile_path = '{}{}_{}_{}_Delta.zip'.format(working_dir, fiscal_year, agency_code, award_type.capitalize())
        zipped_csvs = zipfile.ZipFile(zipfile_path, 'a', compression=zipfile.ZIP_DEFLATED, allowZip64=True)
        for split_csv_part in split_csvs:
            zipped_csvs.write(split_csv_part, os.path.basename(split_csv_part))

        os.close(temp_sql_file)
        os.remove(temp_sql_file_path)

    def add_correction_delete_ind(self, award_type, source):
        """Retrieve the default lookups for the award_type, then add the correction_delete_ind if necessary"""
        # Add the correction_delete_indicator and move it to the beginning of the OrderedDict
        if award_type == 'contracts':
            indicator_col = 'correction_delete_ind'
        else:
            indicator_col = 'transaction__assistance_data__correction_late_delete_ind'

        # Move the correction_delete_ind to the beginning of the OrderedDict and update the Source's human_names
        source.query_paths.update({
            'correction_delete_ind': indicator_col
        })
        source.query_paths.move_to_end('correction_delete_ind', last=False)
        source.human_names = list(source.query_paths.keys())

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

    def parse_filters(self, fiscal_year, agency):
        filters = {
            'time_period': [{
                'start_date': '{}-{}-{}'.format(str(int(fiscal_year)-1), '10', '01'),
                'end_date': '{}-{}-{}'.format(fiscal_year, '09', '30'),
                'date_type': 'action_date'
            }]
        }
        agency_code = agency
        if agency != 'all':
            agency_code = agency['code']
            filters['agencies'] = [{'type': 'awarding', 'tier': 'toptier', 'name': agency['name']}]

        return filters, agency_code

    def handle(self, *args, **options):
        """Run the application."""
        self.download('contracts', '2018-04-01', '2018', {'code': '012', 'name': 'Department of Agriculture'})
