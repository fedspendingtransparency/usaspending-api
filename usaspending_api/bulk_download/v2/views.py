import sys
import itertools
import json
import logging
import os
import pandas as pd
import datetime
import re
import boto
from collections import OrderedDict

from django.conf import settings
from django.db.models import F, Q, Max
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.exceptions import NotFound
from rest_framework_extensions.cache.decorators import cache_response

from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping, \
    grant_type_mapping, direct_payment_type_mapping, loan_type_mapping, other_type_mapping
from usaspending_api.awards.models import Award, Subaward, Agency, TransactionNormalized
from usaspending_api.references.models import ToptierAgency
from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.csv_helpers import sqs_queue
from usaspending_api.common.helpers import generate_raw_quoted_query, order_nested_object
from usaspending_api.bulk_download.filestreaming import csv_selection
from usaspending_api.bulk_download.filestreaming.s3_handler import S3Handler
from usaspending_api.bulk_download.models import BulkDownloadJob
from usaspending_api.download.lookups import JOB_STATUS_DICT

# List of CFO CGACS for list agencies viewset in the correct order, names included for reference
# TODO: Find a solution that marks the CFO agencies in the database AND have the correct order
CFO_CGACS_MAPPING = OrderedDict([('012', 'Department of Agriculture'),
                                 ('013', 'Department of Commerce'),
                                 ('097', 'Department of Defense'),
                                 ('091', 'Department of Education'),
                                 ('089', 'Department of Energy'),
                                 ('075', 'Department of Health and Human Services'),
                                 ('070', 'Department of Homeland Security'),
                                 ('086', 'Department of Housing and Urban Development'),
                                 ('015', 'Department of Justice'),
                                 ('1601', 'Department of Labor'),
                                 ('019', 'Department of State'),
                                 ('014', 'Department of the Interior'),
                                 ('020', 'Department of the Treasury'),
                                 ('069', 'Department of Transportation'),
                                 ('036', 'Department of Veterans Affairs'),
                                 ('068', 'Environmental Protection Agency'),
                                 ('047', 'General Services Administration'),
                                 ('080', 'National Aeronautics and Space Administration'),
                                 ('049', 'National Science Foundation'),
                                 ('031', 'Nuclear Regulatory Commission'),
                                 ('024', 'Office of Personnel Management'),
                                 ('073', 'Small Business Administration'),
                                 ('028', 'Social Security Administration'),
                                 ('072', 'Agency for International Development')])
CFO_CGACS = list(CFO_CGACS_MAPPING.keys())

logger = logging.getLogger('console')

# Mappings to help with the filters
award_type_mappings = {
    'contracts': list(contract_type_mapping.keys()),
    'grants': list(grant_type_mapping.keys()),
    'direct_payments': list(direct_payment_type_mapping.keys()),
    'loans': list(loan_type_mapping.keys()),
    'other_financial_assistance': list(other_type_mapping.keys())
}
award_mappings = {
    'contracts': ['contracts'],
    'assistance': ['grants', 'direct_payments', 'loans', 'other_financial_assistance']
}
value_mappings = {
    # Award Level
    # 'prime_awards': {
    #     'table': Award,
    #     'table_name': 'award',
    #     'download_name': 'awards',
    #     'action_date': 'latest_transaction__action_date',
    #     'last_modified_date': 'last_modified_date',
    #     'type': 'type',
    #     'awarding_agency_id': 'awarding_agency_id',
    #     'funding_agency_id': 'funding_agency_id',
    #     'contract_data': 'latest_transaction__contract_data',
    #     'assistance_data': 'latest_transaction__assistance_data'
    # },
    # Transaction Level
    'prime_awards': {
        'table': TransactionNormalized,
        'table_name': 'transaction',
        'download_name': 'awards',
        'action_date': 'action_date',
        'last_modified_date': 'last_modified_date',
        'type': 'award__type',
        'awarding_agency_id': 'awarding_agency_id',
        'funding_agency_id': 'funding_agency_id',
        'contract_data': 'contract_data',
        'assistance_data': 'assistance_data'
    },
    'sub_awards': {
        'table': Subaward,
        'table_name': 'subaward',
        'download_name': 'subawards',
        'action_date': 'action_date',
        'last_modified_date': 'award__last_modified_date',
        'type': 'award__type',
        'awarding_agency_id': 'awarding_agency_id',
        'funding_agency_id': 'funding_agency_id',
        'contract_data': 'award__latest_transaction__contract_data',
        'assistance_data': 'award__latest_transaction__assistance_data'
    }
}


class BaseDownloadViewSet(APIView):

    s3_handler = S3Handler(name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME, region=settings.BULK_DOWNLOAD_AWS_REGION)

    def get_download_response(self, file_name):
        """ Generate download response which encompasses various elements to provide accurate status for state
        of a download job"""

        download_job = BulkDownloadJob.objects.filter(file_name=file_name).first()
        if not download_job:
            raise NotFound('Download job with filename {} does not exist.'.format(file_name))

        # compile url to file
        file_path = settings.CSV_LOCAL_PATH + file_name if settings.IS_LOCAL else \
            self.s3_handler.get_simple_url(file_name=file_name)

        # add additional response elements that should be part of anything calling this function
        response = {
            'status': download_job.job_status.name,
            'url': file_path,
            'message': download_job.error_message,
            'file_name': file_name,
            # converting size from bytes to kilobytes if file_size isn't None
            'total_size': download_job.file_size / 1000  # TODO: 1000 or 1024?  Put units in name?
            if download_job.file_size else None,
            'total_columns': download_job.number_of_columns,
            'total_rows': download_job.number_of_rows,
            'seconds_elapsed': download_job.seconds_elapsed()
        }

        return Response(response)

    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data

        for required_param in ['file_format', 'award_levels', 'filters']:
            if required_param not in json_request:
                raise InvalidParameterException('{} parameter not provided'.format(required_param))

        # TODO: Refactor with the bulk_download method in populate_monthly_files.py
        # Check if the same request has been called today
        current_date = datetime.datetime.utcnow()
        updated_date_timestamp = datetime.datetime.strftime(current_date, '%Y-%m-%d')
        cached_download = BulkDownloadJob.objects.filter(
            json_request=json.dumps(order_nested_object(json_request)),
            update_date__gte=updated_date_timestamp).exclude(job_status_id=4).values('file_name')
        if cached_download:
            # By returning the cached files, there should be no duplicates on a daily basis
            cached_filename = cached_download[0]['file_name']
            return self.get_download_response(file_name=cached_filename)

        csv_sources = self.get_csv_sources(json_request)

        # get timestamped name to provide unique file name
        timestamped_file_name = self.s3_handler.get_timestamped_filename(
            self.DOWNLOAD_NAME + '.zip')

        # create download job in database to track progress. Starts at 'ready'
        # status by default.
        download_job_kwargs = {'job_status_id': JOB_STATUS_DICT['ready'],
                               'json_request': json.dumps(order_nested_object(json_request)),
                               'file_name': timestamped_file_name}
        award_levels = json_request['award_levels']
        award_types = json_request['filters']['award_types']
        agency = json_request['filters']['agency']
        sub_agency = json_request['filters'].get('sub_agency', None)
        start_date = json_request['filters']['date_range'].get('start_date', None)
        end_date = json_request['filters']['date_range'].get('end_date', None)
        date_type = json_request['filters']['date_type']

        for award_level in award_levels:
            download_job_kwargs[award_level] = True
        for award_type in award_types:
            download_job_kwargs[award_type] = True
        if agency and agency != 'all':
            download_job_kwargs['agency'] = ToptierAgency.objects.filter(toptier_agency_id=agency).first()
        if sub_agency:
            download_job_kwargs['sub_agency'] = sub_agency
        if start_date:
            download_job_kwargs['start_date'] = start_date
        if end_date:
            download_job_kwargs['end_date'] = end_date
        if date_type:
            download_job_kwargs['date_type'] = date_type
        download_job = BulkDownloadJob(**download_job_kwargs)
        download_job.save()

        logger.info('Added Bulk Download Job: {}\n'
                    'Filename: {}\n'
                    'Request Params: {}'.format(download_job.bulk_download_job_id,
                                                download_job.file_name,
                                                download_job.json_request))

        kwargs = {'download_job': download_job,
                  'file_name': timestamped_file_name,
                  'columns': json_request.get('columns', None),
                  'sources': csv_sources}

        if 'pytest' in sys.modules:
            # We are testing, and cannot use threads - the testing db connection
            # is not shared with the thread
            csv_selection.write_csvs(**kwargs)
        else:
            # Send a SQS message that will be processed by another server
            # which will eventually run csv_selection.write_csvs(**kwargs)
            # (see generate_bulk_zip.py)
            message_attributes = {
                'download_job_id': {
                    'StringValue': str(kwargs['download_job'].bulk_download_job_id),
                    'DataType': 'String'
                },
                'file_name': {
                    'StringValue': kwargs['file_name'],
                    'DataType': 'String'
                },
                'columns': {
                    'StringValue': json.dumps(kwargs['columns']),
                    'DataType': 'String'
                },
                'sources': {
                    'StringValue': json.dumps(tuple([source.toJsonDict() for source in kwargs['sources']])),
                    'DataType': 'String'
                }
            }
            queue = sqs_queue(region_name=settings.BULK_DOWNLOAD_AWS_REGION,
                              QueueName=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
            queue.send_message(MessageBody='Test', MessageAttributes=message_attributes)

        return self.get_download_response(file_name=timestamped_file_name)


def verify_requested_columns_available(sources, requested):
    bad_cols = set(requested)
    for source in sources:
        bad_cols -= set(source.columns(requested))
    if bad_cols:
        raise InvalidParameterException('Unknown columns: {}'.format(bad_cols))


class BulkDownloadListAgenciesViewSet(APIView):
    modified_agencies_list = os.path.join(settings.BASE_DIR,
                                          'usaspending_api', 'data', 'modified_authoritative_agency_list.csv')
    sub_agencies_map = {}

    def pull_modified_agencies_cgacs_subtiers(self):
        # Get a dict of used subtiers and their associated CGAC code pulled from
        # modified_agencies_list
        with open(self.modified_agencies_list, encoding='Latin-1') as modified_agencies_list_csv:
            mod_gencies_list_df = pd.read_csv(modified_agencies_list_csv, dtype=str)
        mod_gencies_list_df = mod_gencies_list_df[['CGAC AGENCY CODE', 'SUBTIER CODE']]
        mod_gencies_list_df['CGAC AGENCY CODE'] = mod_gencies_list_df['CGAC AGENCY CODE'] \
            .apply(lambda x: x.zfill(3))
        for _, row in mod_gencies_list_df.iterrows():
            self.sub_agencies_map[row['SUBTIER CODE']] = row['CGAC AGENCY CODE']

    def post(self, request):
        """Return list of agencies if no POST data is provided.
        Otherwise, returns sub_agencies/federal_accounts associated with the agency provided"""
        response_data = {'agencies': [],
                         'sub_agencies': [],
                         'federal_accounts': []}
        if not self.sub_agencies_map:
            # populate the sub_agencies dictionary
            self.pull_modified_agencies_cgacs_subtiers()
        used_cgacs = set(self.sub_agencies_map.values())
        # Adding 1601 as Department of Labor uses the FREC instead of the CGAC '0016'
        used_cgacs.add('1601')

        agency_id = None
        post_data = request.data
        if post_data:
            if 'agency' not in post_data:
                raise InvalidParameterException('agency parameter not provided')
            agency_id = post_data['agency']

        # Get all the top tier agencies
        toptier_agencies = list(ToptierAgency.objects.filter(cgac_code__in=used_cgacs)
                                .values('name', 'toptier_agency_id', 'cgac_code'))

        if not agency_id:
            # Return all the agencies if no agency id provided
            cfo_agencies = sorted(list(filter(lambda agency: agency['cgac_code'] in CFO_CGACS,
                                              toptier_agencies)),
                                  key=lambda agency: CFO_CGACS.index(agency['cgac_code']))
            other_agencies = sorted([agency for agency in toptier_agencies
                                     if agency not in cfo_agencies],
                                    key=lambda agency: agency['name'])
            response_data['agencies'] = {'cfo_agencies': cfo_agencies,
                                         'other_agencies': other_agencies}
        else:
            # Get the top tier agency object based on the agency id provided
            top_tier_agency = list(filter(lambda toptier: toptier['toptier_agency_id'] == agency_id,
                                          toptier_agencies))
            if not top_tier_agency:
                raise InvalidParameterException('Agency ID not found')
            top_tier_agency = top_tier_agency[0]
            # Get the sub agencies and federal accounts associated with that top tier agency
            response_data['sub_agencies'] = Agency.objects.filter(toptier_agency_id=agency_id)\
                .values(subtier_agency_name=F('subtier_agency__name'),
                        subtier_agency_code=F('subtier_agency__subtier_code'))\
                .order_by('subtier_agency_name')\
                .distinct('subtier_agency_name')
            # Tried converting this to queryset filtering but ran into issues trying to
            # double check the right used subtier_agency by cross checking the cgac_code
            # see the last 2 lines of the list comprehension below
            response_data['sub_agencies'] = [subagency for subagency in response_data['sub_agencies']
                                             if subagency['subtier_agency_code'] in self.sub_agencies_map and
                                             self.sub_agencies_map[subagency['subtier_agency_code']] ==
                                             top_tier_agency['cgac_code']]
            for subagency in response_data['sub_agencies']:
                del subagency['subtier_agency_code']

            response_data['federal_accounts'] = FederalAccount.objects\
                .filter(agency_identifier=top_tier_agency['cgac_code'])\
                .values(federal_account_name=F('account_title'), federal_account_id=F('id'))\
                .order_by('federal_account_name')
        return Response(response_data)


class ListMonthylDownloadsViewset(APIView):

    s3_handler = S3Handler(name=settings.MONTHLY_DOWNLOAD_S3_BUCKET_NAME, region=settings.BULK_DOWNLOAD_AWS_REGION)

    @cache_response()
    def post(self, request):
        """Return list of downloads that match the requested params"""
        response_data = {}

        post_data = request.data
        agency_id = post_data.get('agency', None)
        fiscal_year = post_data.get('fiscal_year', None)
        download_type = post_data.get('type', None)

        required_params = {'agency':agency_id, 'fiscal_year': fiscal_year, 'type': download_type}
        for required_param, param_value in required_params.items():
            if param_value is None:
                raise InvalidParameterException('Required param not provided: {}'.format(required_param))

        # Populate regex
        fiscal_year_regex = str(fiscal_year) if fiscal_year else '\d{4}'
        download_type_regex = download_type.capitalize() if download_type else '(Contracts|Assistance)'

        cgac_regex = '.*'
        if agency_id and agency_id == 'all':
            cgac_regex = 'all'
        elif agency_id:
            cgac_codes = ToptierAgency.objects.filter(toptier_agency_id=agency_id).values('cgac_code')
            if cgac_codes:
                cgac_regex = cgac_codes[0]['cgac_code']
            else:
                raise InvalidParameterException('{} agency not found'.format(agency_id))
        monthly_dl_regex = '{}_{}_{}_Full_.*\.zip'.format(fiscal_year_regex, cgac_regex, download_type_regex)

        # Generate regex possible prefix
        prefixes = []
        for regex, add_regex in [(fiscal_year_regex, fiscal_year), (cgac_regex, agency_id),
                                 (download_type_regex, download_type)]:
            if not add_regex:
                break
            prefixes.append(regex)
        prefix = '_'.join(prefixes)

        # Get and filter the files we need
        bucket_name = self.s3_handler.bucketRoute
        region_name = S3Handler.REGION
        bucket = boto.s3.connect_to_region(region_name).get_bucket(bucket_name)
        monthly_dls_names = list(filter(re.compile(monthly_dl_regex).search,
                                        [key.name for key in bucket.list(prefix=prefix)]))
        # Generate response
        downloads = []
        for name in monthly_dls_names:
            name_data = re.findall('(.*)_(.*)_(.*)_Full_(.*)\.zip', name)[0]
            agency_name = None
            agency_abbr = None
            agency_cgac = name_data[1]
            if agency_cgac != 'all':
                agency = ToptierAgency.objects.filter(cgac_code=agency_cgac).values('name', 'abbreviation')
                if agency:
                    agency_name = agency[0]['name']
                    agency_abbr = agency[0]['abbreviation']
            # Simply adds dashes for the date, 20180101 -> 2018-01-01, could also use strftime
            updated_date = '-'.join([name_data[3][:4], name_data[3][4:6], name_data[3][6:]])
            downloads.append({'fiscal_year': name_data[0],
                              'agency_name': agency_name,
                              'agency_acronym': agency_abbr,
                              'type': name_data[2].lower(),
                              'updated_date': updated_date,
                              'file_name': name,
                              'url': self.s3_handler.get_simple_url(file_name=name)})
        response_data['monthly_files'] = downloads
        return Response(response_data)


class BulkDownloadAwardsViewSet(BaseDownloadViewSet):
    """Generate bulk download for awards"""

    # TODO: Merge into award and transaction filters
    def process_filters(self, filters, award_level):
        """Filter function for Bulk Download Award Generation"""

        for required_param in ['award_types', 'agency', 'date_type', 'date_range']:
            if required_param not in filters:
                raise InvalidParameterException('{} filter not provided'.format(required_param))

        table = value_mappings[award_level]['table']
        queryset = table.objects.all()

        # Adding award type filter
        award_types = []
        try:
            for award_type in filters['award_types']:
                if award_type in award_type_mappings:
                    award_types.extend(award_type_mappings[award_type])
                else:
                    raise InvalidParameterException('Invalid award_type: {}'.format(award_type))
        except TypeError:
            raise InvalidParameterException('award_types parameter not provided as a list')
        # if the filter is calling everything, just remove the filter, save on the query performance
        if set(award_types) != set(itertools.chain(*award_type_mappings.values())):
            type_queryset_filters = {}
            type_queryset_filters['{}__in'.format(value_mappings[award_level]['type'])] = award_types
            type_queryset = Q(**type_queryset_filters)
            if (filters['award_types'] == ['contracts']):
                # IDV Flag
                idv_queryset_filters = {'{}__pulled_from'.format(value_mappings[award_level]['contract_data']): 'IDV'}
                type_queryset |= Q(**idv_queryset_filters)
            queryset &= table.objects.filter(type_queryset)

        # Adding date range filters
        # Get the date type attribute
        date_attribute = None
        if filters['date_type'] == 'action_date':
            date_attribute = value_mappings[award_level]['action_date']
        elif filters['date_type'] == 'last_modified_date':
            date_attribute = value_mappings[award_level]['last_modified_date']
        else:
            raise InvalidParameterException('Invalid parameter for date_type: {}'.format(filters['date_type']))
        # Get the date ranges
        try:
            date_range_filters = {}
            if 'start_date' in filters['date_range'] and filters['date_range']['start_date']:
                date_range_filters['{}__gte'.format(date_attribute)] = filters['date_range']['start_date']
            if 'end_date' in filters['date_range'] and filters['date_range']['end_date']:
                date_range_filters['{}__lte'.format(date_attribute)] = filters['date_range']['end_date']
            queryset &= table.objects.filter(**date_range_filters)
        except TypeError:
            raise InvalidParameterException('date_range parameter not provided as an object')

        # Agencies are to be OR'd together and then AND'd to the major query
        agencies_queryset = None
        if filters['agency'] != 'all':
            agencies_queryset = Q(awarding_agency__toptier_agency_id=filters['agency'])
            if 'sub_agency' in filters and filters['sub_agency']:
                agencies_queryset &= Q(awarding_agency__subtier_agency__name=filters['sub_agency'])
            queryset &= table.objects.filter(agencies_queryset)

        return queryset

    def get_csv_sources(self, json_request):
        """
        Generate the CSV Sources (filtered queryset and metadata) based on the request
        """
        award_levels = json_request['award_levels']
        # TODO: Send use file_format
        # file_format = json_request['file_format']

        csv_sources = []
        self.DOWNLOAD_NAME = '_'.join(value_mappings[award_level]['download_name']
                                      for award_level in award_levels)
        try:
            for award_level in award_levels:
                if award_level not in value_mappings:
                    raise InvalidParameterException('Invalid award_level: {}'.format(award_level))

                queryset = self.process_filters(json_request['filters'], award_level)
                award_level_table = value_mappings[award_level]['table']

                award_types = set(json_request['filters']['award_types'])
                d1_award_types = set(['contracts'])
                d2_award_types = set(['grants', 'direct_payments', 'loans', 'other_financial_assistance'])
                if award_types & d1_award_types:
                    # only generate d1 files if the user is asking for contracts
                    d1_source = csv_selection.CsvSource(value_mappings[award_level]['table_name'],
                                                        'd1', award_level)
                    d1_filters = {
                        '{}__isnull'.format(value_mappings[award_level]['contract_data']): False}
                    d1_source.queryset = queryset & award_level_table.objects.\
                        filter(**d1_filters)
                    csv_sources.append(d1_source)
                if award_types & d2_award_types:
                    # only generate d2 files if the user is asking for assistance data
                    d2_source = csv_selection.CsvSource(value_mappings[award_level]['table_name'],
                                                        'd2', award_level)
                    d2_filters = {
                        '{}__isnull'.format(value_mappings[award_level]['assistance_data']): False}
                    d2_source.queryset = queryset & award_level_table.objects.\
                        filter(**d2_filters)
                    csv_sources.append(d2_source)
                verify_requested_columns_available(tuple(csv_sources), json_request.get('columns', None))

        except TypeError:
            raise InvalidParameterException('award_levels parameter not provided as a list')
        return tuple(csv_sources)


class BulkDownloadStatusViewSet(BaseDownloadViewSet):
    def get(self, request):
        """Obtain status for the download job matching the file name provided"""

        get_request = request.query_params
        file_name = get_request.get('file_name')

        if not file_name:
            raise InvalidParameterException(
                'Missing one or more required query parameters: file_name')

        return self.get_download_response(file_name=file_name)
